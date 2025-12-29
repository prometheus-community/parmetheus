// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promparquet

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"
)

// Constraint represents a filter constraint on a parquet column.
type Constraint interface {
	fmt.Stringer

	// init initializes the constraint with respect to the file schema and projections.
	init(f ParquetFileView) error

	// path is the path for the column that is constrained
	path() string

	// prefilter returns a set of non-overlapping increasing row indexes that may satisfy the constraint.
	// This MUST be a superset of the real set of matching rows.
	prefilter(rgIdx int, rr []RowRange) ([]RowRange, error)

	// filter returns a set of non-overlapping increasing row indexes that do satisfy the constraint.
	// This MUST be the precise set of matching rows.
	filter(ctx context.Context, rgIdx int, primary bool, rr []RowRange) ([]RowRange, error)
}

// MatchersToConstraints converts Prometheus label matchers into parquet search constraints.
// It supports MatchEqual, MatchNotEqual, MatchRegexp, and MatchNotRegexp matcher types.
// Returns a slice of constraints that can be used to filter parquet data based on the
// provided label matchers, or an error if an unsupported matcher type is encountered.
func MatchersToConstraints(matchers ...*labels.Matcher) ([]Constraint, error) {
	r := make([]Constraint, 0, len(matchers))
	for _, matcher := range matchers {
		var c Constraint
	S:
		switch matcher.Type {
		case labels.MatchEqual:
			c = Equal(LabelToColumn(matcher.Name), parquet.ValueOf(matcher.Value))
		case labels.MatchNotEqual:
			c = Not(Equal(LabelToColumn(matcher.Name), parquet.ValueOf(matcher.Value)))
		case labels.MatchRegexp:
			if matcher.GetRegexString() == ".*" {
				continue
			}
			if matcher.GetRegexString() == ".+" {
				c = Not(Equal(LabelToColumn(matcher.Name), parquet.ValueOf("")))
				break S
			}
			if set := matcher.SetMatches(); len(set) == 1 {
				c = Equal(LabelToColumn(matcher.Name), parquet.ValueOf(set[0]))
				break S
			}
			rc, err := Regex(LabelToColumn(matcher.Name), matcher)
			if err != nil {
				return nil, fmt.Errorf("unable to construct regex matcher: %w", err)
			}
			c = rc
		case labels.MatchNotRegexp:
			inverted, err := matcher.Inverse()
			if err != nil {
				return nil, fmt.Errorf("unable to invert matcher: %w", err)
			}
			if set := inverted.SetMatches(); len(set) == 1 {
				c = Not(Equal(LabelToColumn(matcher.Name), parquet.ValueOf(set[0])))
				break S
			}
			rc, err := Regex(LabelToColumn(matcher.Name), inverted)
			if err != nil {
				return nil, fmt.Errorf("unable to construct regex matcher: %w", err)
			}
			c = Not(rc)
		default:
			return nil, fmt.Errorf("unsupported matcher type %s", matcher.Type)
		}
		r = append(r, c)
	}
	return r, nil
}

// Initialize prepares the given constraints for use with the specified parquet file.
// It calls the init method on each constraint to validate compatibility with the
// file schema and set up any necessary internal state.
func Initialize(f ParquetFileView, cs ...Constraint) error {
	for i := range cs {
		if err := cs[i].init(f); err != nil {
			return fmt.Errorf("unable to initialize constraint %d: %w", i, err)
		}
	}
	return nil
}

// sortConstraintsBySortingColumns reorders constraints to prioritize those that match sorting columns.
// Constraints matching sorting columns are moved to the front, ordered by the sorting column priority.
// Other constraints maintain their original relative order.
func sortConstraintsBySortingColumns(cs []Constraint, sc []parquet.SortingColumn) {
	if len(sc) == 0 {
		return
	}

	sortingPaths := make(map[string]int, len(sc))
	for i, col := range sc {
		sortingPaths[col.Path()[0]] = i
	}

	slices.SortStableFunc(cs, func(a, b Constraint) int {
		aIdx, aIsSorting := sortingPaths[a.path()]
		bIdx, bIsSorting := sortingPaths[b.path()]

		if aIsSorting && bIsSorting {
			return aIdx - bIdx
		}
		if aIsSorting {
			return -1
		}
		if bIsSorting {
			return 1
		}
		return 0
	})
}

// Filter applies the given constraints to a parquet row group and returns the row ranges
// that satisfy all constraints. It optimizes performance by prioritizing constraints on
// sorting columns, which are cheaper to evaluate.
func Filter(ctx context.Context, f ParquetShard, rgIdx int, cs ...Constraint) ([]RowRange, error) {
	rg := f.LabelsFile().RowGroups()[rgIdx]

	// Constraints for sorting columns are cheaper to evaluate, so we sort them first.
	sc := rg.SortingColumns()

	sortConstraintsBySortingColumns(cs, sc)

	var (
		err error
		mu  sync.Mutex
		g   errgroup.Group
	)

	// First pass prefilter with a quick index scan to find a superset of matching rows
	rr := []RowRange{{From: int64(0), Count: rg.NumRows()}}
	for i := range cs {
		rr, err = cs[i].prefilter(rgIdx, rr)
		if err != nil {
			return nil, fmt.Errorf("unable to prefilter with constraint %d: %w", i, err)
		}
	}
	res := slices.Clone(rr)

	if len(res) == 0 {
		return nil, nil
	}

	// Second pass page filter find the real set of matching rows, done concurrently because it involves IO
	for i := range cs {
		g.Go(func() error {
			isPrimary := len(sc) > 0 && cs[i].path() == sc[0].Path()[0]

			srr, err := cs[i].filter(ctx, rgIdx, isPrimary, rr)
			if err != nil {
				return fmt.Errorf("unable to filter with constraint %d: %w", i, err)
			}
			mu.Lock()
			res = intersectRowRanges(res, srr)
			mu.Unlock()

			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return nil, fmt.Errorf("unable to do second pass filter: %w", err)
	}

	return res, nil
}

// PageToRead represents a page that needs to be read with its bounds.
type PageToRead struct {
	idx int

	// for data pages
	pfrom int64
	pto   int64

	// for data and dictionary pages
	off int64
	csz int64 // compressed size
}

// NewPageToRead creates a new PageToRead with the given parameters.
func NewPageToRead(idx int, pfrom, pto, off, csz int64) PageToRead {
	return PageToRead{
		idx:   idx,
		pfrom: pfrom,
		pto:   pto,
		off:   off,
		csz:   csz,
	}
}

func (p *PageToRead) From() int64 {
	return p.pfrom
}

func (p *PageToRead) To() int64 {
	return p.pto
}

func (p *PageToRead) Offset() int64 {
	return p.off
}

func (p *PageToRead) CompressedSize() int64 {
	return p.csz
}

// SymbolTable is a helper that can decode the i-th value of a page.
// It only works for optional dictionary encoded columns.
type SymbolTable struct {
	dict parquet.Dictionary
	syms []int32
	defs []byte
	idx  []int32
}

func (s *SymbolTable) Get(r int) parquet.Value {
	i := s.GetIndex(r)
	switch i {
	case -1:
		return parquet.NullValue()
	default:
		return s.dict.Index(i)
	}
}

func (s *SymbolTable) DecodeBuffer(buf []parquet.Value, from, to int) {
	s.idx = slices.Grow(s.idx, to-from)[:to-from]
	for j := from; j < to; j++ {
		s.idx[j-from] = s.syms[j]
	}
	s.dict.Lookup(s.idx, buf)
}

func (s *SymbolTable) GetIndex(i int) int32 {
	switch s.defs[i] {
	case 1:
		return s.syms[i]
	default:
		return -1
	}
}

func (s *SymbolTable) Reset(pg parquet.Page) {
	dict := pg.Dictionary()
	data := pg.Data()
	syms := data.Int32()
	s.defs = pg.DefinitionLevels()

	if s.syms == nil {
		s.syms = make([]int32, len(s.defs))
	} else {
		s.syms = slices.Grow(s.syms, len(s.defs))[:len(s.defs)]
	}

	sidx := 0
	for i := range s.defs {
		if s.defs[i] == 1 {
			s.syms[i] = syms[sidx]
			sidx++
		}
	}
	s.dict = dict
}

func (s *SymbolTable) ResetWithRange(pg parquet.Page, l, r int) {
	dict := pg.Dictionary()
	data := pg.Data()
	syms := data.Int32()
	s.defs = pg.DefinitionLevels()

	if s.syms == nil {
		s.syms = make([]int32, len(s.defs))
	} else {
		s.syms = slices.Grow(s.syms, len(s.defs))[:len(s.defs)]
	}

	sidx := 0
	for i := range l {
		if s.defs[i] == 1 {
			sidx++
		}
	}
	for i := l; i < r; i++ {
		if s.defs[i] == 1 {
			s.syms[i] = syms[sidx]
			sidx++
		}
	}
	s.dict = dict
}

// Equal creates a constraint that matches rows where the column equals the given value.
func Equal(path string, value parquet.Value) Constraint {
	return &equalConstraint{pth: path, val: value}
}

type equalConstraint struct {
	pth string

	val parquet.Value
	f   ParquetFileView

	comp func(l, r parquet.Value) int
}

func (ec *equalConstraint) String() string {
	return fmt.Sprintf("equal(%q,%q)", ec.pth, ec.val)
}

func (ec *equalConstraint) prefilter(rgIdx int, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	from, to := rr[0].From, rr[len(rr)-1].From+rr[len(rr)-1].Count

	rg := ec.f.RowGroups()[rgIdx]

	col, ok := rg.Schema().Lookup(ec.path())
	if !ok {
		if ec.matches(parquet.ValueOf("")) {
			return slices.Clone(rr), nil
		}
		return []RowRange{}, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex]

	if skip, err := ec.skipByBloomfilter(cc); err != nil {
		return nil, fmt.Errorf("unable to skip by bloomfilter: %w", err)
	} else if skip {
		return nil, nil
	}

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	res := make([]RowRange, 0)
	for i := range cidx.NumPages() {
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		if cidx.NullPage(i) {
			if ec.matches(parquet.ValueOf("")) {
				res = append(res, RowRange{pfrom, pcount})
			}
			continue
		}

		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !ec.matches(parquet.ValueOf("")) && !maxv.IsNull() && ec.comp(ec.val, maxv) > 0 {
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !ec.matches(parquet.ValueOf("")) && !minv.IsNull() && ec.comp(ec.val, minv) < 0 {
			if cidx.IsAscending() {
				break
			}
			continue
		}
		res = append(res, RowRange{From: pfrom, Count: pto - pfrom})
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (ec *equalConstraint) filter(ctx context.Context, rgIdx int, primary bool, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	from, to := rr[0].From, rr[len(rr)-1].From+rr[len(rr)-1].Count

	rg := ec.f.RowGroups()[rgIdx]

	col, ok := rg.Schema().Lookup(ec.path())
	if !ok {
		if ec.matches(parquet.ValueOf("")) {
			return slices.Clone(rr), nil
		}
		return []RowRange{}, nil
	}

	cc := rg.ColumnChunks()[col.ColumnIndex]
	if skip, err := ec.skipByBloomfilter(cc); err != nil {
		return nil, fmt.Errorf("unable to skip by bloomfilter: %w", err)
	} else if skip {
		return nil, nil
	}

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	var (
		res     = make([]RowRange, 0)
		readPgs = make([]PageToRead, 0, 10)
	)
	for i := range cidx.NumPages() {
		poff, pcsz := oidx.Offset(i), oidx.CompressedPageSize(i)

		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		if cidx.NullPage(i) {
			if ec.matches(parquet.ValueOf("")) {
				res = append(res, RowRange{pfrom, pcount})
			}
			continue
		}

		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !ec.matches(parquet.ValueOf("")) && !maxv.IsNull() && ec.comp(ec.val, maxv) > 0 {
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !ec.matches(parquet.ValueOf("")) && !minv.IsNull() && ec.comp(ec.val, minv) < 0 {
			if cidx.IsAscending() {
				break
			}
			continue
		}
		readPgs = append(readPgs, NewPageToRead(i, pfrom, pto, poff, pcsz))
	}

	if len(readPgs) == 0 {
		return intersectRowRanges(simplify(res), rr), nil
	}

	dictOff, dictSz := ec.f.DictionaryPageBounds(rgIdx, col.ColumnIndex)

	minOffset := uint64(readPgs[0].Offset())
	maxOffset := readPgs[len(readPgs)-1].Offset() + readPgs[len(readPgs)-1].CompressedSize()

	if int(minOffset-(dictOff+dictSz)) < ec.f.PagePartitioningMaxGapSize() {
		minOffset = dictOff
	}

	pgs, err := ec.f.GetPages(ctx, cc, int64(minOffset), int64(maxOffset))
	if err != nil {
		return nil, err
	}
	defer func() { _ = pgs.Close() }()

	symbols := new(SymbolTable)
	buf := make([]parquet.Value, 1024)
	for _, p := range readPgs {
		pfrom := p.From()
		pto := p.To()

		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}
		symbols.Reset(pg)

		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))
		var l, r int
		switch {
		case cidx.IsAscending() && primary:
			l = sort.Search(n, func(i int) bool { return ec.comp(ec.val, symbols.Get(i)) <= 0 })
			r = sort.Search(n, func(i int) bool { return ec.comp(ec.val, symbols.Get(i)) < 0 })

			if lv, rv := max(bl, l), min(br, r); rv > lv {
				res = append(res, RowRange{pfrom + int64(lv), int64(rv - lv)})
			}
		default:
			off, count := bl, 0
			buf = slices.Grow(buf, br-bl)[:br-bl]
			symbols.DecodeBuffer(buf, bl, br)
			for j := bl; j < br; j++ {
				if !ec.matches(buf[j-bl]) {
					if count != 0 {
						res = append(res, RowRange{pfrom + int64(off), int64(count)})
					}
					off, count = j, 0
				} else {
					if count == 0 {
						off = j
					}
					count++
				}
			}
			if count != 0 {
				res = append(res, RowRange{pfrom + int64(off), int64(count)})
			}
		}
		parquet.Release(pg)
	}

	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (ec *equalConstraint) init(f ParquetFileView) error {
	c, ok := f.Schema().Lookup(ec.path())
	ec.f = f
	if !ok {
		return nil
	}
	stringKind := parquet.String().Type().Kind()
	if ec.val.Kind() != stringKind {
		return fmt.Errorf("schema: can only search string kind, got: %s", ec.val.Kind())
	}
	if c.Node.Type().Kind() != stringKind {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", stringKind, c.Node.Type().Kind())
	}
	ec.comp = c.Node.Type().Compare
	return nil
}

func (ec *equalConstraint) path() string {
	return ec.pth
}

func (ec *equalConstraint) matches(v parquet.Value) bool {
	return bytes.Equal(v.ByteArray(), ec.val.ByteArray())
}

func (ec *equalConstraint) skipByBloomfilter(cc parquet.ColumnChunk) (bool, error) {
	if ec.f.SkipBloomFilters() {
		return false, nil
	}

	bf := cc.BloomFilter()
	if bf == nil {
		return false, nil
	}
	ok, err := bf.Check(ec.val)
	if err != nil {
		return false, fmt.Errorf("unable to check bloomfilter: %w", err)
	}
	return !ok, nil
}

// Regex creates a constraint that matches rows where the column matches the given regex.
// r MUST be a matcher of type Regex.
func Regex(path string, r *labels.Matcher) (Constraint, error) {
	if r.Type != labels.MatchRegexp {
		return nil, fmt.Errorf("unsupported matcher type: %s", r.Type)
	}
	return &regexConstraint{
		pth:   path,
		cache: make(map[int32]bool),
		r:     r,
	}, nil
}

type regexConstraint struct {
	f     ParquetFileView
	pth   string
	cache map[int32]bool

	minv parquet.Value
	maxv parquet.Value

	r            *labels.Matcher
	matchesEmpty bool

	comp func(l, r parquet.Value) int
}

func (rc *regexConstraint) String() string {
	return fmt.Sprintf("regex(%v,%v)", rc.pth, rc.r.GetRegexString())
}

func (rc *regexConstraint) prefilter(rgIdx int, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	from, to := rr[0].From, rr[len(rr)-1].From+rr[len(rr)-1].Count

	rg := rc.f.RowGroups()[rgIdx]

	col, ok := rg.Schema().Lookup(rc.path())
	if !ok {
		if rc.matchesEmpty {
			return slices.Clone(rr), nil
		}
		return []RowRange{}, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex].(*parquet.FileColumnChunk)

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	res := make([]RowRange, 0)
	for i := range cidx.NumPages() {
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		if cidx.NullPage(i) {
			if rc.matchesEmpty {
				res = append(res, RowRange{pfrom, pcount})
			}
			continue
		}
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !rc.minv.IsNull() && !rc.maxv.IsNull() {
			if !rc.matchesEmpty && !maxv.IsNull() && rc.comp(rc.minv, maxv) > 0 {
				if cidx.IsDescending() {
					break
				}
				continue
			}
			if !rc.matchesEmpty && !minv.IsNull() && rc.comp(rc.maxv, minv) < 0 {
				if cidx.IsAscending() {
					break
				}
				continue
			}
		}
		res = append(res, RowRange{From: pfrom, Count: pto - pfrom})
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (rc *regexConstraint) filter(ctx context.Context, rgIdx int, isPrimary bool, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	from, to := rr[0].From, rr[len(rr)-1].From+rr[len(rr)-1].Count

	rg := rc.f.RowGroups()[rgIdx]

	col, ok := rg.Schema().Lookup(rc.path())
	if !ok {
		if rc.matchesEmpty {
			return slices.Clone(rr), nil
		}
		return []RowRange{}, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex].(*parquet.FileColumnChunk)

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	var (
		res     = make([]RowRange, 0)
		readPgs = make([]PageToRead, 0, 10)
	)
	for i := range cidx.NumPages() {
		poff, pcsz := oidx.Offset(i), oidx.CompressedPageSize(i)
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		if cidx.NullPage(i) {
			if rc.matchesEmpty {
				res = append(res, RowRange{pfrom, pcount})
			}
			continue
		}
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !rc.minv.IsNull() && !rc.maxv.IsNull() {
			if !rc.matchesEmpty && !maxv.IsNull() && rc.comp(rc.minv, maxv) > 0 {
				if cidx.IsDescending() {
					break
				}
				continue
			}
			if !rc.matchesEmpty && !minv.IsNull() && rc.comp(rc.maxv, minv) < 0 {
				if cidx.IsAscending() {
					break
				}
				continue
			}
		}
		readPgs = append(readPgs, NewPageToRead(i, pfrom, pto, poff, pcsz))
	}
	if len(readPgs) == 0 {
		return intersectRowRanges(simplify(res), rr), nil
	}

	dictOff, dictSz := rc.f.DictionaryPageBounds(rgIdx, col.ColumnIndex)

	minOffset := uint64(readPgs[0].Offset())
	maxOffset := readPgs[len(readPgs)-1].Offset() + readPgs[len(readPgs)-1].CompressedSize()

	if int(minOffset-(dictOff+dictSz)) < rc.f.PagePartitioningMaxGapSize() {
		minOffset = dictOff
	}

	pgs, err := rc.f.GetPages(ctx, cc, int64(minOffset), int64(maxOffset))
	if err != nil {
		return nil, err
	}
	defer func() { _ = pgs.Close() }()

	symbols := new(SymbolTable)
	for _, p := range readPgs {
		pfrom := p.pfrom
		pto := p.pto

		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}

		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))
		off, count := bl, 0
		symbols.ResetWithRange(pg, bl, br)
		for j := bl; j < br; j++ {
			if !rc.matches(symbols, symbols.GetIndex(j)) {
				if count != 0 {
					res = append(res, RowRange{pfrom + int64(off), int64(count)})
				}
				off, count = j, 0
			} else {
				if count == 0 {
					off = j
				}
				count++
			}
		}
		if count != 0 {
			res = append(res, RowRange{pfrom + int64(off), int64(count)})
		}
		parquet.Release(pg)
	}

	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (rc *regexConstraint) init(f ParquetFileView) error {
	c, ok := f.Schema().Lookup(rc.path())
	rc.f = f
	rc.matchesEmpty = rc.r.Matches("")
	if !ok {
		return nil
	}
	if stringKind := parquet.String().Type().Kind(); c.Node.Type().Kind() != stringKind {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", stringKind, c.Node.Type().Kind())
	}
	rc.cache = make(map[int32]bool)
	rc.comp = c.Node.Type().Compare

	rc.minv = parquet.NullValue()
	rc.maxv = parquet.NullValue()
	if len(rc.r.SetMatches()) > 0 {
		sm := make([]parquet.Value, len(rc.r.SetMatches()))
		for i, m := range rc.r.SetMatches() {
			sm[i] = parquet.ValueOf(m)
		}
		rc.minv = slices.MinFunc(sm, rc.comp)
		rc.maxv = slices.MaxFunc(sm, rc.comp)
	} else if len(rc.r.Prefix()) > 0 {
		rc.minv = parquet.ValueOf(rc.r.Prefix())
		rc.maxv = parquet.ValueOf(append([]byte(rc.r.Prefix()), bytes.Repeat([]byte{0xff}, parquet.DefaultColumnIndexSizeLimit)...))
	}

	return nil
}

func (rc *regexConstraint) path() string {
	return rc.pth
}

func (rc *regexConstraint) matches(symbols *SymbolTable, i int32) bool {
	accept, seen := rc.cache[i]
	if !seen {
		var v parquet.Value
		switch i {
		case -1:
			v = parquet.NullValue()
		default:
			v = symbols.dict.Index(i)
		}
		accept = rc.r.Matches(yoloString(v.ByteArray()))
		rc.cache[i] = accept
	}
	return accept
}

// Not creates a constraint that negates the given constraint.
func Not(c Constraint) Constraint {
	return &notConstraint{c: c}
}

type notConstraint struct {
	c Constraint
}

func (nc *notConstraint) String() string {
	return fmt.Sprintf("not(%v)", nc.c.String())
}

func (nc *notConstraint) prefilter(_ int, rr []RowRange) ([]RowRange, error) {
	// NOT constraints cannot be prefiltered since the child constraint returns a superset of the matching row range,
	// if we were to complement this row range the result here would be a subset and this would violate our interface.
	return slices.Clone(rr), nil
}

func (nc *notConstraint) filter(ctx context.Context, rgIdx int, primary bool, rr []RowRange) ([]RowRange, error) {
	base, err := nc.c.filter(ctx, rgIdx, primary, rr)
	if err != nil {
		return nil, fmt.Errorf("unable to compute child constraint: %w", err)
	}
	return complementRowRanges(base, rr), nil
}

func (nc *notConstraint) init(f ParquetFileView) error {
	return nc.c.init(f)
}

func (nc *notConstraint) path() string {
	return nc.c.path()
}
