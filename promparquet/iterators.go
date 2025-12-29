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
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

var _ prom_storage.ChunkSeries = &ConcreteChunksSeries{}

// ConcreteChunksSeries is a concrete implementation of ChunkSeries.
type ConcreteChunksSeries struct {
	lbls labels.Labels
	chks []chunks.Meta
}

func (c ConcreteChunksSeries) Labels() labels.Labels {
	return c.lbls
}

func (c ConcreteChunksSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return prom_storage.NewListChunkSeriesIterator(c.chks...)
}

func (c ConcreteChunksSeries) ChunkCount() (int, error) {
	return len(c.chks), nil
}

// ChunkSeriesSetCloser extends ChunkSeriesSet with a Close method.
type ChunkSeriesSetCloser interface {
	prom_storage.ChunkSeriesSet

	// Close releases any memory buffers held by the ChunkSeriesSet or the
	// underlying ChunkSeries. It is not safe to use the ChunkSeriesSet
	// or any of its ChunkSeries after calling Close.
	Close() error
}

// NoChunksConcreteLabelsSeriesSet is a series set without chunks.
type NoChunksConcreteLabelsSeriesSet struct {
	seriesSet        []*ConcreteChunksSeries
	currentSeriesIdx int
}

// NewNoChunksConcreteLabelsSeriesSet creates a new series set from labels.
func NewNoChunksConcreteLabelsSeriesSet(sLbls []labels.Labels) *NoChunksConcreteLabelsSeriesSet {
	seriesSet := make([]*ConcreteChunksSeries, len(sLbls))
	for i, lbls := range sLbls {
		seriesSet[i] = &ConcreteChunksSeries{lbls: lbls}
	}
	return &NoChunksConcreteLabelsSeriesSet{
		seriesSet:        seriesSet,
		currentSeriesIdx: -1,
	}
}

func (s *NoChunksConcreteLabelsSeriesSet) At() prom_storage.ChunkSeries {
	return s.seriesSet[s.currentSeriesIdx]
}

func (s *NoChunksConcreteLabelsSeriesSet) Next() bool {
	if s.currentSeriesIdx+1 == len(s.seriesSet) {
		return false
	}
	s.currentSeriesIdx++
	return true
}

func (s *NoChunksConcreteLabelsSeriesSet) Err() error {
	return nil
}

func (s *NoChunksConcreteLabelsSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func (s *NoChunksConcreteLabelsSeriesSet) Close() error {
	return nil
}

// FilterEmptyChunkSeriesSet is a ChunkSeriesSet that lazily filters out series with no chunks.
type FilterEmptyChunkSeriesSet struct {
	ctx     context.Context
	lblsSet []labels.Labels
	chnkSet ChunksIteratorIterator

	currentSeries              *ConcreteChunksSeries
	materializedSeriesCallback MaterializedSeriesFunc
	err                        error
}

// NewFilterEmptyChunkSeriesSet creates a new filtering series set.
func NewFilterEmptyChunkSeriesSet(
	ctx context.Context,
	lblsSet []labels.Labels,
	chnkSet ChunksIteratorIterator,
	materializeSeriesCallback MaterializedSeriesFunc,
) *FilterEmptyChunkSeriesSet {
	return &FilterEmptyChunkSeriesSet{
		ctx:                        ctx,
		lblsSet:                    lblsSet,
		chnkSet:                    chnkSet,
		materializedSeriesCallback: materializeSeriesCallback,
	}
}

func (s *FilterEmptyChunkSeriesSet) At() prom_storage.ChunkSeries {
	return s.currentSeries
}

func (s *FilterEmptyChunkSeriesSet) Next() bool {
	metas := make([]chunks.Meta, 0, 128)
	for s.chnkSet.Next() {
		if len(s.lblsSet) == 0 {
			s.err = errors.New("less labels than chunks, this should not happen")
			return false
		}
		lbls := s.lblsSet[0]
		s.lblsSet = s.lblsSet[1:]
		iter := s.chnkSet.At()
		for iter.Next() {
			metas = append(metas, iter.At())
		}

		if iter.Err() != nil {
			s.err = iter.Err()
			return false
		}

		if len(metas) == 0 {
			continue
		}
		metasCpy := make([]chunks.Meta, len(metas))
		copy(metasCpy, metas)

		s.currentSeries = &ConcreteChunksSeries{
			lbls: lbls,
			chks: metasCpy,
		}
		s.err = s.materializedSeriesCallback(s.ctx, s.currentSeries)
		return s.err == nil
	}
	if s.chnkSet.Err() != nil {
		s.err = s.chnkSet.Err()
	}
	if len(s.lblsSet) > 0 {
		s.err = errors.New("more labels than chunks, this should not happen")
	}
	return false
}

func (s *FilterEmptyChunkSeriesSet) Err() error {
	if s.err != nil {
		return s.err
	}
	return s.chnkSet.Err()
}

func (s *FilterEmptyChunkSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func (s *FilterEmptyChunkSeriesSet) Close() error {
	return s.chnkSet.Close()
}

// ChunksIteratorIterator iterates over chunks iterators.
type ChunksIteratorIterator interface {
	Next() bool
	At() chunks.Iterator
	Err() error
	Close() error
}

// MultiColumnChunksDecodingIterator yields a prometheus chunks.Iterator from multiple parquet Columns.
type MultiColumnChunksDecodingIterator struct {
	mint int64
	maxt int64

	columnValueIterators []*ColumnValueIterator
	d                    *PrometheusParquetChunksDecoder

	current *ValueDecodingChunkIterator
	err     error
}

func (i *MultiColumnChunksDecodingIterator) At() chunks.Iterator {
	return i.current
}

func (i *MultiColumnChunksDecodingIterator) Next() bool {
	if i.err != nil || len(i.columnValueIterators) == 0 {
		return false
	}

	multiColumnValues := make([]parquet.Value, 0, len(i.columnValueIterators))
	for _, columnValueIter := range i.columnValueIterators {
		if !columnValueIter.Next() {
			i.err = columnValueIter.Err()
			if i.err != nil {
				return false
			}
			continue
		}
		at := columnValueIter.At()
		multiColumnValues = append(multiColumnValues, at)
	}
	if len(multiColumnValues) == 0 {
		return false
	}

	i.current = &ValueDecodingChunkIterator{
		mint:   i.mint,
		maxt:   i.maxt,
		values: multiColumnValues,
		d:      i.d,
	}
	return true
}

func (i *MultiColumnChunksDecodingIterator) Err() error {
	return i.err
}

func (i *MultiColumnChunksDecodingIterator) Close() error {
	var errs []error
	for _, iter := range i.columnValueIterators {
		if err := iter.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ValueDecodingChunkIterator decodes and yields chunks from a parquet Values slice.
type ValueDecodingChunkIterator struct {
	mint   int64
	maxt   int64
	values []parquet.Value
	d      *PrometheusParquetChunksDecoder

	decoded []chunks.Meta
	current chunks.Meta
	err     error
}

func (i *ValueDecodingChunkIterator) At() chunks.Meta {
	return i.current
}

func (i *ValueDecodingChunkIterator) Next() bool {
	if i.err != nil {
		return false
	}
	if len(i.values) == 0 && len(i.decoded) == 0 {
		return false
	}
	if len(i.decoded) > 0 {
		i.current = i.decoded[0]
		i.decoded = i.decoded[1:]
		return true
	}
	value := i.values[0]
	i.values = i.values[1:]

	i.decoded, i.err = i.d.Decode(value.ByteArray(), i.mint, i.maxt)
	return i.Next()
}

func (i *ValueDecodingChunkIterator) Err() error {
	return i.err
}

// ColumnValueIterator iterates over values from multiple row ranges.
type ColumnValueIterator struct {
	currentIteratorIndex int
	rowRangesIterators   []*RowRangesValueIterator

	current parquet.Value
	err     error
}

func (i *ColumnValueIterator) At() parquet.Value {
	return i.current
}

func (i *ColumnValueIterator) Next() bool {
	if i.err != nil {
		return false
	}

	found := false
	for !found {
		if i.currentIteratorIndex >= len(i.rowRangesIterators) {
			return false
		}

		currentIterator := i.rowRangesIterators[i.currentIteratorIndex]
		hasNext := currentIterator.Next()

		if !hasNext {
			if err := currentIterator.Err(); err != nil {
				i.err = err
				_ = currentIterator.Close()
				return false
			}
			_ = currentIterator.Close()
			i.currentIteratorIndex++
			continue
		}
		i.current = currentIterator.At()
		found = true
	}
	return found
}

func (i *ColumnValueIterator) Err() error {
	return i.err
}

func (i *ColumnValueIterator) Close() error {
	var errs []error
	for _, iter := range i.rowRangesIterators {
		if err := iter.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// RowRangesValueIterator yields individual parquet Values from specified row ranges in its FilePages.
type RowRangesValueIterator struct {
	pgs          parquet.Pages
	pageIterator *PageValueIterator

	remainingRr []RowRange
	currentRr   RowRange
	next        int64
	remaining   int64
	currentRow  int64

	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func newRowRangesValueIterator(
	ctx context.Context,
	file ParquetFileView,
	cc parquet.ColumnChunk,
	pageRange PageToReadWithRow,
	dictOff uint64,
	dictSz uint64,
) (*RowRangesValueIterator, error) {
	minOffset := uint64(pageRange.Offset())
	maxOffset := uint64(pageRange.Offset() + pageRange.CompressedSize())

	if dictOff > 0 && int(minOffset-(dictOff+dictSz)) < file.PagePartitioningMaxGapSize() {
		minOffset = dictOff
	}

	pgs, err := file.GetPages(ctx, cc, int64(minOffset), int64(maxOffset))
	if err != nil {
		if pgs != nil {
			_ = pgs.Close()
		}
		return nil, fmt.Errorf("failed to get pages: %w", err)
	}

	err = pgs.SeekToRow(pageRange.rows[0].From)
	if err != nil {
		_ = pgs.Close()
		return nil, fmt.Errorf("failed to seek to row: %w", err)
	}

	remainingRr := pageRange.rows

	currentRr := remainingRr[0]
	next := currentRr.From
	remaining := currentRr.Count
	currentRow := currentRr.From

	remainingRr = remainingRr[1:]
	return &RowRangesValueIterator{
		pgs:          pgs,
		pageIterator: new(PageValueIterator),

		remainingRr: remainingRr,
		currentRr:   currentRr,
		next:        next,
		remaining:   remaining,
		currentRow:  currentRow,
	}, nil
}

func (i *RowRangesValueIterator) At() parquet.Value {
	return i.buffer[i.currentBufferIndex]
}

func (i *RowRangesValueIterator) Next() bool {
	if i.err != nil {
		return false
	}

	if len(i.buffer) > 0 && i.currentBufferIndex < len(i.buffer)-1 {
		i.currentBufferIndex++
		return true
	}

	if len(i.remainingRr) == 0 && i.remaining == 0 {
		return false
	}

	found := false
	for !found {
		page, err := i.pgs.ReadPage()
		if err != nil {
			i.err = fmt.Errorf("failed to read page: %w", err)
			return false
		}
		i.pageIterator.Reset(page)
		i.currentBufferIndex = 0
		i.buffer = i.buffer[:0]

		for i.pageIterator.Next() {
			if i.currentRow == i.next {
				found = true
				i.buffer = append(i.buffer, i.pageIterator.At())

				i.remaining--
				if i.remaining > 0 {
					i.next = i.next + 1
				} else if len(i.remainingRr) > 0 {
					i.currentRr = i.remainingRr[0]
					i.next = i.currentRr.From
					i.remaining = i.currentRr.Count
					i.remainingRr = i.remainingRr[1:]
				}
			}
			if i.remaining > 0 {
				i.currentRow += i.pageIterator.Skip(i.next - i.currentRow - 1)
			}
			i.currentRow++
		}
		parquet.Release(page)
		if i.pageIterator.Err() != nil {
			i.err = fmt.Errorf("failed to read page values: %w", i.pageIterator.Err())
			return false
		}
	}
	return found
}

func (i *RowRangesValueIterator) Err() error {
	return i.err
}

func (i *RowRangesValueIterator) Close() error {
	return i.pgs.Close()
}

// PageValueIterator yields individual parquet Values from its Page.
type PageValueIterator struct {
	p parquet.Page

	cachedSymbols map[int32]parquet.Value
	st            SymbolTable

	vr parquet.ValueReader

	current            int
	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func (i *PageValueIterator) At() parquet.Value {
	if i.vr == nil {
		dicIndex := i.st.GetIndex(i.current)
		if _, ok := i.cachedSymbols[dicIndex]; !ok {
			i.cachedSymbols[dicIndex] = i.st.Get(i.current).Clone()
		}
		return i.cachedSymbols[dicIndex]
	}
	return i.buffer[i.currentBufferIndex].Clone()
}

func (i *PageValueIterator) Next() bool {
	if i.err != nil {
		return false
	}

	i.current++
	if i.current >= int(i.p.NumRows()) {
		return false
	}

	if i.vr == nil {
		return true
	}

	i.currentBufferIndex++

	if i.currentBufferIndex == len(i.buffer) {
		n, err := i.vr.ReadValues(i.buffer[:cap(i.buffer)])
		if err != nil && err != io.EOF {
			i.err = err
		}
		i.buffer = i.buffer[:n]
		i.currentBufferIndex = 0
	}

	return true
}

func (vi *PageValueIterator) Skip(n int64) int64 {
	r := min(n, vi.p.NumRows()-int64(vi.current)-1)
	vi.current += int(r)
	if vi.vr != nil {
		vi.currentBufferIndex += int(r)
		for vi.currentBufferIndex >= len(vi.buffer) {
			vi.currentBufferIndex = vi.currentBufferIndex - len(vi.buffer)
			num, err := vi.vr.ReadValues(vi.buffer[:cap(vi.buffer)])
			if err != nil && err != io.EOF {
				vi.err = err
			}
			vi.buffer = vi.buffer[:num]
		}
	}

	return r
}

func (i *PageValueIterator) Reset(p parquet.Page) {
	i.p = p
	i.vr = nil
	if p.Dictionary() != nil {
		i.st.Reset(p)
		i.cachedSymbols = make(map[int32]parquet.Value, p.Dictionary().Len())
	} else {
		i.vr = p.Values()
		if i.buffer != nil {
			i.buffer = i.buffer[:0]
		} else {
			i.buffer = make([]parquet.Value, 0, 128)
		}
		i.currentBufferIndex = -1
	}
	i.current = -1
}

func (i *PageValueIterator) Err() error {
	return i.err
}
