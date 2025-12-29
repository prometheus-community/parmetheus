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
	"fmt"
	"slices"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"golang.org/x/sync/errgroup"
)

// Materializer handles materialization of labels and chunks from parquet files.
type Materializer struct {
	b           ParquetShard
	s           *TSDBSchema
	d           *PrometheusParquetChunksDecoder
	partitioner Partitioner

	colIdx      int
	concurrency int

	dataColToIndex []int

	materializedSeriesCallback       MaterializedSeriesFunc
	materializedLabelsFilterCallback MaterializedLabelsFilterCallback

	honorProjectionHints bool
}

// MaterializedSeriesFunc is a callback function that can be used to add limiter or statistic logics for
// materialized series.
type MaterializedSeriesFunc func(ctx context.Context, series prom_storage.ChunkSeries) error

// NoopMaterializedSeriesFunc is a noop callback function that does nothing.
func NoopMaterializedSeriesFunc(_ context.Context, _ prom_storage.ChunkSeries) error {
	return nil
}

// MaterializedLabelsFilterCallback returns a filter and a boolean indicating if the filter is enabled or not.
type MaterializedLabelsFilterCallback func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool)

// MaterializedLabelsFilter is a filter that can be used to filter series based on their labels.
type MaterializedLabelsFilter interface {
	Filter(ls labels.Labels) bool
	Close()
}

// NoopMaterializedLabelsFilterCallback is a noop MaterializedLabelsFilterCallback function that filters nothing.
func NoopMaterializedLabelsFilterCallback(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
	return nil, false
}

// NewMaterializer creates a new Materializer instance.
func NewMaterializer(s *TSDBSchema,
	d *PrometheusParquetChunksDecoder,
	block ParquetShard,
	concurrency int,
	materializeSeriesCallback MaterializedSeriesFunc,
	materializeLabelsFilterCallback MaterializedLabelsFilterCallback,
	honorProjectionHints bool,
) (*Materializer, error) {
	colIdx, ok := block.LabelsFile().Schema().Lookup(ColIndexesColumn)
	if !ok {
		return nil, fmt.Errorf("schema index %s not found", ColIndexesColumn)
	}

	dataColToIndex := make([]int, len(block.ChunksFile().Schema().Columns()))
	for i := 0; i < len(s.DataColsIndexes); i++ {
		c, ok := block.ChunksFile().Schema().Lookup(DataColumn(i))
		if !ok {
			return nil, fmt.Errorf("schema column %s not found", DataColumn(i))
		}

		dataColToIndex[i] = c.ColumnIndex
	}

	return &Materializer{
		s:                                s,
		d:                                d,
		b:                                block,
		colIdx:                           colIdx.ColumnIndex,
		concurrency:                      concurrency,
		partitioner:                      NewGapBasedPartitioner(block.ChunksFile().PagePartitioningMaxGapSize()),
		dataColToIndex:                   dataColToIndex,
		materializedSeriesCallback:       materializeSeriesCallback,
		materializedLabelsFilterCallback: materializeLabelsFilterCallback,
		honorProjectionHints:             honorProjectionHints,
	}, nil
}

// Materialize reconstructs the ChunkSeries that belong to the specified row ranges (rr).
func (m *Materializer) Materialize(ctx context.Context, hints *prom_storage.SelectHints, rgi int, mint, maxt int64, skipChunks bool, rr []RowRange) (ChunkSeriesSetCloser, error) {
	sLbls, err := m.MaterializeLabels(ctx, hints, rgi, rr)
	if err != nil {
		return nil, fmt.Errorf("error materializing labels: %w", err)
	}

	seriesSetLabels, rr := m.FilterSeriesLabels(ctx, hints, sLbls, rr)

	if skipChunks {
		return NewNoChunksConcreteLabelsSeriesSet(seriesSetLabels), nil
	}

	chks, err := m.MaterializeChunks(ctx, rgi, mint, maxt, rr)
	if err != nil {
		return nil, fmt.Errorf("materializer failed to materialize chunks: %w", err)
	}

	// Build series with chunks, filtering out series with no chunks
	results := make([]*ConcreteChunksSeries, 0, len(seriesSetLabels))
	for i, lbls := range seriesSetLabels {
		if len(chks[i]) == 0 {
			continue
		}
		series := &ConcreteChunksSeries{
			lbls: lbls,
			chks: chks[i],
		}
		if err := m.materializedSeriesCallback(ctx, series); err != nil {
			return nil, err
		}
		results = append(results, series)
	}

	return &ConcreteChunksSeriesSet{
		seriesSet:        results,
		currentSeriesIdx: -1,
	}, nil
}

// FilterSeriesLabels filters series labels based on the callback filter.
func (m *Materializer) FilterSeriesLabels(ctx context.Context, hints *prom_storage.SelectHints, sLbls [][]labels.Label, rr []RowRange) ([]labels.Labels, []RowRange) {
	seriesLabels := make([]labels.Labels, 0, len(sLbls))
	labelsFilter, ok := m.materializedLabelsFilterCallback(ctx, hints)
	if !ok {
		for _, s := range sLbls {
			seriesLabels = append(seriesLabels, labels.New(s...))
		}
		return seriesLabels, rr
	}

	defer labelsFilter.Close()

	filteredRR := make([]RowRange, 0, len(rr))
	var currentRange RowRange
	inRange := false
	seriesIdx := 0

	for _, rowRange := range rr {
		for i := int64(0); i < rowRange.Count; i++ {
			actualRowID := rowRange.From + i
			lbls := labels.New(sLbls[seriesIdx]...)

			if labelsFilter.Filter(lbls) {
				seriesLabels = append(seriesLabels, lbls)

				if !inRange {
					currentRange = RowRange{
						From:  actualRowID,
						Count: 1,
					}
					inRange = true
				} else if actualRowID == currentRange.From+currentRange.Count {
					currentRange.Count++
				} else {
					filteredRR = append(filteredRR, currentRange)
					currentRange = RowRange{
						From:  actualRowID,
						Count: 1,
					}
				}
			} else {
				if inRange {
					filteredRR = append(filteredRR, currentRange)
					inRange = false
				}
			}
			seriesIdx++
		}
	}

	if inRange {
		filteredRR = append(filteredRR, currentRange)
	}

	return seriesLabels, filteredRR
}

// MaterializeAllLabelNames extracts and returns all label names from the schema.
func (m *Materializer) MaterializeAllLabelNames() []string {
	r := make([]string, 0, len(m.b.LabelsFile().Schema().Columns()))
	for _, c := range m.b.LabelsFile().Schema().Columns() {
		lbl, ok := ExtractLabelFromColumn(c[0])
		if !ok {
			continue
		}

		r = append(r, lbl)
	}
	return r
}

// MaterializeLabelNames extracts and returns all unique label names from the specified row ranges.
func (m *Materializer) MaterializeLabelNames(ctx context.Context, rgi int, rr []RowRange) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cc := labelsRg.ColumnChunks()[m.colIdx]
	colsIdxs, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rr, cc, false)
	if err != nil {
		return nil, fmt.Errorf("materializer failed to materialize columns: %w", err)
	}

	seen := make(map[string]struct{})
	colsMap := make(map[string]struct{}, 10)
	for _, colsIdx := range colsIdxs {
		key := yoloString(colsIdx.ByteArray())
		if _, ok := seen[key]; !ok {
			idxs, err := DecodeUintSlice(colsIdx.ByteArray())
			if err != nil {
				return nil, fmt.Errorf("failed to decode column index: %w", err)
			}
			for _, idx := range idxs {
				if _, ok := colsMap[m.b.LabelsFile().Schema().Columns()[idx][0]]; !ok {
					colsMap[m.b.LabelsFile().Schema().Columns()[idx][0]] = struct{}{}
				}
			}
		}
	}
	lbls := make([]string, 0, len(colsMap))
	for col := range colsMap {
		l, ok := ExtractLabelFromColumn(col)
		if !ok {
			return nil, fmt.Errorf("error extracting label name from col %v", col)
		}
		lbls = append(lbls, l)
	}
	return lbls, nil
}

// MaterializeLabelValues extracts and returns all unique values for a specific label name.
func (m *Materializer) MaterializeLabelValues(ctx context.Context, name string, rgi int, rr []RowRange) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cIdx, ok := m.b.LabelsFile().Schema().Lookup(LabelToColumn(name))
	if !ok {
		return []string{}, nil
	}
	cc := labelsRg.ColumnChunks()[cIdx.ColumnIndex]
	values, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rr, cc, false)
	if err != nil {
		return nil, fmt.Errorf("materializer failed to materialize columns: %w", err)
	}

	r := make([]string, 0, len(values))
	vMap := make(map[string]struct{}, 10)
	for _, v := range values {
		strValue := yoloString(v.ByteArray())
		if _, ok := vMap[strValue]; !ok {
			r = append(r, strValue)
			vMap[strValue] = struct{}{}
		}
	}
	return r, nil
}

// MaterializeAllLabelValues extracts all unique values for a specific label name from the dictionary.
func (m *Materializer) MaterializeAllLabelValues(ctx context.Context, name string, rgi int) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cIdx, ok := m.b.LabelsFile().Schema().Lookup(LabelToColumn(name))
	if !ok {
		return []string{}, nil
	}
	cc := labelsRg.ColumnChunks()[cIdx.ColumnIndex]
	pages, err := m.b.LabelsFile().GetPages(ctx, cc, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get pages: %w", err)
	}
	p, err := pages.ReadPage()
	if err != nil {
		return []string{}, fmt.Errorf("failed to read page: %w", err)
	}
	defer parquet.Release(p)

	r := make([]string, 0, p.Dictionary().Len())
	for i := 0; i < p.Dictionary().Len(); i++ {
		r = append(r, p.Dictionary().Index(int32(i)).String())
	}
	return r, nil
}

// MaterializeLabels retrieves series labels, optionally filtered by projection hints.
func (m *Materializer) MaterializeLabels(ctx context.Context, hints *prom_storage.SelectHints, rgi int, rr []RowRange) ([][]labels.Label, error) {
	if !m.honorProjectionHints {
		hints = nil
	}

	columnIndexes, err := m.getColumnIndexes(ctx, rgi, rr)
	if err != nil {
		return nil, fmt.Errorf("failed to get column indexes: %w", err)
	}

	var (
		colsMap   = make(map[int][]RowRange, 10)
		needsHash bool
	)

	labelsSchema := m.b.LabelsFile().Schema()

	columnToRowRanges, _, err := m.buildColumnMappings(rr, columnIndexes)
	if err != nil {
		return nil, fmt.Errorf("failed to build column mapping: %w", err)
	}

	switch {
	case hints != nil && hints.ProjectionInclude:
		for _, labelName := range hints.ProjectionLabels {
			if labelName == SeriesHashColumn {
				needsHash = true
				col, ok := labelsSchema.Lookup(SeriesHashColumn)
				if ok {
					colsMap[col.ColumnIndex] = []RowRange{}
				}
			} else {
				col, ok := labelsSchema.Lookup(LabelToColumn(labelName))
				if !ok {
					continue
				}
				colsMap[col.ColumnIndex] = []RowRange{}
			}
		}

		for columnId := range colsMap {
			if rowRanges, exists := columnToRowRanges[columnId]; exists {
				colsMap[columnId] = rowRanges
			}
		}

	case hints != nil && !hints.ProjectionInclude:
		for columnId, rowRanges := range columnToRowRanges {
			colsMap[columnId] = rowRanges
		}

		seriesHashExcluded := false
		for _, labelName := range hints.ProjectionLabels {
			if labelName == SeriesHashColumn {
				seriesHashExcluded = true
				break
			}
		}
		needsHash = !seriesHashExcluded && len(hints.ProjectionLabels) > 0

		for _, labelName := range hints.ProjectionLabels {
			if labelName == SeriesHashColumn {
				col, ok := labelsSchema.Lookup(SeriesHashColumn)
				if ok {
					delete(colsMap, col.ColumnIndex)
				}
			} else {
				col, ok := labelsSchema.Lookup(LabelToColumn(labelName))
				if !ok {
					continue
				}
				delete(colsMap, col.ColumnIndex)
			}
		}

	default:
		for columnId, rowRanges := range columnToRowRanges {
			colsMap[columnId] = rowRanges
		}
	}

	results := make([][]labels.Label, len(columnIndexes))
	mtx := sync.Mutex{}
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(m.concurrency)
	labelsRowGroup := m.b.LabelsFile().RowGroups()[rgi]

	rowRangeToStartIndex := make(map[RowRange]int, len(rr))
	resultIndex := 0
	for _, rowRange := range rr {
		rowRangeToStartIndex[rowRange] = resultIndex
		resultIndex += int(rowRange.Count)
	}

	for columnIndex, rowRanges := range colsMap {
		errGroup.Go(func() error {
			columnChunk := labelsRowGroup.ColumnChunks()[columnIndex]
			columnName := labelsSchema.Columns()[columnIndex][0]

			var labelName string
			var ok bool

			if columnName == SeriesHashColumn {
				labelName = SeriesHashColumn
				ok = true
			} else {
				labelName, ok = ExtractLabelFromColumn(columnName)
			}

			if !ok {
				return fmt.Errorf("column %d not found in schema", columnIndex)
			}

			labelValues, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rowRanges, columnChunk, false)
			if err != nil {
				return fmt.Errorf("failed to materialize label values: %w", err)
			}

			mtx.Lock()
			defer mtx.Unlock()

			valueIndex := 0
			for _, rowRange := range rowRanges {
				startIndex := rowRangeToStartIndex[rowRange]

				for rowInRange := 0; rowInRange < int(rowRange.Count); rowInRange++ {
					if !labelValues[valueIndex].IsNull() {
						results[startIndex+rowInRange] = append(results[startIndex+rowInRange], labels.Label{
							Name:  labelName,
							Value: yoloString(labelValues[valueIndex].ByteArray()),
						})
					}
					valueIndex++
				}
			}
			return nil
		})
	}

	var hashes []parquet.Value
	if needsHash {
		errGroup.Go(func() error {
			col, ok := labelsSchema.Lookup(SeriesHashColumn)
			if !ok {
				return fmt.Errorf("unable to find series hash column %q", SeriesHashColumn)
			}
			columnChunk := labelsRowGroup.ColumnChunks()[col.ColumnIndex]

			h, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rr, columnChunk, false)
			if err != nil {
				return fmt.Errorf("unable to materialize hash values: %w", err)
			}
			hashes = h
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	if needsHash {
		for i := range hashes {
			if !hashes[i].IsNull() {
				results[i] = append(results[i], labels.Label{
					Name:  SeriesHashColumn,
					Value: yoloString(hashes[i].ByteArray()),
				})
			}
		}
	}

	return results, nil
}

func (m *Materializer) getColumnIndexes(ctx context.Context, rgi int, rr []RowRange) ([]parquet.Value, error) {
	labelsRowGroup := m.b.LabelsFile().RowGroups()[rgi]
	columnChunk := labelsRowGroup.ColumnChunks()[m.colIdx]

	columnIndexes, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rr, columnChunk, false)
	if err != nil {
		return nil, fmt.Errorf("failed to materialize column indexes: %w", err)
	}

	return columnIndexes, nil
}

func (m *Materializer) buildColumnMappings(rr []RowRange, columnIndexes []parquet.Value) (map[int][]RowRange, map[RowRange]int, error) {
	columnToRowRanges := make(map[int][]RowRange, 10)
	rowRangeToStartIndex := make(map[RowRange]int, len(rr))

	columnIndexPos := 0
	resultIndex := 0

	for _, rowRange := range rr {
		rowRangeToStartIndex[rowRange] = resultIndex
		seenColumns := NewBitmap(len(m.b.LabelsFile().ColumnIndexes()))

		for rowInRange := int64(0); rowInRange < rowRange.Count; rowInRange++ {
			columnIds, err := DecodeUintSlice(columnIndexes[columnIndexPos].ByteArray())
			columnIndexPos++

			if err != nil {
				return nil, nil, err
			}

			for _, columnId := range columnIds {
				if !seenColumns.Get(columnId) {
					columnToRowRanges[columnId] = append(columnToRowRanges[columnId], rowRange)
					seenColumns.Set(columnId)
				}
			}
			resultIndex++
		}
	}

	return columnToRowRanges, rowRangeToStartIndex, nil
}

// MaterializeChunks materializes chunks for the given row ranges and returns them directly.
func (m *Materializer) MaterializeChunks(ctx context.Context, rgi int, mint, maxt int64, rr []RowRange) ([][]chunks.Meta, error) {
	minDataCol := m.s.DataColumIdx(mint)
	maxDataCol := m.s.DataColumIdx(maxt)
	rg := m.b.ChunksFile().RowGroups()[rgi]
	r := make([][]chunks.Meta, totalRows(rr))

	for i := minDataCol; i <= min(maxDataCol, len(m.dataColToIndex)-1); i++ {
		cc := rg.ColumnChunks()[m.dataColToIndex[i]]
		values, err := m.materializeColumnSlice(ctx, m.b.ChunksFile(), rgi, rr, cc, true)
		if err != nil {
			return r, fmt.Errorf("failed to materialize column: %w", err)
		}

		for vi, value := range values {
			chks, err := m.d.Decode(value.ByteArray(), mint, maxt)
			if err != nil {
				return r, fmt.Errorf("failed to decode chunks: %w", err)
			}
			r[vi] = append(r[vi], chks...)
		}
	}

	return r, nil
}

func (m *Materializer) materializeColumnSlice(
	ctx context.Context,
	file ParquetFileView,
	rgi int,
	rr []RowRange,
	cc parquet.ColumnChunk,
	chunkColumn bool,
) ([]parquet.Value, error) {
	pageRanges, err := m.GetPageRangesForColumn(cc, file, rgi, rr, chunkColumn)
	if err != nil || len(pageRanges) == 0 {
		return nil, err
	}

	pageRangesValues := make([][]parquet.Value, len(pageRanges))

	dictOff, dictSz := file.DictionaryPageBounds(rgi, cc.Column())
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(m.concurrency)

	for i, pageRange := range pageRanges {
		errGroup.Go(func() error {
			valuesIter, err := newRowRangesValueIterator(ctx, file, cc, pageRange, dictOff, dictSz)
			if err != nil {
				return fmt.Errorf("failed to create row values iterator for page: %w", err)
			}
			defer func() { _ = valuesIter.Close() }()

			iterValues := make([]parquet.Value, 0, totalRows(pageRange.rows))
			for valuesIter.Next() {
				iterValues = append(iterValues, valuesIter.At())
			}
			if err = valuesIter.Err(); err != nil {
				return err
			}

			pageRangesValues[i] = iterValues
			return nil
		})
	}
	err = errGroup.Wait()
	if err != nil {
		return nil, fmt.Errorf("failed to materialize columns: %w", err)
	}

	valuesFlattened := make([]parquet.Value, 0, totalRows(rr))
	for _, pageRangeValues := range pageRangesValues {
		valuesFlattened = append(valuesFlattened, pageRangeValues...)
	}

	return valuesFlattened, nil
}

// PageToReadWithRow extends PageToRead with row range information.
type PageToReadWithRow struct {
	PageToRead
	rows []RowRange
}

// NewPageToReadWithRow creates a new PageToReadWithRow.
func NewPageToReadWithRow(p PageToRead, rows []RowRange) PageToReadWithRow {
	return PageToReadWithRow{
		PageToRead: p,
		rows:       rows,
	}
}

// Rows returns the row ranges for this page.
func (pr *PageToReadWithRow) Rows() []RowRange {
	return pr.rows
}

// GetPageRangesForColumn returns the page ranges needed to read the specified row ranges.
func (m *Materializer) GetPageRangesForColumn(cc parquet.ColumnChunk, file ParquetFileView, rgi int, rr []RowRange, chunkColumn bool) ([]PageToReadWithRow, error) {
	if len(rr) == 0 {
		return nil, nil
	}

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to get offset index: %w", err)
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to get column index: %w", err)
	}

	group := file.RowGroups()[rgi]
	pagesToRowsMap := make(map[int][]RowRange, len(rr))

	for i := 0; i < cidx.NumPages(); i++ {
		pageRowRange := RowRange{
			From: oidx.FirstRowIndex(i),
		}
		pageRowRange.Count = group.NumRows()

		if i < oidx.NumPages()-1 {
			pageRowRange.Count = oidx.FirstRowIndex(i+1) - pageRowRange.From
		}

		for _, r := range rr {
			if pageRowRange.Overlaps(r) {
				pagesToRowsMap[i] = append(pagesToRowsMap[i], pageRowRange.Intersection(r))
			}
		}
	}

	pageRanges := m.CoalescePageRanges(pagesToRowsMap, oidx)
	return pageRanges, nil
}

// CoalescePageRanges merges nearby pages to enable efficient sequential reads.
func (m *Materializer) CoalescePageRanges(pagedIdx map[int][]RowRange, offset parquet.OffsetIndex) []PageToReadWithRow {
	if len(pagedIdx) == 0 {
		return []PageToReadWithRow{}
	}
	idxs := make([]int, 0, len(pagedIdx))
	for idx := range pagedIdx {
		idxs = append(idxs, idx)
	}

	slices.Sort(idxs)

	parts := m.partitioner.Partition(len(idxs), func(i int) (int, int) {
		return int(offset.Offset(idxs[i])), int(offset.Offset(idxs[i]) + offset.CompressedPageSize(idxs[i]))
	})

	r := make([]PageToReadWithRow, 0, len(parts))
	for _, part := range parts {
		var rows []RowRange
		for i := part.ElemRng[0]; i < part.ElemRng[1]; i++ {
			rows = append(rows, pagedIdx[idxs[i]]...)
		}

		pageToReadWithRow := NewPageToReadWithRow(
			NewPageToRead(
				0,
				int64(part.ElemRng[0]),
				int64(part.ElemRng[1]),
				int64(part.Start),
				int64(part.End-part.Start),
			),
			rows,
		)

		r = append(r, pageToReadWithRow)
	}

	return r
}
