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

// ConcreteChunksSeriesSet is a series set that iterates over a slice of ConcreteChunksSeries.
type ConcreteChunksSeriesSet struct {
	seriesSet        []*ConcreteChunksSeries
	currentSeriesIdx int
}

func (s *ConcreteChunksSeriesSet) At() prom_storage.ChunkSeries {
	return s.seriesSet[s.currentSeriesIdx]
}

func (s *ConcreteChunksSeriesSet) Next() bool {
	if s.currentSeriesIdx+1 == len(s.seriesSet) {
		return false
	}
	s.currentSeriesIdx++
	return true
}

func (s *ConcreteChunksSeriesSet) Err() error {
	return nil
}

func (s *ConcreteChunksSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func (s *ConcreteChunksSeriesSet) Close() error {
	return nil
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
