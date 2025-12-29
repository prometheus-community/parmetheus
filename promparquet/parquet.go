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
	"io"

	"github.com/parquet-go/parquet-go"
)

// SizeReaderAt extends io.ReaderAt with a Size method.
type SizeReaderAt interface {
	io.ReaderAt
	Size() int64
}

// ReadAtWithContextCloser provides context-aware reading with close support.
type ReadAtWithContextCloser interface {
	WithContext(ctx context.Context) SizeReaderAt
	io.Closer
}

// ParquetFileConfigView provides read-only access to parquet file configuration.
type ParquetFileConfigView interface {
	SkipMagicBytes() bool
	SkipPageIndex() bool
	SkipBloomFilters() bool
	OptimisticRead() bool
	ReadBufferSize() int
	ReadMode() parquet.ReadMode

	// PagePartitioningMaxGapSize returns the maximum gap size between pages
	// that should be downloaded together in a single read call.
	PagePartitioningMaxGapSize() int
}

// ParquetFileView provides read access to a parquet file.
type ParquetFileView interface {
	parquet.FileView
	GetPages(ctx context.Context, cc parquet.ColumnChunk, minOffset, maxOffset int64) (parquet.Pages, error)
	DictionaryPageBounds(rgIdx, colIdx int) (uint64, uint64)

	ReadAtWithContextCloser

	ParquetFileConfigView
}

// ParquetShard represents a shard containing labels and chunks parquet files.
type ParquetShard interface {
	Name() string
	ShardIdx() int
	LabelsFile() ParquetFileView
	ChunksFile() ParquetFileView
	TSDBSchema() (*TSDBSchema, error)
}
