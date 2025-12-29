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
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// Column naming constants following the l_<label> and s_data_<n> convention.
const (
	LabelColumnPrefix = "l_"
	DataColumnPrefix  = "s_data_"
	ColIndexesColumn  = "s_col_indexes"
	SeriesHashColumn  = "s_series_hash"

	DataColSizeMd = "data_col_duration_ms"
	MinTMd        = "minT"
	MaxTMd        = "maxT"
)

// LabelToColumn converts a label name to its column name.
func LabelToColumn(lbl string) string {
	return fmt.Sprintf("%s%s", LabelColumnPrefix, lbl)
}

// ExtractLabelFromColumn extracts the label name from a column name.
func ExtractLabelFromColumn(col string) (string, bool) {
	if !strings.HasPrefix(col, LabelColumnPrefix) {
		return "", false
	}
	return col[len(LabelColumnPrefix):], true
}

// IsDataColumn returns true if the column name is a data column.
func IsDataColumn(col string) bool {
	return strings.HasPrefix(col, DataColumnPrefix)
}

// DataColumn returns the column name for a data column at index i.
func DataColumn(i int) string {
	return fmt.Sprintf("%s%v", DataColumnPrefix, i)
}

// TSDBSchema contains the schema information for a TSDB parquet block.
type TSDBSchema struct {
	Schema   *parquet.Schema
	Metadata map[string]string

	DataColsIndexes   []int
	MinTs, MaxTs      int64
	DataColDurationMs int64
}

// DataColumIdx returns the data column index for a given timestamp.
func (s *TSDBSchema) DataColumIdx(t int64) int {
	if t < s.MinTs {
		return 0
	}
	return int((t - s.MinTs) / s.DataColDurationMs)
}

// PrometheusParquetChunksDecoder decodes chunks from parquet values.
type PrometheusParquetChunksDecoder struct {
	Pool chunkenc.Pool
}

// NewPrometheusParquetChunksDecoder creates a new decoder with the given pool.
func NewPrometheusParquetChunksDecoder(pool chunkenc.Pool) *PrometheusParquetChunksDecoder {
	return &PrometheusParquetChunksDecoder{
		Pool: pool,
	}
}

// Decode deserializes chunk data that was previously encoded.
// It takes binary data containing serialized chunks and reconstructs them as chunks.Meta objects.
// The function filters chunks based on the provided time range [mint, maxt].
func (e *PrometheusParquetChunksDecoder) Decode(data []byte, mint, maxt int64) ([]chunks.Meta, error) {
	result := make([]chunks.Meta, 0, 5)

	b := bytes.NewBuffer(data)

	for {
		chkEnc, err := binary.ReadUvarint(b)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		minTime, err := binary.ReadUvarint(b)
		if err != nil {
			return nil, err
		}

		maxTime, err := binary.ReadUvarint(b)
		if err != nil {
			return nil, err
		}
		size, err := binary.ReadUvarint(b)
		if err != nil {
			return nil, err
		}
		cData := b.Bytes()[:size]
		chk, err := e.Pool.Get(chunkenc.Encoding(chkEnc), cData)
		if err != nil {
			return nil, err
		}
		b.Next(int(size))

		if int64(minTime) > maxt {
			continue
		}

		if int64(maxTime) >= mint {
			result = append(result, chunks.Meta{
				MinTime: int64(minTime),
				MaxTime: int64(maxTime),
				Chunk:   chk,
			})
		}
	}

	return result, nil
}

// DecodeUintSlice decodes a variable-length encoded slice of integers.
func DecodeUintSlice(b []byte) ([]int, error) {
	buffer := bytes.NewBuffer(b)

	// size
	s, err := binary.ReadVarint(buffer)
	if err != nil {
		return nil, err
	}

	r := make([]int, 0, s)

	for i := int64(0); i < s; i++ {
		v, err := binary.ReadVarint(buffer)
		if err != nil {
			return nil, err
		}
		r = append(r, int(v))
	}

	return r, nil
}
