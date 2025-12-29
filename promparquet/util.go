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
	"unsafe"
)

// yoloString converts a byte slice to a string without copying.
// This is unsafe and the string should not be used after the byte slice is modified.
// Note: This pattern is used in various places in the Prometheus codebase.
func yoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

const bitsPerWord = 64

// Bitmap is a simple bitmap implementation for tracking column presence.
type Bitmap struct {
	size int
	bits []uint64
}

// NewBitmap initializes a bitmap with the given size.
func NewBitmap(size int) *Bitmap {
	return &Bitmap{
		size: size,
		bits: make([]uint64, (size+bitsPerWord-1)/bitsPerWord),
	}
}

// Set sets the bit at position i to 1.
func (bm *Bitmap) Set(i int) {
	if i < 0 || i >= bm.size {
		return
	}
	bm.bits[i/bitsPerWord] |= 1 << (i % bitsPerWord)
}

// Clear sets the bit at position i to 0.
func (bm *Bitmap) Clear(i int) {
	if i < 0 || i >= bm.size {
		return
	}
	bm.bits[i/bitsPerWord] &^= 1 << (i % bitsPerWord)
}

// Get returns true if the bit at position i is set.
func (bm *Bitmap) Get(i int) bool {
	if i < 0 || i >= bm.size {
		return false
	}
	return (bm.bits[i/bitsPerWord] & (1 << (i % bitsPerWord))) != 0
}

// Part represents a partition of page ranges for I/O optimization.
type Part struct {
	Start int
	End   int

	ElemRng [2]int
}

// Partitioner partitions length entries into n <= length ranges that cover all
// input ranges. It supports overlapping ranges.
type Partitioner interface {
	Partition(length int, rng func(int) (int, int)) []Part
}

type gapBasedPartitioner struct {
	maxGapSize int
}

// NewGapBasedPartitioner creates a partitioner that combines pages separated by gaps
// smaller than maxGapSize.
func NewGapBasedPartitioner(maxGapSize int) Partitioner {
	return gapBasedPartitioner{
		maxGapSize: maxGapSize,
	}
}

// Partition partitions length entries into n <= length ranges that cover all
// input ranges by combining entries that are separated by reasonably small gaps.
func (g gapBasedPartitioner) Partition(length int, rng func(int) (int, int)) (parts []Part) {
	j := 0
	k := 0
	for k < length {
		j = k
		k++

		p := Part{}
		p.Start, p.End = rng(j)

		// Keep growing the range until the end or we encounter a large gap.
		for ; k < length; k++ {
			s, e := rng(k)

			if p.End+g.maxGapSize < s {
				break
			}

			if p.End <= e {
				p.End = e
			}
		}
		p.ElemRng = [2]int{j, k}
		parts = append(parts, p)
	}
	return parts
}
