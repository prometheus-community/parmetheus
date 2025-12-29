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
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestFilterSeriesLabels(t *testing.T) {
	ctx := context.Background()

	// Create sample labels for testing
	sampleLabels := [][]labels.Label{
		{
			{Name: "__name__", Value: "metric_1"},
			{Name: "instance", Value: "server1"},
			{Name: "job", Value: "web"},
		},
		{
			{Name: "__name__", Value: "metric_2"},
			{Name: "instance", Value: "server2"},
			{Name: "job", Value: "db"},
		},
		{
			{Name: "__name__", Value: "metric_3"},
			{Name: "instance", Value: "server1"},
			{Name: "job", Value: "web"},
		},
		{
			{Name: "__name__", Value: "metric_4"},
			{Name: "instance", Value: "server3"},
			{Name: "job", Value: "cache"},
		},
	}

	// Use non-sequential row ranges to test proper row mapping
	sampleRowRanges := []RowRange{
		{From: 10, Count: 1},
		{From: 25, Count: 1},
		{From: 50, Count: 1},
		{From: 100, Count: 1},
	}

	// Create a mock materializer
	materializer := &Materializer{}

	t.Run("NoFilteringEnabled", func(t *testing.T) {
		// Test when filtering is disabled (callback returns false)
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return nil, false
		}

		results, filteredRR := materializer.FilterSeriesLabels(ctx, nil, sampleLabels, sampleRowRanges)

		// Should return all series without filtering
		require.Len(t, results, len(sampleLabels))
		require.Equal(t, sampleRowRanges, filteredRR)

		// Verify all labels are preserved
		for i, result := range results {
			expectedLabels := labels.New(sampleLabels[i]...)
			require.Equal(t, expectedLabels, result)
		}
	})

	t.Run("AcceptAllFilter", func(t *testing.T) {
		// Test filter that accepts all series
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return &acceptAllFilter{}, true
		}

		results, filteredRR := materializer.FilterSeriesLabels(ctx, nil, sampleLabels, sampleRowRanges)

		// Should return all series
		require.Len(t, results, len(sampleLabels))
		require.Equal(t, sampleRowRanges, filteredRR)

		// Verify all labels are preserved
		for i, result := range results {
			expectedLabels := labels.New(sampleLabels[i]...)
			require.Equal(t, expectedLabels, result)
		}
	})

	t.Run("RejectAllFilter", func(t *testing.T) {
		// Test filter that rejects all series
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return &rejectAllFilter{}, true
		}

		results, filteredRR := materializer.FilterSeriesLabels(ctx, nil, sampleLabels, sampleRowRanges)

		// Should return no series
		require.Len(t, results, 0)
		require.Len(t, filteredRR, 0)
	})

	t.Run("SelectiveFilter", func(t *testing.T) {
		// Test filter that only accepts series with job="web"
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return &jobWebFilter{}, true
		}

		results, filteredRR := materializer.FilterSeriesLabels(ctx, nil, sampleLabels, sampleRowRanges)

		// Should return only series with job="web" (indices 0 and 2)
		require.Len(t, results, 2)
		require.Len(t, filteredRR, 2)

		// Verify correct series are returned
		require.Equal(t, "metric_1", results[0].Get("__name__"))
		require.Equal(t, "web", results[0].Get("job"))
		require.Equal(t, "metric_3", results[1].Get("__name__"))
		require.Equal(t, "web", results[1].Get("job"))

		// Verify row ranges map to the actual row positions from input
		expectedRR := []RowRange{
			{From: 10, Count: 1},
			{From: 50, Count: 1},
		}
		require.Equal(t, expectedRR, filteredRR)
	})

	t.Run("ContiguousRangeMerging", func(t *testing.T) {
		// Test with contiguous row ranges that should be merged
		contiguousRowRanges := []RowRange{
			{From: 100, Count: 1},
			{From: 101, Count: 1},
			{From: 102, Count: 1},
			{From: 200, Count: 1},
		}

		// Filter that accepts first two series (should create contiguous range)
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return &firstTwoFilter{}, true
		}

		results, filteredRR := materializer.FilterSeriesLabels(ctx, nil, sampleLabels, contiguousRowRanges)

		// Should return first two series
		require.Len(t, results, 2)
		require.Len(t, filteredRR, 1) // Should be merged into one contiguous range

		// Verify correct series are returned
		require.Equal(t, "metric_1", results[0].Get("__name__"))
		require.Equal(t, "metric_2", results[1].Get("__name__"))

		// Verify contiguous ranges are merged
		expectedRR := []RowRange{
			{From: 100, Count: 2}, // Merged range covering rows 100-101
		}
		require.Equal(t, expectedRR, filteredRR)
	})

	t.Run("EmptyInput", func(t *testing.T) {
		// Test with empty input
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return &acceptAllFilter{}, true
		}

		results, filteredRR := materializer.FilterSeriesLabels(ctx, nil, nil, nil)

		require.Len(t, results, 0)
		require.Len(t, filteredRR, 0)
	})

	t.Run("FilterCloseIsCalled", func(t *testing.T) {
		// Test that Close() is called on the filter
		closeCalled := false
		filter := &trackingFilter{
			acceptAll:   true,
			closeCalled: &closeCalled,
		}

		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return filter, true
		}

		_, _ = materializer.FilterSeriesLabels(ctx, nil, sampleLabels, sampleRowRanges)

		require.True(t, closeCalled, "Filter Close() should have been called")
	})
}

// Test filter implementations

type acceptAllFilter struct{}

func (f *acceptAllFilter) Filter(ls labels.Labels) bool {
	return true
}

func (f *acceptAllFilter) Close() {}

type rejectAllFilter struct{}

func (f *rejectAllFilter) Filter(ls labels.Labels) bool {
	return false
}

func (f *rejectAllFilter) Close() {}

type jobWebFilter struct{}

func (f *jobWebFilter) Filter(ls labels.Labels) bool {
	return ls.Get("job") == "web"
}

func (f *jobWebFilter) Close() {}

type firstTwoFilter struct{}

func (f *firstTwoFilter) Filter(ls labels.Labels) bool {
	name := ls.Get("__name__")
	return name == "metric_1" || name == "metric_2"
}

func (f *firstTwoFilter) Close() {}

type trackingFilter struct {
	acceptAll   bool
	closeCalled *bool
}

func (f *trackingFilter) Filter(ls labels.Labels) bool {
	return f.acceptAll
}

func (f *trackingFilter) Close() {
	*f.closeCalled = true
}
