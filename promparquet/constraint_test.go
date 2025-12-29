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
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func mustNewMatcher(t testing.TB, s string) *labels.Matcher {
	res, err := labels.NewMatcher(labels.MatchRegexp, "doesntmatter", s)
	if err != nil {
		t.Fatalf("unable to build fast regex matcher: %s", err)
	}
	return res
}

func mustRegexConstraint(t testing.TB, col string, m *labels.Matcher) Constraint {
	res, err := Regex(col, m)
	if err != nil {
		t.Fatalf("unable to build regex constraint: %s", err)
	}
	return res
}

func TestEqualConstraint(t *testing.T) {
	c := Equal("test_col", parquet.ValueOf("test_value"))
	require.Equal(t, "test_col", c.path())
	require.Contains(t, c.String(), "test_col")
	require.Contains(t, c.String(), "test_value")
}

func TestNotConstraint(t *testing.T) {
	inner := Equal("test_col", parquet.ValueOf("test_value"))
	c := Not(inner)
	require.Equal(t, "test_col", c.path())
	require.Contains(t, c.String(), "not")
}

func TestRegexConstraint(t *testing.T) {
	m := mustNewMatcher(t, "foo.*")
	c := mustRegexConstraint(t, "test_col", m)
	require.Equal(t, "test_col", c.path())
	require.Contains(t, c.String(), "test_col")
}

func TestMatchersToConstraints(t *testing.T) {
	tests := []struct {
		name        string
		matchers    []*labels.Matcher
		wantLen     int
		expectError bool
	}{
		{
			name: "equal matcher",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
			},
			wantLen: 1,
		},
		{
			name: "not equal matcher",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "bar"),
			},
			wantLen: 1,
		},
		{
			name: "regex matcher",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar.*"),
			},
			wantLen: 1,
		},
		{
			name: "not regex matcher",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", "bar.*"),
			},
			wantLen: 1,
		},
		{
			name: "multiple matchers",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchRegexp, "baz", "qux.*"),
			},
			wantLen: 2,
		},
		{
			name: "everything matcher converted to nothing",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
			},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			constraints, err := MatchersToConstraints(tt.matchers...)
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, constraints, tt.wantLen)
		})
	}
}

// Mock constraint for testing constraint ordering
type mockConstraint struct {
	pathName string
}

func (m *mockConstraint) String() string { return fmt.Sprintf("mock(%s)", m.pathName) }
func (m *mockConstraint) path() string   { return m.pathName }
func (m *mockConstraint) init(f ParquetFileView) error {
	return nil
}
func (m *mockConstraint) filter(ctx context.Context, rgIdx int, primary bool, rr []RowRange) ([]RowRange, error) {
	return rr, nil
}

func (m *mockConstraint) prefilter(rgIdx int, rr []RowRange) ([]RowRange, error) {
	return rr, nil
}

type mockSortingColumn struct {
	pathName string
}

func (m *mockSortingColumn) Path() []string   { return []string{m.pathName} }
func (m *mockSortingColumn) Descending() bool { return false }
func (m *mockSortingColumn) NullsFirst() bool { return false }

func TestSortConstraintsBySortingColumns(t *testing.T) {
	tests := []struct {
		name           string
		sortingColumns []string
		constraints    []string
		expectedOrder  []string
	}{
		{
			name:           "no sorting columns",
			sortingColumns: []string{},
			constraints:    []string{"a", "b", "c"},
			expectedOrder:  []string{"a", "b", "c"}, // original order preserved
		},
		{
			name:           "single sorting column with matching constraint",
			sortingColumns: []string{"b"},
			constraints:    []string{"a", "b", "c"},
			expectedOrder:  []string{"b", "a", "c"}, // b moved to front
		},
		{
			name:           "multiple sorting columns with matching constraints",
			sortingColumns: []string{"c", "a"},
			constraints:    []string{"a", "b", "c", "d"},
			expectedOrder:  []string{"c", "a", "b", "d"}, // c first (sc[0]), then a (sc[1])
		},
		{
			name:           "multiple constraints per sorting column",
			sortingColumns: []string{"x", "y"},
			constraints:    []string{"a", "x", "b", "x", "y", "c"},
			expectedOrder:  []string{"x", "x", "y", "a", "b", "c"}, // all x constraints first, then y, then others
		},
		{
			name:           "sorting columns with no matching constraints",
			sortingColumns: []string{"x", "y"},
			constraints:    []string{"a", "b", "c"},
			expectedOrder:  []string{"a", "b", "c"}, // original order preserved
		},
		{
			name:           "mixed scenario",
			sortingColumns: []string{"col1", "col2", "col3"},
			constraints:    []string{"other1", "col2", "col1", "other2", "col1", "col3"},
			expectedOrder:  []string{"col1", "col1", "col2", "col3", "other1", "other2"}, // sorting cols by priority, then others
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sortingColumns []parquet.SortingColumn
			for _, colName := range tt.sortingColumns {
				sortingColumns = append(sortingColumns, &mockSortingColumn{pathName: colName})
			}

			var constraints []Constraint
			for _, constraintPath := range tt.constraints {
				constraints = append(constraints, &mockConstraint{pathName: constraintPath})
			}

			sortConstraintsBySortingColumns(constraints, sortingColumns)

			var actualOrder []string
			for _, c := range constraints {
				actualOrder = append(actualOrder, c.path())
			}

			require.Equal(t, tt.expectedOrder, actualOrder, "expected order %v, got %v", tt.expectedOrder, actualOrder)
		})
	}
}
