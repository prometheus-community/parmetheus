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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsResourceExhausted(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "resource exhausted error",
			err:      &resourceExhausted{used: 100},
			expected: true,
		},
		{
			name:     "wrapped resource exhausted error",
			err:      fmt.Errorf("wrapped: %w", &resourceExhausted{used: 50}),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsResourceExhausted(tt.err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestQuota_Reserve(t *testing.T) {
	tests := []struct {
		name         string
		quotaLimit   int64
		reservations []int64
		expectError  bool
	}{
		{
			name:         "reserve within limit",
			quotaLimit:   100,
			reservations: []int64{30, 40, 20},
			expectError:  false,
		},
		{
			name:         "reserve exact limit",
			quotaLimit:   100,
			reservations: []int64{100},
			expectError:  false,
		},
		{
			name:         "reserve beyond limit",
			quotaLimit:   100,
			reservations: []int64{50, 60},
			expectError:  true,
		},
		{
			name:         "reserve zero amount",
			quotaLimit:   100,
			reservations: []int64{0},
			expectError:  false,
		},
		{
			name:         "reserve negative amount",
			quotaLimit:   100,
			reservations: []int64{-10},
			expectError:  false,
		},
		{
			name:         "unlimited quota",
			quotaLimit:   0,
			reservations: []int64{1000, 2000, 3000},
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			quota := NewQuota(tt.quotaLimit)
			var lastErr error

			for _, amount := range tt.reservations {
				err := quota.Reserve(amount)
				if err != nil {
					lastErr = err
					require.True(t, IsResourceExhausted(err), "Expected resource exhausted error")
					break
				}
			}

			if tt.expectError {
				require.Error(t, lastErr, "Expected error but got none")
			} else {
				require.NoError(t, lastErr, "Unexpected error")
			}
		})
	}
}

func TestQuota_ConcurrentReserve(t *testing.T) {
	quota := NewQuota(1000)
	const numGoroutines = 10
	const reservationAmount = 100

	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			err := quota.Reserve(reservationAmount)
			errChan <- err
		}()
	}

	var errors []error
	for i := 0; i < numGoroutines; i++ {
		err := <-errChan
		errors = append(errors, err)
	}

	successCount := 0
	exhaustedCount := 0
	for _, err := range errors {
		if err == nil {
			successCount++
		} else if IsResourceExhausted(err) {
			exhaustedCount++
		} else {
			require.Fail(t, "Unexpected error type", "error: %v", err)
		}
	}

	expectedSuccess := 10
	require.Equal(t, expectedSuccess, successCount, "Expected %d successful reservations", expectedSuccess)
	require.Equal(t, 0, exhaustedCount, "Expected 0 exhausted errors")

	err := quota.Reserve(1)
	require.Error(t, err, "Expected error when reserving beyond quota limit")
	require.True(t, IsResourceExhausted(err), "Expected resource exhausted error")
}
