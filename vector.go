// Copyright (c) 2026 The Riot Authors.
//
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

package ice

import (
	"container/heap"
	"context"
	"fmt"
	"sort"

	"github.com/vcaesar/ice/vec"
)

// SearchVectors exhaustively searches encoded vectors in stored field values.
// Repeated fields contribute their best score per document. Results are ordered
// by descending score, then ascending segment-local document number. accept is
// an optional prefilter; callers must use it to exclude live-index deletions.
// Every matching value in an accepted document must be valid, even outside top k.
func (s *Segment) SearchVectors(ctx context.Context, field string, query []float32, k int,
	metric vec.Metric, accept func(uint64) bool) ([]vec.Match, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if k <= 0 {
		return nil, fmt.Errorf("vector search k must be positive")
	}
	if err := vec.Validate(query, metric); err != nil {
		return nil, err
	}
	var top vectorHeap
	for number := uint64(0); number < s.Count(); number++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if accept != nil && !accept(number) {
			continue
		}
		var best float64
		var found bool
		var valueErr error
		err := s.VisitStoredFields(number, func(name string, data []byte) bool {
			if valueErr = ctx.Err(); valueErr != nil {
				return false
			}
			if name != field {
				return true
			}
			var vector []float32
			vector, valueErr = vec.Decode(data)
			if valueErr != nil {
				return false
			}
			var score float64
			score, valueErr = vec.Score(query, vector, metric)
			if valueErr != nil {
				return false
			}
			if !found || score > best {
				best, found = score, true
			}
			return true
		})
		if err != nil {
			return nil, fmt.Errorf("vector search document %d: %w", number, err)
		}
		if valueErr != nil {
			return nil, fmt.Errorf("vector search document %d field %q: %w", number, field, valueErr)
		}
		if !found {
			continue
		}
		match := vec.Match{Number: number, Score: best}
		if len(top) < k {
			heap.Push(&top, match)
		} else if vectorBetter(match, top[0]) {
			top[0] = match
			heap.Fix(&top, 0)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.Slice(top, func(i, j int) bool { return vectorBetter(top[i], top[j]) })
	return []vec.Match(top), nil
}

func vectorBetter(a, b vec.Match) bool {
	return a.Score > b.Score || (a.Score == b.Score && a.Number < b.Number)
}

// The worst retained match is at the root.
type vectorHeap []vec.Match

func (h vectorHeap) Len() int            { return len(h) }
func (h vectorHeap) Less(i, j int) bool  { return vectorBetter(h[j], h[i]) }
func (h vectorHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *vectorHeap) Push(x interface{}) { *h = append(*h, x.(vec.Match)) }
func (h *vectorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
