//  Copyright (c) 2026 The AtomAI.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ice

import (
	"math/rand/v2"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

// TestPostingsSubsetMatchesNext checks iterators whose Actual bitmap is a
// subset of the postings (deletions via except, or a conjunction's
// ReplaceActual) against a clean Next walk filtered to that subset.
func TestPostingsSubsetMatchesNext(t *testing.T) {
	for _, chunkMode := range []uint32{16, 1024, chunkModeV1} {
		seg := buildSkipSegment(t, 20000, chunkMode)
		all := allPostings(t, skipIterator(t, seg))
		rng := rand.New(rand.NewPCG(uint64(chunkMode), 7)) // #nosec G404 -- deterministic test data.
		keep := roaring.New()
		except := roaring.New()
		var want []plainPosting
		for _, p := range all {
			// #nosec G115 -- test documents number below 20000.
			if rng.IntN(5) == 0 {
				except.Add(uint32(p.doc)) // #nosec G115 -- see above.
				continue
			}
			keep.Add(uint32(p.doc)) // #nosec G115 -- see above.
			want = append(want, p)
		}

		dict, err := seg.Dictionary("body")
		if err != nil {
			t.Fatal(err)
		}
		subset := func(byExcept bool) *PostingsIterator {
			var ex *roaring.Bitmap
			if byExcept {
				ex = except
			}
			pl, err := dict.PostingsList([]byte("t"), ex, nil)
			if err != nil {
				t.Fatal(err)
			}
			it, err := pl.Iterator(true, true, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			pi := it.(*PostingsIterator)
			if !byExcept {
				pi.ReplaceActual(keep.Clone())
			}
			return pi
		}
		for _, byExcept := range []bool{true, false} {
			got := allPostings(t, subset(byExcept))
			if len(got) != len(want) {
				t.Fatalf("mode %d except=%v: %d postings, want %d", chunkMode, byExcept, len(got), len(want))
			}
			for k := range want {
				if got[k] != want[k] {
					t.Fatalf("mode %d except=%v: posting %d = %+v, want %+v", chunkMode, byExcept, k, got[k], want[k])
				}
			}
			for trial := range 20 {
				checkAdvance(t, subset(byExcept), want, rng, chunkMode, trial)
			}
			checkNextBatch(t, subset(byExcept), want, chunkMode)
		}
	}
}
