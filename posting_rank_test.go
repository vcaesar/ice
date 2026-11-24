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
	"bytes"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

// rankIterator is an iterator over postings bm of a numDocs segment.
func rankIterator(bm *roaring.Bitmap, numDocs uint64) *PostingsIterator {
	return &PostingsIterator{postings: &PostingsList{postings: bm, sb: &Segment{footer: &footer{numDocs: numDocs}}}}
}

// TestCountPostingsBadMaximumStaysOnBitmap: postings past the segment's
// documents never get words.
func TestCountPostingsBadMaximumStaysOnBitmap(t *testing.T) {
	bm := roaring.BitmapOf(1, 5, 9000)
	it := rankIterator(bm, 100)
	for range 1000 {
		if got := it.countPostings(bm, 0, 6); got != 2 {
			t.Fatalf("countPostings = %d, want 2", got)
		}
	}
	if it.rankBuilt {
		t.Fatal("rankDense built for postings past the segment")
	}
}

// TestCountPostingsMatchesCardinality checks countPostings against roaring
// before and after rankDense is built, over array, bitmap and run
// containers, single-word, multi-word and wide ranges.
func TestCountPostingsMatchesCardinality(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 9)) // #nosec G404 -- deterministic test data.
	bm := roaring.New()
	for d := uint32(0); d < 200000; d++ {
		switch {
		case d < 70000: // sparse: array containers
			if rng.IntN(20) == 0 {
				bm.Add(d)
			}
		case d < 140000: // half full: bitmap containers
			if rng.IntN(2) == 0 {
				bm.Add(d)
			}
		default: // runs
			if d%1000 < 900 {
				bm.Add(d)
			}
		}
	}
	bm.RunOptimize()
	it := rankIterator(bm, 200000)
	maxDoc := uint64(bm.Maximum()) + 1
	for k := range 20000 {
		lo := rng.Uint64N(maxDoc)
		var hi uint64
		switch k % 3 {
		case 0:
			hi = lo + rng.Uint64N(64)
		case 1:
			hi = lo + rng.Uint64N(64*maxRankGapWords)
		default:
			hi = lo + rng.Uint64N(maxDoc)
		}
		hi = min(hi, maxDoc)
		if got, want := it.countPostings(bm, lo, hi), bm.CardinalityInRange(lo, hi); got != want {
			t.Fatalf("seek %d (built %v): countPostings(%d, %d) = %d, want %d", k, it.rankBuilt, lo, hi, got, want)
		}
	}
	if !it.rankBuilt {
		t.Fatal("rankDense was never built")
	}
	if got := it.countPostings(bm, 10, 10); got != 0 {
		t.Fatalf("empty range = %d, want 0", got)
	}
	if got, want := it.countPostings(bm, 0, maxDoc+500), bm.GetCardinality(); got != want {
		t.Fatalf("range past the end = %d, want %d", got, want)
	}
}

// TestCountPostingsLargeListStaysOnBitmap checks that a list too large for
// rankDense keeps counting with the bitmap.
func TestCountPostingsLargeListStaysOnBitmap(t *testing.T) {
	bm := roaring.BitmapOf(1, 5, uint32(maxRankWords*64+10))
	it := rankIterator(bm, maxRankWords*64+11)
	for range 1000 {
		if got := it.countPostings(bm, 0, 6); got != 2 {
			t.Fatalf("countPostings = %d, want 2", got)
		}
	}
	if it.rankBuilt {
		t.Fatal("rankDense built for a list over maxRankWords")
	}
}

// TestNextBatchWordsMixedWithAdvance checks NextBatch's word scan, mixed
// with Advance, Next and SkipBlocks, against a plain Next walk.
//
//nolint:gocyclo,funlen // one randomized walk over every read method.
func TestNextBatchWordsMixedWithAdvance(t *testing.T) {
	for _, chunkMode := range []uint32{16, 1024, chunkModeV1} {
		seg := buildSkipSegment(t, 20000, chunkMode)
		want := allPostings(t, skipIterator(t, seg))
		at := make(map[uint64]int, len(want))
		for k, p := range want {
			at[p.doc] = k
		}
		rng := rand.New(rand.NewPCG(uint64(chunkMode), 5)) // #nosec G404 -- deterministic test data.
		for trial := range 20 {
			it := skipIterator(t, seg)
			docs, freqs, norms := make([]uint64, 64), make([]int, 64), make([]float64, 64)
			next := 0 // index in want of the next posting
			expect := func(op string, got plainPosting) {
				t.Helper()
				if next >= len(want) || got != want[next] {
					t.Fatalf("mode %d trial %d %s: got %+v at %d (built %v)", chunkMode, trial, op, got, next, it.rankBuilt)
				}
				next++
			}
			for next < len(want) {
				switch op := rng.IntN(8); {
				case op < 5 || !it.rankBuilt:
					n, err := it.NextBatch(docs[:1+rng.IntN(64)], freqs, norms)
					if err != nil {
						t.Fatal(err)
					}
					if n == 0 && next < len(want) {
						t.Fatalf("mode %d trial %d: NextBatch ended at %d of %d", chunkMode, trial, next, len(want))
					}
					for k := range n {
						expect("NextBatch", plainPosting{docs[k], freqs[k], norms[k]})
					}
				case op < 7:
					target := want[next].doc + rng.Uint64N(300)
					p, err := it.Advance(target)
					if err != nil {
						t.Fatal(err)
					}
					for next < len(want) && want[next].doc < target {
						next++
					}
					if p == nil {
						if next != len(want) {
							t.Fatalf("mode %d trial %d: Advance(%d) = end", chunkMode, trial, target)
						}
						continue
					}
					expect("Advance", plainPosting{p.Number(), p.Frequency(), p.Norm()})
				default:
					p, err := it.Next()
					if err != nil || p == nil {
						t.Fatalf("mode %d trial %d: Next = %v, %v", chunkMode, trial, p, err)
					}
					expect("Next", plainPosting{p.Number(), p.Frequency(), p.Norm()})
				}
			}
			if !it.rankBuilt {
				t.Fatalf("mode %d trial %d: word scan never used", chunkMode, trial)
			}
		}
		// skipping every block from the word scan's position ends the list
		it := skipIterator(t, seg)
		for !it.rankBuilt {
			if n, err := it.NextBatch(make([]uint64, 64), make([]int, 64), make([]float64, 64)); err != nil || n == 0 {
				t.Fatalf("NextBatch = %d, %v", n, err)
			}
		}
		read := 0
		for it.Actual.HasNext() && uint64(it.Actual.PeekNext()) > want[read].doc {
			read++
		}
		skipped, err := it.SkipBlocks(func(int, float64) bool { return false })
		if err != nil {
			t.Fatal(err)
		}
		if read+int(skipped) != len(want) { // #nosec G115 -- a small count.
			t.Fatalf("mode %d: read %d + skipped %d, want %d", chunkMode, read, skipped, len(want))
		}
		if n, err := it.NextBatch(make([]uint64, 8), make([]int, 8), make([]float64, 8)); err != nil || n != 0 {
			t.Fatalf("NextBatch after SkipBlocks = %d, %v", n, err)
		}
	}
}

// TestNextBatchSubsetWords checks nextBatchSubset's word scan, mixed with
// Advance, against the subset of a plain Next walk.
func TestNextBatchSubsetWords(t *testing.T) {
	for _, chunkMode := range []uint32{16, 1024, chunkModeV1} {
		seg := buildSkipSegment(t, 20000, chunkMode)
		all := allPostings(t, skipIterator(t, seg))
		rng := rand.New(rand.NewPCG(uint64(chunkMode), 11)) // #nosec G404 -- deterministic test data.
		sub := roaring.New()
		var want []plainPosting
		for _, p := range all {
			if rng.IntN(3) != 0 {
				sub.Add(uint32(p.doc)) // #nosec G115 -- a test document number.
				want = append(want, p)
			}
		}
		it := skipIterator(t, seg)
		it.ReplaceActual(sub)
		docs, freqs, norms := make([]uint64, 50), make([]int, 50), make([]float64, 50)
		next := 0
		for next < len(want) {
			if it.actualBuilt && rng.IntN(4) == 0 {
				target := want[next].doc + rng.Uint64N(200)
				p, err := it.Advance(target)
				if err != nil {
					t.Fatal(err)
				}
				for next < len(want) && want[next].doc < target {
					next++
				}
				if p == nil {
					break
				}
				if got := (plainPosting{p.Number(), p.Frequency(), p.Norm()}); got != want[next] {
					t.Fatalf("mode %d: Advance(%d) = %+v, want %+v", chunkMode, target, got, want[next])
				}
				next++
				continue
			}
			n, err := it.NextBatch(docs, freqs, norms)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				t.Fatalf("mode %d: NextBatch ended at %d of %d", chunkMode, next, len(want))
			}
			for k := range n {
				if got := (plainPosting{docs[k], freqs[k], norms[k]}); got != want[next] {
					t.Fatalf("mode %d: posting %d = %+v, want %+v (built %v)", chunkMode, next, got, want[next], it.actualBuilt)
				}
				next++
			}
		}
		if next != len(want) || !it.actualBuilt {
			t.Fatalf("mode %d: read %d of %d, word scan used %v", chunkMode, next, len(want), it.actualBuilt)
		}
		it.ReplaceActual(roaring.BitmapOf(uint32(want[0].doc))) // #nosec G115 -- a test document number.
		if it.actualBuilt {
			t.Fatal("ReplaceActual kept the old subset words")
		}
	}
}

// TestNextBatchWordsEmptyBatch: reading into no room consumes no posting,
// from the postings and from a subset of them.
func TestNextBatchWordsEmptyBatch(t *testing.T) {
	seg := buildSkipSegment(t, 20000, 1024)
	want := allPostings(t, skipIterator(t, seg))
	sub := roaring.New()
	for _, p := range want {
		sub.Add(uint32(p.doc)) // #nosec G115 -- a test document number.
	}
	for _, subset := range []bool{false, true} {
		it := skipIterator(t, seg)
		if subset {
			it.ReplaceActual(sub)
		}
		docs, freqs, norms := make([]uint64, 64), make([]int, 64), make([]float64, 64)
		next := 0
		for !it.rankBuilt && !it.actualBuilt {
			n, err := it.NextBatch(docs, freqs, norms)
			if err != nil || n == 0 {
				t.Fatalf("subset %v: NextBatch = %d, %v", subset, n, err)
			}
			next += n
		}
		if n, err := it.NextBatch(docs[:0], freqs[:0], norms[:0]); n != 0 || err != nil {
			t.Fatalf("subset %v: empty NextBatch = %d, %v", subset, n, err)
		}
		n, err := it.NextBatch(docs, freqs, norms)
		if err != nil || n == 0 {
			t.Fatalf("subset %v: NextBatch = %d, %v", subset, n, err)
		}
		if got := (plainPosting{docs[0], freqs[0], norms[0]}); got != want[next] {
			t.Fatalf("subset %v: after an empty batch got %+v, want %+v", subset, got, want[next])
		}
	}
}

// TestSubsetWordsPastSegmentStaysOnBitmap: a subset holding a document past
// the segment's is read with the roaring iterator, and a list without a
// footer never gets words.
func TestSubsetWordsPastSegmentStaysOnBitmap(t *testing.T) {
	seg := buildSkipSegment(t, 20000, 1024)
	want := allPostings(t, skipIterator(t, seg))[:200]
	sub := roaring.BitmapOf(20000 + 64)
	for _, p := range want {
		sub.Add(uint32(p.doc)) // #nosec G115 -- a test document number.
	}
	it := skipIterator(t, seg)
	it.ReplaceActual(sub)
	docs, freqs, norms := make([]uint64, 50), make([]int, 50), make([]float64, 50)
	for next := 0; next < len(want); {
		n, err := it.NextBatch(docs, freqs, norms)
		if err != nil || n != len(docs) {
			t.Fatalf("NextBatch = %d, %v", n, err)
		}
		for k := range n {
			if got := (plainPosting{docs[k], freqs[k], norms[k]}); got != want[next] {
				t.Fatalf("posting %d = %+v, want %+v", next, got, want[next])
			}
			next++
		}
	}
	if it.actualBuilt || it.batchRead*batchWordsPerRead < it.segmentWords() {
		t.Fatalf("subset words built %v after %d postings", it.actualBuilt, it.batchRead)
	}

	bm := roaring.BitmapOf(1, 5, 9)
	bare := &PostingsIterator{postings: &PostingsList{postings: bm}}
	for range 1000 {
		if got := bare.countPostings(bm, 0, 6); got != 2 {
			t.Fatalf("countPostings = %d, want 2", got)
		}
	}
	if bare.rankBuilt {
		t.Fatal("rankDense built without a footer")
	}
}

// TestNextBatchWordsErrors: the word scan reports unreadable chunks and
// freq/norm entries.
func TestNextBatchWordsErrors(t *testing.T) {
	freq := binary.AppendUvarint(nil, encodeFreqHasLocs(1, false))
	overflow := append(bytes.Repeat([]byte{0xff}, binary.MaxVarintLen64), 1)
	for _, tc := range []struct {
		name   string
		data   []byte // the loaded chunk; nil: none, and none can be loaded
		subset bool
	}{
		{"load chunk", nil, false},
		{"seek", nil, true},
		{"freq", overflow, false},
		{"norm", append(slices.Clone(freq), overflow...), false},
		{"norm bits", binary.AppendUvarint(slices.Clone(freq), math.MaxUint32+1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bm := roaring.BitmapOf(3, 70)
			it := rankIterator(bm, 128)
			it.postings.chunkSize = 1024
			it.includeFreqNorm, it.ActualBM, it.batchRead = true, bm, 1
			it.freqNormReader = &chunkedIntDecoder{startOffset: 1} // encoded, without chunks
			if tc.data != nil {
				it.freqNormReader.curChunkBytes, it.freqNormReader.r = tc.data, newMemUvarintReader(tc.data)
				it.chunkEnd = it.postings.chunkSize
			}
			if tc.subset {
				it.ActualBM = roaring.BitmapOf(70)
			}
			n, err := it.NextBatch(make([]uint64, 4), make([]int, 4), make([]float64, 4))
			if err == nil || n != 0 {
				t.Fatalf("NextBatch = %d, %v", n, err)
			}
			if !it.rankBuilt && !it.actualBuilt {
				t.Fatal("the word scan was not used")
			}
		})
	}
}
