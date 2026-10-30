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
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

// persistedSkipSegment builds, persists and reloads a skip segment written
// as version ver.
func persistedSkipSegment(t *testing.T, ver uint32, numDocs int) *Segment {
	t.Helper()
	prev := writeVersion
	writeVersion = ver
	defer func() { writeVersion = prev }()
	mem := buildSkipSegment(t, numDocs, chunkModeV1)
	path := filepath.Join(t.TempDir(), "seg.ice")
	if err := persistToFile(mem, path); err != nil {
		t.Fatal(err)
	}
	seg, closeF, err := openFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := closeF(); err != nil {
			t.Error(err)
		}
	})
	if seg.Version() != ver {
		t.Fatalf("segment version %d, want %d", seg.Version(), ver)
	}
	return seg
}

// TestVersion4SegmentsStillRead checks a version 4 segment (no block
// bounds) reads the same postings as a version 5 one, and merging the two
// writes a version 5 segment with every posting intact.
func TestVersion4SegmentsStillRead(t *testing.T) {
	v4 := persistedSkipSegment(t, versionNumericDocValues, 6000)
	v5 := persistedSkipSegment(t, Version, 6000)
	want := allPostings(t, skipIterator(t, v5))
	if got := allPostings(t, skipIterator(t, v4)); !reflect.DeepEqual(got, want) {
		t.Fatalf("v4 read %d postings, v5 %d, or they differ", len(got), len(want))
	}
	if n, err := skipIterator(t, v4).SkipBlocks(func(int, float64) bool { return false }); err != nil || n != 0 {
		t.Fatalf("v4 SkipBlocks skipped %d, %v; want 0 (no bounds)", n, err)
	}

	path := filepath.Join(t.TempDir(), "merged.ice")
	if _, err := mergeSegments([]segment.Segment{v4, v5}, []*roaring.Bitmap{nil, nil}, path); err != nil {
		t.Fatal(err)
	}
	merged, closeF, err := openFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeF(); err != nil {
			t.Error(err)
		}
	}()
	if merged.Version() != Version {
		t.Fatalf("merged version %d, want %d", merged.Version(), Version)
	}
	got, _ := checkBounds(t, merged)
	if len(got) != 2*len(want) {
		t.Fatalf("merged %d postings, want %d", len(got), 2*len(want))
	}
	for k, p := range want {
		q := got[len(want)+k]
		q.doc -= 6000
		if got[k] != p || q != p {
			t.Fatalf("merged posting %d = %+v / %+v, want %+v", k, got[k], q, p)
		}
	}
}

// checkBounds checks the stored chunk bounds of seg's "t" postings are the
// exact max freq and min norm bits of each chunk, and (0,0) for empty
// chunks. It returns the postings and the chunk size.
func checkBounds(t *testing.T, seg *Segment) (all []plainPosting, cs uint64) {
	t.Helper()
	all = allPostings(t, skipIterator(t, seg))
	it := skipIterator(t, seg)
	r := it.freqNormReader
	cs = it.postings.chunkSize
	if len(r.maxFreqs) != len(r.chunkOffsets) || len(r.minNorms) != len(r.chunkOffsets) {
		t.Fatalf("%d/%d bounds for %d chunks", len(r.maxFreqs), len(r.minNorms), len(r.chunkOffsets))
	}
	maxF, minN := make([]uint64, len(r.maxFreqs)), make([]uint64, len(r.maxFreqs))
	seen := make([]bool, len(r.maxFreqs))
	for _, p := range all {
		c := p.doc / cs
		if c >= uint64(len(seen)) {
			t.Fatalf("doc %d past the %d chunks", p.doc, len(seen))
		}
		f := uint64(p.freq) // #nosec G115 -- fixture freqs are 1..4.
		n := uint64(math.Float32bits(float32(p.norm)))
		if !seen[c] {
			maxF[c], minN[c], seen[c] = f, n, true
		}
		maxF[c], minN[c] = max(maxF[c], f), min(minN[c], n)
	}
	for c := range maxF {
		if r.maxFreqs[c] != maxF[c] || r.minNorms[c] != minN[c] {
			t.Fatalf("chunk %d bounds (%d,%d), want (%d,%d)", c, r.maxFreqs[c], r.minNorms[c], maxF[c], minN[c])
		}
	}
	return all, cs
}

// expectSkip returns the index in all of the next posting after reading
// pre postings and then skipping the rest of rejects chunks.
func expectSkip(all []plainPosting, cs uint64, pre, rejects int) int {
	pos := pre
	for range rejects {
		if pos == len(all) {
			break
		}
		c := all[pos].doc / cs
		for pos < len(all) && all[pos].doc/cs == c {
			pos++
		}
	}
	return pos
}

// TestBlockBoundsAndSkip checks the stored chunk bounds are exact and that
// SkipBlocks skips exactly the rejected chunks, from a chunk start or mid
// chunk, leaving the iterator in sync for the postings after.
func TestBlockBoundsAndSkip(t *testing.T) {
	seg := persistedSkipSegment(t, Version, 20000)
	all, cs := checkBounds(t, seg)
	if all[len(all)-1].doc/cs < 3 {
		t.Fatalf("want several chunks, got %d", all[len(all)-1].doc/cs+1)
	}
	for _, tc := range []struct {
		name         string
		pre, rejects int
	}{
		{"none", 0, 0},
		{"first two", 0, 2},
		{"mid chunk", 3, 1},
		{"mid chunk then two", 3, 3},
		{"all", 0, len(all)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			it := skipIterator(t, seg)
			for range tc.pre {
				if p, err := it.Next(); err != nil || p == nil {
					t.Fatalf("Next = %v, %v", p, err)
				}
			}
			calls := 0
			n, err := it.SkipBlocks(func(int, float64) bool {
				calls++
				return calls > tc.rejects
			})
			if err != nil {
				t.Fatal(err)
			}
			pos := expectSkip(all, cs, tc.pre, tc.rejects)
			if n != uint64(pos-tc.pre) { // #nosec G115 -- expectSkip never moves pos before pre.
				t.Fatalf("skipped %d postings, want %d", n, pos-tc.pre)
			}
			if rest := allPostings(t, it); !slices.Equal(rest, all[pos:]) {
				t.Fatalf("after skip read %d postings, want %d, or they differ", len(rest), len(all)-pos)
			}
		})
	}
}

// TestSkipBlocksAndNextBatchAfterAdvancePastEnd checks an iterator advanced
// past the uint32 doc range reports the end instead of panicking.
func TestSkipBlocksAndNextBatchAfterAdvancePastEnd(t *testing.T) {
	seg := persistedSkipSegment(t, Version, 3000)
	it := skipIterator(t, seg)
	if p, err := it.Advance(math.MaxUint32 + 1); err != nil || p != nil {
		t.Fatalf("Advance past end = %v, %v", p, err)
	}
	if n, err := it.SkipBlocks(func(int, float64) bool { return false }); err != nil || n != 0 {
		t.Fatalf("SkipBlocks = %d, %v", n, err)
	}
	docs, freqs, norms := make([]uint64, 8), make([]int, 8), make([]float64, 8)
	if n, err := it.NextBatch(docs, freqs, norms); err != nil || n != 0 {
		t.Fatalf("NextBatch = %d, %v", n, err)
	}
}
