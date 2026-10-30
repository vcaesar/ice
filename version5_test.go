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
	got := allPostings(t, skipIterator(t, merged))
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

// TestBlockBoundsAndSkip checks the stored chunk bounds are exact and that
// SkipBlocks skips exactly the rejected chunks.
func TestBlockBoundsAndSkip(t *testing.T) {
	seg := persistedSkipSegment(t, Version, 20000)
	all := allPostings(t, skipIterator(t, seg))
	it := skipIterator(t, seg)
	r, cs := it.freqNormReader, it.postings.chunkSize
	if len(r.maxFreqs) < 2 {
		t.Fatalf("want several chunks, got %d", len(r.maxFreqs))
	}
	maxF := make([]uint64, len(r.maxFreqs))
	minN := make([]uint64, len(r.maxFreqs))
	for k := range minN {
		minN[k] = math.MaxUint64
	}
	for _, p := range all {
		c := p.doc / cs
		maxF[c] = max(maxF[c], uint64(p.freq))
		minN[c] = min(minN[c], uint64(math.Float32bits(float32(p.norm))))
	}
	for c := range maxF {
		if maxF[c] == 0 {
			continue // empty chunk
		}
		if r.maxFreqs[c] != maxF[c] || r.minNorms[c] != minN[c] {
			t.Fatalf("chunk %d bounds (%d,%d), want (%d,%d)", c, r.maxFreqs[c], r.minNorms[c], maxF[c], minN[c])
		}
	}

	// reject chunks without a freq-4 posting: the next posting must be the
	// first one in an accepted chunk, and the skip count everything before
	n, err := it.SkipBlocks(func(maxFreq int, _ float64) bool { return maxFreq >= 4 })
	if err != nil {
		t.Fatal(err)
	}
	first := 0
	for first < len(all) && maxF[all[first].doc/cs] < 4 {
		first++
	}
	if n != uint64(first) {
		t.Fatalf("skipped %d postings, want %d", n, first)
	}
	p, err := it.Next()
	if err != nil {
		t.Fatal(err)
	}
	if first == len(all) {
		if p != nil {
			t.Fatalf("got %d after skipping everything", p.Number())
		}
		return
	}
	if p == nil || (plainPosting{p.Number(), p.Frequency(), p.Norm()}) != all[first] {
		t.Fatalf("after skip got %v, want %+v", p, all[first])
	}

	it = skipIterator(t, seg)
	if n, err := it.SkipBlocks(func(int, float64) bool { return false }); err != nil || n != uint64(len(all)) {
		t.Fatalf("reject-all skipped %d, %v; want %d", n, err, len(all))
	}
	if p, err := it.Next(); err != nil || p != nil {
		t.Fatalf("Next after reject-all = %v, %v", p, err)
	}
}
