// Copyright (c) 2026 The AtomAI.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ice

import (
	"encoding/binary"
	"math"
	"reflect"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

// locBatchPostings drains it with NextLocBatch in batches of size.
func locBatchPostings(t *testing.T, it *PostingsIterator, size int) []locPosting {
	t.Helper()
	docs, freqs, norms, ends := make([]uint64, size), make([]int, size), make([]float64, size), make([]int, size)
	var rv []locPosting
	var fields []string
	var locs []int
	for {
		n, f, l, err := it.NextLocBatch(docs, freqs, norms, ends, fields[:0], locs[:0])
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return rv
		}
		fields, locs = f, l
		lo := 0
		for k := range n {
			lp := locPosting{doc: docs[k], freq: freqs[k], norm: norms[k]}
			for j := lo; j < ends[k]; j++ {
				lp.locs = append(lp.locs, plainLoc{fields[j], locs[3*j], locs[3*j+1], locs[3*j+2]})
			}
			lo = ends[k]
			rv = append(rv, lp)
		}
	}
}

func locIterator(t *testing.T, seg *Segment, field, term string) *PostingsIterator {
	t.Helper()
	dict, err := seg.Dictionary(field)
	if err != nil {
		t.Fatal(err)
	}
	pl, err := dict.PostingsList([]byte(term), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	it, err := pl.Iterator(true, true, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	return it.(*PostingsIterator)
}

// NextLocBatch must read exactly what Next does, for compact and legacy
// locations, any batch size, subsets, after Advance and mixed with Next.
func TestNextLocBatchMatchesNext(t *testing.T) {
	for _, ver := range []uint32{versionBlockBounds, versionCompactPostings, Version} {
		seg := buildLocSegment(t, 700, ver)
		for _, ft := range [][2]string{{"body", "search"}, {"_all", "engine"}, {"body", "w5"}, {"_id", "17"}} {
			want := locPostings(t, seg, ft[0], ft[1])
			if len(want) == 0 {
				t.Fatalf("%v: no postings", ft)
			}
			for _, size := range []int{1, 7, 128} {
				if got := locBatchPostings(t, locIterator(t, seg, ft[0], ft[1]), size); !reflect.DeepEqual(got, want) {
					t.Fatalf("v%d %v size %d: batch read %d postings, Next %d, or they differ",
						ver, ft, size, len(got), len(want))
				}
			}

			// subset: every third posting dropped
			keep := roaring.New()
			var wantSub []locPosting
			for k, p := range want {
				if k%3 != 1 {
					keep.Add(uint32(p.doc))
					wantSub = append(wantSub, p)
				}
			}
			it := locIterator(t, seg, ft[0], ft[1])
			it.ReplaceActual(keep)
			if got := locBatchPostings(t, it, 5); !reflect.DeepEqual(got, wantSub) {
				t.Fatalf("v%d %v subset: batch read %d postings, want %d", ver, ft, len(got), len(wantSub))
			}

			// Advance, then Next, then batches
			if len(want) < 3 {
				continue
			}
			it = locIterator(t, seg, ft[0], ft[1])
			p, err := it.Advance(want[1].doc)
			if err != nil || p == nil || p.Number() != want[1].doc {
				t.Fatalf("advance: %v %v", p, err)
			}
			if p, err = it.Next(); err != nil || p == nil || p.Number() != want[2].doc {
				t.Fatalf("next after advance: %v %v", p, err)
			}
			if got := locBatchPostings(t, it, 3); !reflect.DeepEqual(got, want[3:]) && len(want) > 3 {
				t.Fatalf("v%d %v after advance: batch read %d postings, want %d", ver, ft, len(got), len(want)-3)
			}
		}
	}
}

// The inline location decode must validate records as readLocation does.
func TestNextLocBatchRejectsBadLocations(t *testing.T) {
	for _, tc := range []struct {
		name string
		locs []byte
		pos  int // wanted position when valid
		bad  bool
	}{
		{"valid", []byte{4, 0, 4, 6, 8}, 2, false},
		{"multi-byte", []byte{5, 0, 0x80, 1, 6, 8}, 64, false},
		{"field", []byte{4, 1, 4, 6, 8}, 0, true},
		{"negative-position", []byte{4, 0, 3, 6, 8}, 0, true},
		{"count-exceeds-freq", []byte{8, 0, 4, 6, 8, 0, 4, 6, 8}, 0, true},
		{"bytes-exceed-chunk", []byte{9, 0, 4, 6, 8}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn := binary.AppendUvarint(nil, encodeFreqHasLocs(1, true))
			fn = binary.AppendUvarint(fn, uint64(math.Float32bits(1)))
			// doc 0 was read; doc 1 is next in the loaded chunk
			bm := roaring.BitmapOf(0, 1)
			actual := bm.Iterator()
			actual.Next()
			it := &PostingsIterator{
				postings: &PostingsList{
					sb:        &Segment{footer: &footer{version: versionCompactPostings}, fieldsInv: []string{"body"}},
					postings:  bm,
					chunkSize: 1024,
				},
				ActualBM: bm, Actual: actual, started: true,
				includeFreqNorm: true, includeLocs: true, hasLast: true, chunkEnd: 1024,
				freqNormReader: &chunkedIntDecoder{r: newMemUvarintReader(fn)},
				locReader:      &chunkedIntDecoder{r: newMemUvarintReader(tc.locs)},
			}
			docs, freqs, norms, ends := make([]uint64, 1), make([]int, 1), make([]float64, 1), make([]int, 1)
			n, fields, locs, err := it.NextLocBatch(docs, freqs, norms, ends, nil, nil)
			if (err != nil) != tc.bad {
				t.Fatalf("error=%v want error=%v", err, tc.bad)
			}
			if err == nil && (n != 1 || docs[0] != 1 || ends[0] != 1 || fields[0] != "body" ||
				!reflect.DeepEqual(locs, []int{tc.pos, 3, 11})) {
				t.Fatalf("n=%d docs=%v fields=%v locs=%v", n, docs, fields, locs)
			}
		})
	}
}
