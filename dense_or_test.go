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
	"strconv"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

// TestOrRoaringDense checks the direct decoder against roaring for every
// container kind, ORing into a bitset that already holds other bits.
func TestOrRoaringDense(t *testing.T) {
	const numDocs = 5 * 65536
	rng := rand.New(rand.NewPCG(1, 2)) // #nosec G404 -- deterministic test data.
	sparse := func(lo, hi, n int) []uint32 {
		v := make([]uint32, n)
		for i := range v {
			v[i] = uint32(lo + rng.IntN(hi-lo)) // #nosec G115 -- values are below numDocs.
		}
		return v
	}
	for _, tt := range []struct {
		name  string
		build func(bm *roaring.Bitmap)
	}{
		{"empty", func(*roaring.Bitmap) {}},
		{"array", func(bm *roaring.Bitmap) { bm.AddMany(sparse(0, 65536, 300)) }},
		{"bitmap", func(bm *roaring.Bitmap) { bm.AddMany(sparse(65536, 2*65536, 20000)) }},
		{"run", func(bm *roaring.Bitmap) { bm.AddRange(70000, 90000) }},
		{"mixed", func(bm *roaring.Bitmap) {
			bm.AddMany(sparse(0, 65536, 100))
			bm.AddMany(sparse(65536, 2*65536, 9000))
			bm.AddRange(2*65536+5, 2*65536+700)
			bm.AddRange(3*65536, 3*65536+10)
			bm.AddMany(sparse(4*65536, numDocs, 50))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bm := roaring.New()
			tt.build(bm)
			bm.RunOptimize()
			buf, err := bm.ToBytes()
			if err != nil {
				t.Fatal(err)
			}
			prior := sparse(0, numDocs, 1000)
			want := make([]uint64, (numDocs+63)/64)
			got := make([]uint64, len(want))
			for _, v := range append(prior, bm.ToArray()...) {
				want[v/64] |= 1 << (v % 64)
			}
			for _, v := range prior {
				got[v/64] |= 1 << (v % 64)
			}
			if n, ok := orRoaringDense(got, buf); !ok || n != bm.GetCardinality() {
				t.Fatalf("orRoaringDense = %d %v, want %d true", n, ok, bm.GetCardinality())
			}
			for k := range want {
				if got[k] != want[k] {
					t.Fatalf("word %d = %x, want %x", k, got[k], want[k])
				}
			}
			if !bm.IsEmpty() {
				if _, ok := orRoaringDense(make([]uint64, bm.Maximum()/64), buf); ok {
					t.Fatal("accepted a bitmap past the bitset")
				}
			}
			if _, ok := orRoaringDense(make([]uint64, len(want)), buf[:len(buf)-1]); len(buf) > 8 && ok {
				t.Fatal("accepted a truncated bitmap")
			}
		})
	}
	if _, ok := orRoaringDense(make([]uint64, 4), []byte{1, 2, 3, 4, 0, 0, 0, 0}); ok {
		t.Fatal("accepted an unknown cookie")
	}
}

func TestDenseFull(t *testing.T) {
	for _, tt := range []struct {
		name    string
		dense   []uint64
		numDocs uint64
		want    bool
	}{
		{"empty", nil, 0, true},
		{"whole words", []uint64{^uint64(0), ^uint64(0)}, 128, true},
		{"tail set", []uint64{^uint64(0), 0b111}, 67, true},
		{"tail missing", []uint64{^uint64(0), 0b101}, 67, false},
		{"hole", []uint64{^uint64(0) >> 1, 0b111}, 67, false},
		{"short bitset", []uint64{^uint64(0)}, 65, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			from := 0
			if got := denseFull(tt.dense, tt.numDocs, &from); got != tt.want {
				t.Fatalf("denseFull = %v, want %v", got, tt.want)
			}
		})
	}
	dense := []uint64{^uint64(0), 0, 0}
	from := 0
	if denseFull(dense, 150, &from) || from != 1 {
		t.Fatalf("partial: from = %d, want 1", from)
	}
	dense[1], dense[2] = ^uint64(0), 1<<22-1
	if !denseFull(dense, 150, &from) || from != 2 {
		t.Fatalf("filled: from = %d, want 2", from)
	}
}

// TestOrPostingsDenseStopsWhenFull ORs a range whose first term is in every
// document: the result is every document whichever kind of container that
// term is stored in.
func TestOrPostingsDenseStopsWhenFull(t *testing.T) {
	for _, numDocs := range []int{1000, 5000, 70000} {
		t.Run(strconv.Itoa(numDocs), func(t *testing.T) {
			docs := make([]segment.Document, numDocs)
			for i := range docs {
				docs[i] = &FakeDocument{
					NewFakeField("_id", strconv.Itoa(i), true, false, false),
					NewFakeField("body", "p p"+strconv.Itoa(i%13), false, true, false),
				}
			}
			seg, _, err := newWithChunkMode(docs, encodeNorm, chunkModeV1, DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
			d, err := seg.(*Segment).dictionary("body")
			if err != nil {
				t.Fatal(err)
			}
			start, end := []byte("p"), []byte("q")
			want := make([]uint64, (numDocs+63)/64)
			termUnion(t, d, start, end, want)
			got := make([]uint64, len(want))
			ok, err := d.OrPostingsDense(nil, start, end, got)
			if err != nil || !ok {
				t.Fatalf("OrPostingsDense: %v %v", ok, err)
			}
			from := 0
			if !denseFull(got, uint64(numDocs), &from) { // #nosec G115 -- numDocs is positive.
				t.Fatal("not every document set")
			}
			for k := range want {
				if got[k] != want[k] {
					t.Fatalf("word %d = %x, want %x", k, got[k], want[k])
				}
			}
		})
	}
}
