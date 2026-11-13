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
	"math"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

// docListCases are delta lists shaped like the riot5 workloads: a rare term
// of 100K documents (2-byte gaps), a facet of 10K (1-byte gaps) and lists
// that cross roaring containers.
var docListCases = []struct {
	name       string
	n, step, o int
}{
	{"rare100k", 146, 685, 42},
	{"facet10k", 625, 16, 7},
	{"cross3", 600, 331, 3},
	{"tiny", 15, 667, 1},
}

func docListOf(n, step, o int) (bm *roaring.Bitmap, list []byte) {
	bm = roaring.New()
	for i := range n {
		bm.Add(uint32(o + i*step)) // #nosec G115 -- small test values.
	}
	return bm, appendDocList(nil, bm)
}

func BenchmarkDocListBitmap(b *testing.B) {
	for _, tc := range docListCases {
		_, list := docListOf(tc.n, tc.step, tc.o)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := docListBitmap(list); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkOrListDense(b *testing.B) {
	for _, tc := range docListCases {
		bm, list := docListOf(tc.n, tc.step, tc.o)
		dense := make([]uint64, int(bm.Maximum())/64+1)
		b.Run(tc.name, func(b *testing.B) {
			for b.Loop() {
				if !orListDense(dense, list) {
					b.Fatal("orListDense failed")
				}
			}
		})
	}
}

// A delta list read twice converts once, and a bitmap read from the shared
// buffer copies a container before changing it, so the next read is intact.
func TestDocListCacheShared(t *testing.T) {
	want, list := docListOf(600, 331, 3)
	var c docListCache
	a, err := c.bitmap(7, list)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := c.bitmap(7, list); &b[0] != &a[0] {
		t.Fatal("second read converted the list again")
	}
	first := roaring.New()
	if _, err = first.FromBuffer(a); err != nil {
		t.Fatal(err)
	}
	first.Add(4)
	first.Remove(3)
	first.Or(roaring.BitmapOf(5, 1<<17))
	b, _ := c.bitmap(7, list)
	again := roaring.New()
	if _, err = again.FromBuffer(b); err != nil {
		t.Fatal(err)
	}
	if !again.Equals(want) {
		t.Fatal("changing a bitmap read from the cache changed the cached buffer")
	}
	// another list in the same slot replaces it, never aliases it
	other, otherList := docListOf(10, 9, 1)
	for pos := uint64(8); ; pos++ {
		if docListSlot(pos) != docListSlot(7) {
			continue
		}
		o, err := c.bitmap(pos, otherList)
		if err != nil {
			t.Fatal(err)
		}
		got := roaring.New()
		if _, err = got.FromBuffer(o); err != nil || !got.Equals(other) {
			t.Fatalf("slot collision read %v, %v", got, err)
		}
		break
	}
	if _, err := c.bitmap(9, []byte{5, 0x80}); err == nil {
		t.Fatal("cached a bad list")
	}
}

// docListSlot keeps the top docListSlotBits bits of the hash, so every offset
// maps inside the cache.
func TestDocListSlotRange(t *testing.T) {
	for _, pos := range []uint64{0, 1, 7, 1 << 32, math.MaxUint64} {
		if got, want := docListSlot(pos), (pos*0x9E3779B97F4A7C15)>>58; got != want || got >= docListSlots {
			t.Fatalf("docListSlot(%d) = %d, want %d below %d", pos, got, want, docListSlots)
		}
	}
}

// decodeDocList and orListDense agree with the bitmap for every case,
// including 1, 2 and 3 byte gaps.
func TestDocListDecodeCases(t *testing.T) {
	for _, tc := range append(docListCases, struct {
		name       string
		n, step, o int
	}{"wide gaps", 40, 70_000, 5}) {
		bm, list := docListOf(tc.n, tc.step, tc.o)
		var docs [maxListDocs]uint32
		n, err := decodeDocList(list, &docs)
		if err != nil || !roaring.BitmapOf(docs[:n]...).Equals(bm) {
			t.Fatalf("%s: decodeDocList %d docs, %v", tc.name, n, err)
		}
		dense, want := make([]uint64, int(bm.Maximum())/64+1), make([]uint64, int(bm.Maximum())/64+1)
		bm.WriteDenseTo(want)
		if !orListDense(dense, list) {
			t.Fatalf("%s: orListDense failed", tc.name)
		}
		for i := range dense {
			if dense[i] != want[i] {
				t.Fatalf("%s: orListDense word %d differs", tc.name, i)
			}
		}
	}
}
