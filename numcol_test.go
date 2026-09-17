//  Copyright (c) 2026 The Bluge Authors.
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
	"math"
	"math/rand"
	"testing"

	segment "github.com/vcaesar/bluge_segment_api"
)

type numEntry struct {
	doc uint64
	val int64
}

func roundTripColumn(t *testing.T, numDocs uint64, entries []numEntry) (*Segment, *numericColumn) {
	t.Helper()
	w := newNumericColumnWriter(numDocs)
	for _, e := range entries {
		if err := w.Add(e.doc, e.val); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	buf.WriteString("prefix") // column need not start at 0
	n, err := w.Write(&buf)
	if err != nil {
		t.Fatal(err)
	}
	s := &Segment{data: segment.NewDataBytes(buf.Bytes())}
	col, err := s.loadNumericColumn("f", 6, uint64(6+n)) // #nosec G115 -- small test sizes.
	if err != nil {
		t.Fatal(err)
	}
	return s, col
}

func TestNumericColumnRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		numDocs uint64
		entries []numEntry
	}{
		{"empty", 0, nil},
		{"no values", 5000, nil},
		{"dense small", 3, []numEntry{{0, 5}, {1, 5}, {2, 5}}},
		{"dense wide", 3, []numEntry{{0, math.MinInt64}, {1, 0}, {2, math.MaxInt64}}},
		{"sparse", 3000, []numEntry{{1, -7}, {1023, 9}, {1024, 300}, {2999, 70000}}},
		{"multi valued", 2050, []numEntry{{0, 1}, {0, 2}, {0, 3}, {2049, -1}, {2049, -2}}},
		{"leading empty blocks", 5000, []numEntry{{4096, 1}, {4097, 2}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, col := roundTripColumn(t, tt.numDocs, tt.entries)
			var got []numEntry
			c := newNumericColumnCursor(col)
			err := c.iterate(s, func(doc uint64, v int64) error {
				got = append(got, numEntry{doc, v})
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.entries) {
				t.Fatalf("iterate got %v want %v", got, tt.entries)
			}
			for i := range got {
				if got[i] != tt.entries[i] {
					t.Fatalf("iterate[%d] got %v want %v", i, got[i], tt.entries[i])
				}
			}
			// point visits, in order and then again from a fresh cursor per doc
			want := map[uint64][]int64{}
			for _, e := range tt.entries {
				want[e.doc] = append(want[e.doc], e.val)
			}
			c = newNumericColumnCursor(col)
			for doc := uint64(0); doc < tt.numDocs; doc++ {
				var vals []int64
				if err := c.visit(s, doc, func(_ string, v int64) { vals = append(vals, v) }); err != nil {
					t.Fatal(err)
				}
				if len(vals) != len(want[doc]) {
					t.Fatalf("doc %d got %v want %v", doc, vals, want[doc])
				}
				for i := range vals {
					if vals[i] != want[doc][i] {
						t.Fatalf("doc %d got %v want %v", doc, vals, want[doc])
					}
				}
			}
		})
	}
}

func TestNumericColumnRandom(t *testing.T) {
	rnd := rand.New(rand.NewSource(7)) // #nosec G404 -- deterministic test data.
	const numDocs = 20000
	var entries []numEntry
	for d := uint64(0); d < numDocs; d++ {
		if rnd.Intn(4) == 0 {
			continue
		}
		entries = append(entries, numEntry{d, int64(rnd.Intn(1 << 20))})
	}
	s, col := roundTripColumn(t, numDocs, entries)
	c := newNumericColumnCursor(col)
	i := 0
	for d := uint64(0); d < numDocs; d++ {
		var got []int64
		if err := c.visit(s, d, func(_ string, v int64) { got = append(got, v) }); err != nil {
			t.Fatal(err)
		}
		if i < len(entries) && entries[i].doc == d {
			if len(got) != 1 || got[0] != entries[i].val {
				t.Fatalf("doc %d got %v want %d", d, got, entries[i].val)
			}
			i++
		} else if len(got) != 0 {
			t.Fatalf("doc %d got %v want none", d, got)
		}
	}
	// random access backwards must also work (re-search within block)
	for k := 0; k < 100; k++ {
		e := entries[rnd.Intn(len(entries))]
		var got []int64
		if err := c.visit(s, e.doc, func(_ string, v int64) { got = append(got, v) }); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0] != e.val {
			t.Fatalf("doc %d got %v want %d", e.doc, got, e.val)
		}
	}
}

func TestNumericColumnWriterErrors(t *testing.T) {
	w := newNumericColumnWriter(10)
	if err := w.Add(5, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(4, 1); err == nil {
		t.Fatal("expected out of order error")
	}
	if err := w.Add(1024, 1); err == nil {
		t.Fatal("expected beyond blocks error")
	}
}

func TestNumericColumnCorrupt(t *testing.T) {
	s := &Segment{data: segment.NewDataBytes(make([]byte, 40))}
	if _, err := s.loadNumericColumn("f", 0, 8); err == nil {
		t.Fatal("expected short range error")
	}
	// trailer claims 1 block but the index is empty
	data := make([]byte, 16)
	data[15] = 1
	s = &Segment{data: segment.NewDataBytes(data)}
	if _, err := s.loadNumericColumn("f", 0, 16); err == nil {
		t.Fatal("expected invalid index error")
	}
}

func TestPrefixCodedInt64(t *testing.T) {
	for _, v := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 123456789} {
		got := prefixCodedInt64(v)
		var bits uint64
		for _, b := range got[1:] {
			bits = bits<<7 | uint64(b)
		}
		decoded := int64(bits ^ prefixCodedSignBit) // #nosec G115 -- sign bit restore.
		if decoded != v {
			t.Fatalf("%d round trip -> %d", v, decoded)
		}
	}
}
