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
	segment "github.com/vcaesar/bluge_segment_api"
)

func TestSequentialLocations(t *testing.T) {
	const count = 160
	docs := make([]segment.Document, count)
	want := make([][]Location, count)
	for d := range docs {
		locs := []*FakeLocation{{F: "body", P: 2, S: 7, E: 13}}
		switch d % 11 {
		case 0:
			locs = nil
		case 1:
			locs = []*FakeLocation{{F: "body", P: 300, S: 1000, E: 1010}}
		case 2:
			locs = []*FakeLocation{{F: "body", P: 20, S: 50, E: 55}, {F: "body", P: 2, S: 7, E: 13}}
		case 3:
			locs = []*FakeLocation{{F: "other", P: 2, S: 7, E: 13}}
		}
		for _, l := range locs {
			want[d] = append(want[d], Location{field: l.F, pos: l.P, start: l.S, end: l.E})
		}
		docs[d] = &FakeDocument{
			&FakeField{N: "body", T: []*FakeTerm{{T: "t", F: max(1, len(locs)), L: locs}}},
			NewFakeField("other", "x", false, false, false),
		}
	}
	s, _, err := newWithChunkMode(docs, encodeNorm, 16, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	dict, err := s.Dictionary("body")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := dict.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, subset := range []bool{false, true} {
		for _, seek := range []bool{false, true} {
			keep := roaring.New()
			for d := range uint32(count) {
				if !subset || d%7 != 0 {
					keep.Add(d)
				}
			}
			pl, err := dict.PostingsList([]byte("t"), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			it, err := pl.Iterator(true, true, true, nil)
			if err != nil {
				t.Fatal(err)
			}
			if subset {
				it.(*PostingsIterator).ReplaceActual(keep)
			}
			for d := uint32(0); d < count; d++ {
				if !keep.Contains(d) {
					continue
				}
				var p segment.Posting
				if seek && d%5 == 0 {
					p, err = it.Advance(uint64(d))
				} else {
					p, err = it.Next()
				}
				if err != nil {
					t.Fatal(err)
				}
				if p == nil || p.Number() != uint64(d) || p.Frequency() != max(1, len(want[d])) {
					t.Fatalf("subset=%v seek=%v doc=%d posting=%v", subset, seek, d, p)
				}
				var got []Location
				for _, l := range p.Locations() {
					got = append(got, Location{field: l.Field(), pos: l.Pos(), start: l.Start(), end: l.End()})
				}
				if !reflect.DeepEqual(got, want[d]) {
					t.Fatalf("subset=%v seek=%v doc=%d: locations=%v want=%v", subset, seek, d, got, want[d])
				}
			}
			if p, err := it.Next(); p != nil || err != nil {
				t.Fatalf("end: posting=%v err=%v", p, err)
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSingleLocationAfterEmptyRecord(t *testing.T) {
	freq := binary.AppendUvarint(nil, encodeFreqHasLocs(1, true))
	freq = binary.AppendUvarint(freq, uint64(math.Float32bits(1)))
	it := &PostingsIterator{
		postings:        &PostingsList{sb: &Segment{footer: &footer{version: versionCompactPostings}, fieldsInv: []string{"body"}}},
		includeFreqNorm: true, includeLocs: true,
		freqNormReader: &chunkedIntDecoder{r: newMemUvarintReader(append(freq, freq...))},
		locReader:      &chunkedIntDecoder{r: newMemUvarintReader([]byte{0, 4, 0, 4, 6, 8})},
	}
	p, err := it.readPosting(0)
	if err != nil || len(p.Locations()) != 0 {
		t.Fatalf("empty record: posting=%v error=%v", p, err)
	}
	p, err = it.readPosting(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Locations()) != 1 || p.Locations()[0] == nil || p.Locations()[0].Pos() != 2 {
		t.Fatalf("single location after empty record: %v", p.Locations())
	}
}

func TestSingleLocationDecode(t *testing.T) {
	for _, version := range []uint32{5, versionCompactPostings} {
		for _, tc := range []struct {
			name string
			data []byte
			bad  bool
		}{
			{"valid", []byte{4, 0, 4, 6, 8}, false},
			{"field", []byte{4, 1, 4, 6, 8}, true},
			{"negative-position", []byte{4, 0, 3, 6, 8}, version >= 6},
			{"negative-start", []byte{4, 0, 4, 3, 8}, version >= 6},
			{"truncated", []byte{4, 0, 4, 6}, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				freq := binary.AppendUvarint(nil, encodeFreqHasLocs(1, true))
				freq = binary.AppendUvarint(freq, uint64(math.Float32bits(1)))
				it := &PostingsIterator{
					postings:        &PostingsList{sb: &Segment{footer: &footer{version: version}, fieldsInv: []string{"body"}}},
					includeFreqNorm: true, includeLocs: true,
					freqNormReader: &chunkedIntDecoder{r: newMemUvarintReader(append(freq, freq...))},
					locReader:      &chunkedIntDecoder{r: newMemUvarintReader(append([]byte{4, 0, 4, 6, 8}, tc.data...))},
				}
				if _, err := it.readPosting(0); err != nil {
					t.Fatal(err)
				}
				p, err := it.readPosting(1)
				if (err != nil) != tc.bad {
					t.Fatalf("version=%d: error=%v want error=%v", version, err, tc.bad)
				}
				if err == nil && (p == nil || len(p.Locations()) != 1 || p.Locations()[0].Field() != "body") {
					t.Fatalf("invalid posting %v", p)
				}
			})
		}
	}
}
