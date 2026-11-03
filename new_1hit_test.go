package ice

import (
	"testing"

	segment "github.com/vcaesar/bluge_segment_api"
)

// Terms in one document with freq 1 and no locations are written with the
// 1-hit FST encoding by New, and read back exactly like a full postings list.
func TestNew1HitEncoding(t *testing.T) {
	results := []segment.Document{
		&FakeDocument{NewFakeField("tag", "solo shared", true, false, true)},
		&FakeDocument{NewFakeField("tag", "shared", true, false, false)},
		&FakeDocument{NewFakeField("tag", "pos", true, true, false)},
	}
	seg, _, err := newWithChunkMode(results, encodeNorm, defaultChunkMode, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	s := seg.(*Segment)
	d, err := s.Dictionary("tag")
	if err != nil {
		t.Fatal(err)
	}
	dict := d.(*Dictionary)
	for _, tc := range []struct {
		term    string
		oneHit  bool
		docs    []uint64
		norm    int
		hasLocs bool
	}{
		{"solo", true, []uint64{0}, 2, false},
		{"shared", false, []uint64{0, 1}, 0, false},
		{"pos", false, []uint64{2}, 1, true},
	} {
		pl, err := dict.postingsList([]byte(tc.term), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := pl.normBits1Hit != 0; got != tc.oneHit {
			t.Fatalf("%s: 1-hit=%v want %v", tc.term, got, tc.oneHit)
		}
		if pl.Count() != uint64(len(tc.docs)) {
			t.Fatalf("%s: count %d", tc.term, pl.Count())
		}
		it, err := pl.Iterator(true, true, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		var got []uint64
		for p, err := it.Next(); p != nil || err != nil; p, err = it.Next() {
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, p.Number())
			if p.Frequency() != 1 {
				t.Fatalf("%s: freq %d", tc.term, p.Frequency())
			}
			if tc.norm != 0 && decodeNorm("", float32(p.Norm())) != tc.norm {
				t.Fatalf("%s: norm %v", tc.term, p.Norm())
			}
			if (len(p.Locations()) > 0) != tc.hasLocs {
				t.Fatalf("%s: locations %v", tc.term, p.Locations())
			}
		}
		if len(got) != len(tc.docs) || got[0] != tc.docs[0] {
			t.Fatalf("%s: docs %v want %v", tc.term, got, tc.docs)
		}
	}
	// term doc values still list the 1-hit term
	var terms []string
	dvr, err := s.DocumentValueReader([]string{"tag"})
	if err != nil {
		t.Fatal(err)
	}
	if err := dvr.VisitDocumentValues(0, func(_ string, term []byte) { terms = append(terms, string(term)) }); err != nil {
		t.Fatal(err)
	}
	if len(terms) != 2 {
		t.Fatalf("doc values %v", terms)
	}
}
