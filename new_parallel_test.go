package ice

import (
	"bytes"
	"strconv"
	"testing"

	segment "github.com/vcaesar/bluge_segment_api"
)

// The per-field parallel postings pass must write the same bytes as the
// serial pass, including composite fields, positions and doc values.
func TestProcessFieldsParallelMatchesSerial(t *testing.T) {
	docs := make([]segment.Document, 3000)
	for i := range docs {
		d := &FakeDocument{
			NewFakeField("_id", strconv.Itoa(i), true, false, false),
			NewFakeField("body", "common term"+strconv.Itoa(i%97)+" other term"+strconv.Itoa(i%5), true, true, false),
			NewFakeField("cat", "c"+strconv.Itoa(i%16), true, false, true),
		}
		d.FakeComposite("_all", []string{"_id"})
		docs[i] = d
	}
	build := func(minDocs int) []byte {
		prev := parallelPostingsMinDocs
		parallelPostingsMinDocs = minDocs
		defer func() { parallelPostingsMinDocs = prev }()
		seg, _, err := New(docs, encodeNorm)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if _, err := seg.WriteTo(&buf, nil); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	serial, parallel := build(len(docs)+1), build(1)
	if !bytes.Equal(serial, parallel) {
		t.Fatalf("parallel segment differs from serial (%d vs %d bytes)", len(parallel), len(serial))
	}
}

// A composite location may name a field no document has: prepareDicts
// defines it, and the per-field state must cover it on both postings paths.
func TestLocationOnlyField(t *testing.T) {
	for _, minDocs := range []int{1, 1 << 30} {
		prev := parallelPostingsMinDocs
		parallelPostingsMinDocs = minDocs
		docs := []segment.Document{&FakeDocument{
			NewFakeField("_id", "a", true, false, false),
			NewFakeField("body", "y", false, false, false),
			&FakeField{N: "all", T: []*FakeTerm{{T: "x", F: 1, L: []*FakeLocation{{F: "ghost", P: 1, S: 0, E: 1}}}}},
		}}
		seg, _, err := New(docs, encodeNorm)
		parallelPostingsMinDocs = prev
		if err != nil {
			t.Fatal(err)
		}
		got := locPostings(t, seg.(*Segment), "all", "x")
		if len(got) != 1 || len(got[0].locs) != 1 || got[0].locs[0] != (plainLoc{"ghost", 1, 0, 1}) {
			t.Fatalf("minDocs=%d: postings %v", minDocs, got)
		}
	}
}

// The per-field parallel term count must give the same segment as the
// serial one, also when a field repeats within a document (serial
// postings) or a location names an undefined field (serial fallback).
func TestCountTermsParallelMatchesSerial(t *testing.T) {
	for _, tc := range []string{"plain", "repeated", "ghost"} {
		docs := make([]segment.Document, 2500)
		for i := range docs {
			d := FakeDocument{
				NewFakeField("_id", strconv.Itoa(i), true, false, false),
				NewFakeField("body", "a b"+strconv.Itoa(i%31)+" c", true, true, false),
				NewFakeField("num", strconv.Itoa(i), true, false, true),
			}
			switch {
			case tc == "repeated" && i%7 == 0:
				d = append(d, NewFakeField("body", "again", false, true, false))
			case tc == "ghost" && i == len(docs)-1:
				d = append(d, &FakeField{N: "all", T: []*FakeTerm{{T: "x", F: 1,
					L: []*FakeLocation{{F: "ghost", P: 1, S: 0, E: 1}}}}})
			}
			docs[i] = &d
		}
		build := func(minDocs int) []byte {
			prev := parallelPostingsMinDocs
			parallelPostingsMinDocs = minDocs
			defer func() { parallelPostingsMinDocs = prev }()
			seg, _, err := New(docs, encodeNorm)
			if err != nil {
				t.Fatal(err)
			}
			var buf bytes.Buffer
			if _, err := seg.WriteTo(&buf, nil); err != nil {
				t.Fatal(err)
			}
			return buf.Bytes()
		}
		if serial, parallel := build(len(docs)+1), build(1); !bytes.Equal(serial, parallel) {
			t.Fatalf("%s: parallel count segment differs (%d vs %d bytes)", tc, len(parallel), len(serial))
		}
	}
}
