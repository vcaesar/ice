package ice

import (
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

type plainLoc struct {
	field           string
	pos, start, end int
}

type locPosting struct {
	doc  uint64
	freq int
	norm float64
	locs []plainLoc
}

// buildLocSegment indexes numDocs docs whose body repeats terms, with a
// composite _all field, so postings carry several locations, from several
// fields, whose positions and offsets go backwards between fields.
func buildLocSegment(t *testing.T, numDocs int, ver uint32) *Segment {
	t.Helper()
	prev := writeVersion
	writeVersion = ver
	defer func() { writeVersion = prev }()
	docs := make([]segment.Document, numDocs)
	for i := range docs {
		words := []string{"search", "engine", "w" + strconv.Itoa(i%13), "search", "engine"}
		if i%3 == 0 {
			words = append(words, "search", strings.Repeat("long", 1+i%5))
		}
		doc := &FakeDocument{
			NewFakeField("_id", strconv.Itoa(i), true, false, false),
			NewFakeField("body", strings.Join(words, " "), false, true, false),
			NewFakeField("title", "engine search", false, true, false),
		}
		doc.FakeComposite("_all", []string{"_id"})
		docs[i] = doc
	}
	mem, _, err := newWithChunkMode(docs, encodeNorm, chunkModeV1, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "seg.ice")
	if err := persistToFile(mem.(*Segment), path); err != nil {
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

func locPostings(t *testing.T, seg *Segment, field, term string) []locPosting {
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
	var rv []locPosting
	for {
		p, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if p == nil {
			return rv
		}
		lp := locPosting{doc: p.Number(), freq: p.Frequency(), norm: p.Norm()}
		for _, l := range p.Locations() {
			lp.locs = append(lp.locs, plainLoc{l.Field(), l.Pos(), l.Start(), l.End()})
		}
		rv = append(rv, lp)
	}
}

// TestVersion6LocationsMatchVersion5 checks the compact v6 postings (raw or
// s2 chunks, delta coded locations) read back exactly what v5 stores, and
// that merging v5 with v6 writes v6 with every location intact.
func TestVersion6LocationsMatchVersion5(t *testing.T) {
	const n = 3000
	v5 := buildLocSegment(t, n, versionBlockBounds)
	v6 := buildLocSegment(t, n, versionCompactPostings)
	for _, ft := range [][2]string{{"body", "search"}, {"body", "engine"}, {"_all", "search"}, {"_all", "engine"}, {"body", "w5"}} {
		want := locPostings(t, v5, ft[0], ft[1])
		got := locPostings(t, v6, ft[0], ft[1])
		if len(want) == 0 || !reflect.DeepEqual(got, want) {
			t.Fatalf("%v: v6 read %d postings, v5 %d, or they differ", ft, len(got), len(want))
		}
	}
	if want := locPostings(t, v5, "_all", "search")[0].locs; len(want) < 3 {
		t.Fatalf("want several locations per posting, got %v", want)
	}

	path := filepath.Join(t.TempDir(), "merged.ice")
	if _, err := mergeSegments([]segment.Segment{v5, v6}, []*roaring.Bitmap{nil, nil}, path); err != nil {
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
		t.Fatalf("merged version %d", merged.Version())
	}
	want := locPostings(t, v5, "_all", "search")
	got := locPostings(t, merged, "_all", "search")
	if len(got) != 2*len(want) {
		t.Fatalf("merged %d postings, want %d", len(got), 2*len(want))
	}
	for k, p := range want {
		q := got[len(want)+k]
		q.doc -= n
		if !reflect.DeepEqual(got[k], p) || !reflect.DeepEqual(q, p) {
			t.Fatalf("merged posting %d = %+v / %+v, want %+v", k, got[k], q, p)
		}
	}
}

// TestCompactChunkFlags covers both chunk encodings: incompressible data
// stays raw, repetitive data is stored s2, and both decode to the input.
func TestCompactChunkFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		vals func(i uint64) uint64
		flag byte
	}{
		{"raw", func(i uint64) uint64 { return i * 2654435761 % 1000003 }, chunkRaw},
		{"s2", func(uint64) uint64 { return 7 }, chunkS2},
	} {
		c := newChunkedIntCoder(1024, 1023)
		c.compact = true
		for i := range uint64(1000) {
			if err := c.Add(i, tc.vals(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if c.final[0] != tc.flag {
			t.Fatalf("%s: chunk flag %d, want %d", tc.name, c.final[0], tc.flag)
		}
		var buf countHashWriterBuf
		w := newCountHashWriter(&buf)
		if _, err := w.Write([]byte{0}); err != nil { // offset 0 means "not encoded"
			t.Fatal(err)
		}
		off, err := c.writeAt(w)
		if err != nil {
			t.Fatal(err)
		}
		d, err := newChunkedIntDecoder(segment.NewDataBytes(buf.b), off, nil, false, true)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.loadChunk(0); err != nil {
			t.Fatal(err)
		}
		for i := range uint64(1000) {
			v, err := d.readUvarint()
			if err != nil || v != tc.vals(i) {
				t.Fatalf("%s: value %d = %d, %v", tc.name, i, v, err)
			}
		}
	}
}

type countHashWriterBuf struct{ b []byte }

func (w *countHashWriterBuf) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

func TestZigzagRoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 63, -64, 1 << 40, -(1 << 40)} {
		if got := unzigzag(zigzag(v)); got != v {
			t.Fatalf("zigzag(%d) round trip = %d", v, got)
		}
	}
}
