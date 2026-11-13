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
	"bytes"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

// storedDocs returns 300 documents (several stored chunks, a partial last
// one) with stored values of varying presence and size.
func storedDocs(n int) []segment.Document {
	docs := make([]segment.Document, n)
	for i := range docs {
		doc := FakeDocument{
			NewFakeField("_id", strconv.Itoa(i), true, false, true),
			NewFakeField("cat", "/catalog/group"+strconv.Itoa(i%16), true, false, true),
		}
		if i%3 != 0 {
			doc = append(doc, NewFakeField("title", "title "+strconv.Itoa(i*7), true, false, false))
		}
		if i%5 == 0 {
			doc = append(doc, NewFakeField("tag", "t"+strconv.Itoa(i%4), true, false, true))
		}
		docs[i] = &doc
	}
	return docs
}

func storedOf(t *testing.T, s *Segment) [][]string {
	t.Helper()
	rv := make([][]string, s.Count())
	for d := range rv {
		err := s.VisitStoredFields(uint64(d), func(f string, v []byte) bool {
			rv[d] = append(rv[d], f+"="+string(v))
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return rv
}

func TestVersion9DropsStoredIndex(t *testing.T) {
	docs := storedDocs(300)
	v8, v9 := buildSegment(t, docs, 8), buildSegment(t, docs, Version)
	if want, got := storedOf(t, v8), storedOf(t, v9); !reflect.DeepEqual(want, got) {
		t.Fatal("version 9 stored fields differ from version 8")
	}
	if diff := v8.data.Len() - v9.data.Len(); diff < 8*300 {
		t.Fatalf("version 9 is only %d bytes smaller, want >= %d", diff, 8*300)
	}
	if _, _, err := v9.getDocStoredMetaAndUnCompressed(300); err == nil {
		t.Fatal("out of range document read")
	}
}

// TestVersion9MergeMixed merges a version 8 and a version 9 segment, with and
// without deletions (raw chunk copy and per-document paths).
func TestVersion9MergeMixed(t *testing.T) {
	docs := storedDocs(300)
	v8, v9 := buildSegment(t, docs, 8), buildSegment(t, docs, Version)
	want := append(storedOf(t, v8), storedOf(t, v9)...)
	for name, drops := range map[string][]*roaring.Bitmap{
		"copy":   {nil, nil},
		"remap":  {roaring.BitmapOf(1, 200), nil},
		"remap9": {nil, roaring.BitmapOf(0, 299)},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "merged.ice")
			if _, err := mergeSegments([]segment.Segment{v8, v9}, drops, path); err != nil {
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
			var exp [][]string
			for i, d := range want {
				seg := i / 300
				if drops[seg] != nil && drops[seg].Contains(uint32(i%300)) {
					continue
				}
				exp = append(exp, d)
			}
			if got := storedOf(t, merged); merged.Version() != Version || !reflect.DeepEqual(got, exp) {
				t.Fatalf("merged version %d, stored fields differ", merged.Version())
			}
		})
	}
}

// docValuesOf returns each document's doc values of fields as "field=term".
func docValuesOf(t *testing.T, s *Segment, fields ...string) [][]string {
	t.Helper()
	r, err := s.DocumentValueReader(fields)
	if err != nil {
		t.Fatal(err)
	}
	rv := make([][]string, s.Count())
	for d := range rv {
		err = r.VisitDocumentValues(uint64(d), func(f string, v []byte) {
			rv[d] = append(rv[d], f+"="+string(v))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return rv
}

func dvKindOf(s *Segment, field string) string {
	id := s.fieldsMap[field] - 1
	switch {
	case s.fieldStoredDvs[id]:
		return "stored"
	case s.fieldOrdCols[id] != nil:
		return "ordinal"
	case s.fieldNumCols[id] != nil:
		return "numeric"
	case s.fieldDvReaders[id] != nil:
		return "terms"
	}
	return "none"
}

// TestVersion9DocValueKinds checks _id doc values come from its stored value,
// a low-cardinality field's from ordinals, and both read as in version 8,
// also through a Builder and after merges with version 8 segments.
func TestVersion9DocValueKinds(t *testing.T) {
	docs := storedDocs(300)
	v8, v9 := buildSegment(t, docs, 8), buildSegment(t, docs, Version)
	want := docValuesOf(t, v8, "_id", "cat", "tag")
	if got := docValuesOf(t, v9, "_id", "cat", "tag"); !reflect.DeepEqual(got, want) {
		t.Fatal("version 9 doc values differ from version 8")
	}
	if dvKindOf(v8, "_id") != "terms" || dvKindOf(v9, "_id") != "stored" || dvKindOf(v9, "cat") != "ordinal" {
		t.Fatalf("kinds v8 _id=%s v9 _id=%s cat=%s", dvKindOf(v8, "_id"), dvKindOf(v9, "_id"), dvKindOf(v9, "cat"))
	}

	b := NewBuilder(encodeNorm)
	for _, d := range docs {
		if err := b.Add(d); err != nil {
			t.Fatal(err)
		}
	}
	built, _, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	bs := built.(*Segment)
	if got := docValuesOf(t, bs, "_id", "cat", "tag"); !reflect.DeepEqual(got, want) ||
		dvKindOf(bs, "_id") != "stored" || dvKindOf(bs, "cat") != "ordinal" {
		t.Fatalf("builder doc values differ or kinds _id=%s cat=%s", dvKindOf(bs, "_id"), dvKindOf(bs, "cat"))
	}

	for name, tc := range map[string]struct {
		segs  []segment.Segment
		idDv  string
		tagDv string
	}{
		"v9+v9": {[]segment.Segment{v9, v9}, "stored", "ordinal"},
		"v8+v9": {[]segment.Segment{v8, v9}, "terms", "ordinal"},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "merged.ice")
			drops := []*roaring.Bitmap{roaring.BitmapOf(3), nil}
			if _, err := mergeSegments(tc.segs, drops, path); err != nil {
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
			exp := append(append(append([][]string{}, want[:3]...), want[4:]...), want...)
			if got := docValuesOf(t, merged, "_id", "cat", "tag"); !reflect.DeepEqual(got, exp) {
				t.Fatal("merged doc values differ")
			}
			if dvKindOf(merged, "_id") != tc.idDv || dvKindOf(merged, "cat") != tc.tagDv {
				t.Fatalf("merged kinds _id=%s cat=%s", dvKindOf(merged, "_id"), dvKindOf(merged, "cat"))
			}
		})
	}
}

// TestVersion9IdNotStoredKeepsTerms keeps chunked _id doc values when an _id
// is not stored or not its own term.
func TestVersion9IdNotStoredKeepsTerms(t *testing.T) {
	for name, field := range map[string]*FakeField{
		"unstored": NewFakeField("_id", "x", false, false, true),
		"analyzed": {N: "_id", V: []byte("a b"), S: true, DV: true, T: []*FakeTerm{{T: "a", F: 1}, {T: "b", F: 1}}},
	} {
		t.Run(name, func(t *testing.T) {
			docs := storedDocs(10)
			docs[4] = &FakeDocument{field}
			s := buildSegment(t, docs, Version)
			if k := dvKindOf(s, "_id"); k != "terms" {
				t.Fatalf("_id kind %s, want terms", k)
			}
			got := docValuesOf(t, s, "_id")[4]
			want := []string{"_id=x"}
			if name == "analyzed" {
				want = []string{"_id=a", "_id=b"}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("doc values %v, want %v", got, want)
			}
		})
	}
}

// TestTermDvWriterFallsBackToContent writes chunked content once a field
// has more than maxOrdinalTerms distinct terms, the same bytes as a coder
// that keeps them all until Write.
func TestTermDvWriterFallsBackToContent(t *testing.T) {
	chunkSize, err := getChunkSize(legacyChunkMode, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []uint64{maxOrdinalTerms + 10} {
		w, err := newTermDvWriter(n)
		if err != nil {
			t.Fatal(err)
		}
		var want bytes.Buffer
		ref := newChunkedContentCoder(chunkSize, n-1, &want, false)
		for i := range n {
			terms := []byte(strconv.FormatUint(i*7919, 10) + "\xff")
			if err = errors.Join(w.Add(i, terms), ref.Add(i, terms)); err != nil {
				t.Fatal(err)
			}
		}
		var buf bytes.Buffer
		kind, _, _, err := w.Write(newCountHashWriter(&buf))
		if err != nil || kind != dvKindTerms {
			t.Fatalf("n=%d kind=%d err=%v, want chunked content", n, kind, err)
		}
		if err = ref.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err = ref.Write(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf.Bytes(), want.Bytes()) {
			t.Fatalf("n=%d: %d bytes differ from the %d of a buffered coder", n, buf.Len(), want.Len())
		}
	}
}

// nonNumericField is a keyword field with a NumericValue method that reports
// no number, as riot's TermField does.
type nonNumericField struct{ *FakeField }

func (nonNumericField) NumericValue() (int64, bool) { return 0, false }

func TestVersion9StoredIdWithNumericValueMethod(t *testing.T) {
	docs := make([]segment.Document, 20)
	for i := range docs {
		docs[i] = &FakeDocument{NewFakeField("_id", strconv.Itoa(i), true, false, true)}
	}
	docs[0] = &nonNumericDoc{nonNumericField{NewFakeField("_id", "0", true, false, true)}}
	if k := dvKindOf(buildSegment(t, docs, Version), "_id"); k != "stored" {
		t.Fatalf("_id kind %s, want stored", k)
	}
}

type nonNumericDoc struct{ f nonNumericField }

func (d *nonNumericDoc) Analyze()                        {}
func (d *nonNumericDoc) Timestamp() int64                { return 0 }
func (d *nonNumericDoc) EachField(vf segment.VisitField) { vf(d.f) }

func TestStoredDocValueCheck(t *testing.T) {
	for _, c := range []struct {
		name   string
		fields []*FakeField
		ok     bool
	}{
		{"none", nil, false},
		{"one term", []*FakeField{NewFakeField("_id", "a", true, false, true)}, true},
		{"two docs", []*FakeField{NewFakeField("_id", "a", true, false, true), NewFakeField("_id", "b", true, false, true)}, true},
		{"two terms", []*FakeField{NewFakeField("_id", "a b", true, false, true)}, false},
		{"not stored", []*FakeField{NewFakeField("_id", "a", false, false, true)}, false},
		{"no doc values", []*FakeField{NewFakeField("_id", "a", true, false, false)}, false},
	} {
		var v storedDocValue
		for _, f := range c.fields {
			v.check(f)
		}
		if v.ok() != c.ok {
			t.Errorf("%s: ok %v, want %v", c.name, v.ok(), c.ok)
		}
		if r := v.result(); r.ok() != c.ok || r.visit != nil || r.value != nil {
			t.Errorf("%s: result %+v", c.name, r)
		}
	}
}

// idField is a stored, doc valued field of one term that allocates nothing.
type idField struct{ v []byte }

func (f *idField) Name() string                       { return "_id" }
func (f *idField) Length() int                        { return 1 }
func (f *idField) EachTerm(vt segment.VisitTerm)      { vt(f) }
func (f *idField) Value() []byte                      { return f.v }
func (f *idField) Index() bool                        { return true }
func (f *idField) Store() bool                        { return true }
func (f *idField) IndexDocValues() bool               { return true }
func (f *idField) Term() []byte                       { return f.v }
func (f *idField) Frequency() int                     { return 1 }
func (f *idField) EachLocation(segment.VisitLocation) {}

func TestStoredDocValueCheckAllocs(t *testing.T) {
	f := &idField{v: []byte("a")}
	var v storedDocValue
	v.check(f)
	if n := testing.AllocsPerRun(100, func() { v.check(f) }); n != 0 {
		t.Fatalf("check allocates %v times, want 0", n)
	}
}

// TestTermDvWriterOrdinalsManyValues writes more values than the first
// growth of the ordinal slices, several per document.
func TestTermDvWriterOrdinalsManyValues(t *testing.T) {
	const n = 1000
	w, err := newTermDvWriter(n)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if err := w.Add(uint64(i), []byte("c"+strconv.Itoa(i%3)+"\xffd"+strconv.Itoa(i%5)+"\xff")); err != nil {
			t.Fatal(err)
		}
	}
	if len(w.docs) != 2*n || len(w.vals) != 2*n || w.docs[2*n-1] != n-1 || w.terms[w.vals[2*n-1]] != "d"+strconv.Itoa((n-1)%5) {
		t.Fatalf("docs %d vals %d", len(w.docs), len(w.vals))
	}
	var buf bytes.Buffer
	if _, _, _, err := w.Write(newCountHashWriter(&buf)); err != nil {
		t.Fatal(err)
	}
}

// TestTermDvWriterMalformedTerms adds what a corrupt older segment can hand
// a merge: no terms, and a term without its separator. Ordinals take the
// terms visitDocValues reads, those a separator ends.
func TestTermDvWriterMalformedTerms(t *testing.T) {
	w, err := newTermDvWriter(3)
	if err != nil {
		t.Fatal(err)
	}
	for d, terms := range []string{"", "a\xffb", "b\xff"} {
		if err := w.Add(uint64(d), []byte(terms)); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(w.terms, []string{"a", "b"}) || !reflect.DeepEqual(w.docs, []uint32{1, 2}) {
		t.Fatalf("terms %q of docs %v, want a and b of docs 1 and 2", w.terms, w.docs)
	}
	var buf bytes.Buffer
	if _, _, _, err := w.Write(newCountHashWriter(&buf)); err != nil {
		t.Fatal(err)
	}
}

// BenchmarkStoredFetch reads one stored document at a time, spread over the
// chunks, with (version 8) and without (version 9) the stored-field index.
func BenchmarkStoredFetch(b *testing.B) {
	for _, ver := range []uint32{versionNoStoredIndex - 1, versionNoStoredIndex} {
		b.Run("v"+strconv.Itoa(int(ver)), func(b *testing.B) {
			seg := buildSegment(b, storedDocs(4096), ver)
			n := uint64(0)
			for b.Loop() {
				n = (n + 61) % 4096
				if err := seg.VisitStoredFields(n, func(string, []byte) bool { return true }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestStoredRecordOffsets checks the record offsets a version 9 chunk entry
// keeps against the stored index of version 8: one per document, walked
// once, and an error on a corrupt chunk.
func TestStoredRecordOffsets(t *testing.T) {
	const chunkSize = uint64(defaultDocumentChunkSize)
	docs := storedDocs(300)
	v8, v9 := buildSegment(t, docs, 8), buildSegment(t, docs, Version)
	for chunk, want := range []int{128, 128, 44} {
		e, err := v9.storedChunkEntry(uint64(chunk))
		if err != nil {
			t.Fatal(err)
		}
		offs, err := e.recordOffsets()
		if err != nil || len(offs) != want {
			t.Fatalf("chunk %d: %d offsets, err %v, want %d", chunk, len(offs), err, want)
		}
		for i, off := range offs {
			d := uint64(chunk)*chunkSize + uint64(i)
			_, v8Off, err := v8.getDocStoredOffsetsOnly(d)
			if err != nil || uint64(off) != v8Off {
				t.Fatalf("doc %d: offset %d, version 8 %d (%v)", d, off, v8Off, err)
			}
		}
	}

	e, err := v9.storedChunkEntry(0)
	if err != nil {
		t.Fatal(err)
	}
	fresh := &storedChunkEntry{data: e.data}
	first, err := fresh.recordOffsets()
	if err != nil {
		t.Fatal(err)
	}
	want := slices.Clone(first)
	fresh.data = nil // a second walk would now find no records
	if again, err := fresh.recordOffsets(); err != nil || !slices.Equal(again, want) {
		t.Fatalf("offsets walked twice: %d offsets, err %v", len(again), err)
	}

	bad := &storedChunkEntry{data: []byte{5, 1, 0}}
	if _, err := bad.recordOffsets(); err == nil {
		t.Fatal("corrupt chunk read")
	}
	if offs, err := bad.recordOffsets(); err == nil || len(offs) != 0 {
		t.Fatalf("corrupt chunk offsets kept: %v, err %v", offs, err)
	}
}

// TestStoredRecordOffsetsNoAlloc checks that walking a chunk's record
// offsets allocates nothing, so stored-field cache misses add no garbage.
func TestStoredRecordOffsetsNoAlloc(t *testing.T) {
	v9 := buildSegment(t, storedDocs(300), Version)
	e, err := v9.storedChunkEntry(0)
	if err != nil {
		t.Fatal(err)
	}
	// AllocsPerRun calls the function runs+1 times; each call walks its own
	// preallocated entry so only the walk itself is measured.
	const runs = 100
	entries := make([]storedChunkEntry, runs+1)
	for i := range entries {
		entries[i].data = e.data
	}
	var offs []uint32
	next := 0
	allocs := testing.AllocsPerRun(runs, func() {
		if offs, err = entries[next].recordOffsets(); err != nil {
			t.Fatal(err)
		}
		next++
	})
	if allocs != 0 || len(offs) != int(defaultDocumentChunkSize) {
		t.Fatalf("%.0f allocs, %d offsets; want 0 and %d", allocs, len(offs), defaultDocumentChunkSize)
	}
}

// TestStoredChunkCacheReplaceResetsOffsets checks that re-putting a cached
// chunk installs a new entry, so offsets walked over the old data are not
// served for the new data.
func TestStoredChunkCacheReplaceResetsOffsets(t *testing.T) {
	record := func(dataLen byte) []byte { // metaLen 0, dataLen, data
		return append([]byte{0, dataLen}, make([]byte, dataLen)...)
	}
	var c storedChunkCache
	c.init(2)
	c.put(0, slices.Concat(record(1), record(1))) // records at 0, 3
	load := func() ([]byte, error) { t.Fatal("cached chunk loaded"); return nil, nil }
	old, err := c.getOrLoadEntry(0, load)
	if err != nil {
		t.Fatal(err)
	}
	if offs, oerr := old.recordOffsets(); oerr != nil || !slices.Equal(offs, []uint32{0, 3}) {
		t.Fatalf("old offsets %v, err %v", offs, oerr)
	}

	c.put(0, slices.Concat(record(4), record(0), record(2))) // records at 0, 6, 8
	cur, err := c.getOrLoadEntry(0, load)
	if err != nil {
		t.Fatal(err)
	}
	if offs, err := cur.recordOffsets(); err != nil || !slices.Equal(offs, []uint32{0, 6, 8}) {
		t.Fatalf("offsets after replace %v, err %v", offs, err)
	}
	if c.len() != 1 || c.size() != cap(cur.data) {
		t.Fatalf("len %d size %d, want 1 and %d", c.len(), c.size(), cap(cur.data))
	}
}

// TestOrdinalOutOfRange reads ordinals past the terms, as a corrupt block
// decodes, without panicking.
func TestOrdinalOutOfRange(t *testing.T) {
	s := buildSegment(t, storedDocs(300), Version)
	ords := s.fieldOrdCols[s.fieldsMap["cat"]-1]
	if ords == nil {
		t.Fatal("cat has no ordinal column")
	}
	corrupt := &ordinalColumn{col: ords.col, terms: ords.terms[:1]}
	if err := corrupt.iterate(s, func(uint64, []byte) error { return nil }); err == nil {
		t.Fatal("iterate accepted an out of range ordinal")
	}
	var visited int
	for docNum := range uint64(300) {
		err := newOrdinalCursor(corrupt).visitDoc(s, docNum, func(string, []byte) { visited++ })
		if err != nil {
			t.Fatal(err)
		}
	}
	if visited == 0 || visited == 300 {
		t.Fatalf("visited %d in-range ordinals, want some of 300", visited)
	}
}

// TestTermDvWriterBlockValueLimit gives up ordinals for a block of more
// values than a numeric block loads, and keeps them at the limit.
func TestTermDvWriterBlockValueLimit(t *testing.T) {
	for _, perDoc := range []int{numericBlockSize, numericBlockSize + 1} {
		w, err := newTermDvWriter(numericBlockSize)
		if err != nil {
			t.Fatal(err)
		}
		for d := range numericBlockSize {
			var terms []byte
			for j := range perDoc {
				terms = append(strconv.AppendInt(terms, int64((d*7+j)%2000), 10), termSeparator)
			}
			if err := w.Add(uint64(d), terms); err != nil {
				t.Fatal(err)
			}
		}
		if over := perDoc > numericBlockSize; (w.ids == nil) != over {
			t.Fatalf("%d terms per document: ordinals given up %v, want %v", perDoc, w.ids == nil, over)
		}
		if w.ids == nil {
			var buf bytes.Buffer
			if kind, _, _, err := w.Write(newCountHashWriter(&buf)); err != nil || kind != dvKindTerms {
				t.Fatalf("kind %d, err %v, want chunked content", kind, err)
			}
			continue
		}
		var ord bytes.Buffer
		if err := w.writeOrdinals(&ord); err != nil {
			t.Fatal(err)
		}
		s := &Segment{data: segment.NewDataBytes(ord.Bytes()), footer: &footer{version: Version}}
		if _, err := s.loadOrdinalColumn("f", 0, uint64(ord.Len())); err != nil { // #nosec G115 -- a buffer length.
			t.Fatalf("%d terms per document: %v", perDoc, err)
		}
	}
}

// TestVersion9MergeStoredWithoutDocValues merges a segment whose _id doc
// values are its stored values with one whose _id has none, with terms
// (a dictionary) and without: their documents gain no doc values.
func TestVersion9MergeStoredWithoutDocValues(t *testing.T) {
	a := buildSegment(t, storedDocs(10), Version)
	for name, field := range map[string]*FakeField{
		"analyzed":   NewFakeField("_id", "x y", true, false, false),
		"storedOnly": {N: "_id", V: []byte("x"), S: true},
	} {
		t.Run(name, func(t *testing.T) {
			b := buildSegment(t, []segment.Document{&FakeDocument{field}, &FakeDocument{field}}, Version)
			if k := dvKindOf(b, "_id"); k != "none" {
				t.Fatalf("_id kind %s, want none", k)
			}
			path := filepath.Join(t.TempDir(), "merged.ice")
			if _, err := mergeSegments([]segment.Segment{a, b}, make([]*roaring.Bitmap, 2), path); err != nil {
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
			want := append(docValuesOf(t, a, "_id"), nil, nil)
			if got := docValuesOf(t, merged, "_id"); dvKindOf(merged, "_id") != "terms" || !reflect.DeepEqual(got, want) {
				t.Fatalf("merged _id kind %s, doc values %v, want terms and %v", dvKindOf(merged, "_id"), got, want)
			}
		})
	}
}

// TestStoredDocValuesKeepChunk reads stored doc values in document order
// without a chunk cache: each chunk is decompressed once, by a reader and
// by a merge, and a reader moved to another segment drops the chunk kept.
func TestStoredDocValuesKeepChunk(t *testing.T) {
	docs := storedDocs(300)
	want := docValuesOf(t, buildSegment(t, docs, 8), "_id")
	s := buildSegment(t, docs, Version)
	s.initStoredChunkCache(0)
	if got := docValuesOf(t, s, "_id"); !reflect.DeepEqual(got, want) {
		t.Fatal("doc values differ from version 8")
	}
	if _, misses, _ := s.StoredChunkCacheStats(); misses != 3 {
		t.Fatalf("reader decompressed %d chunks, want 3", misses)
	}

	s.initStoredChunkCache(0)
	var n int
	err := s.iterateStoredDocValues("_id", nil, func(docNum uint64, terms []byte) error {
		if string(terms) != strconv.FormatUint(docNum, 10)+"\xff" {
			t.Fatalf("doc %d: terms %q", docNum, terms)
		}
		n++
		return nil
	})
	if _, misses, _ := s.StoredChunkCacheStats(); err != nil || n != 300 || misses != 3 {
		t.Fatalf("iterated %d documents decompressing %d chunks (%v), want 300 and 3", n, misses, err)
	}

	other := buildSegment(t, []segment.Document{&FakeDocument{NewFakeField("_id", "other", true, false, true)}}, Version)
	dvs, err := s.visitDocumentFieldTerms(0, []string{"_id"}, func(string, []byte) {}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	_, err = other.visitDocumentFieldTerms(0, []string{"_id"}, func(_ string, v []byte) {
		if cap(v) != len(v) {
			t.Fatalf("value of capacity %d over its length %d", cap(v), len(v))
		}
		got = append(got, string(v))
	}, nil, dvs)
	if err != nil || !reflect.DeepEqual(got, []string{"other"}) {
		t.Fatalf("doc values %v (%v) after changing segment, want other", got, err)
	}
}

// TestOrdinalTermsCapped keeps a visitor appending to a term from writing
// the segment's bytes after it.
func TestOrdinalTermsCapped(t *testing.T) {
	s := buildSegment(t, storedDocs(300), Version)
	for _, term := range s.fieldOrdCols[s.fieldsMap["cat"]-1].terms {
		if cap(term) != len(term) {
			t.Fatalf("term %q of capacity %d", term, cap(term))
		}
	}
}
