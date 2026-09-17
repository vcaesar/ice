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
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

// FakeNumericField is a doc-valued field carrying a typed int64.
type FakeNumericField struct {
	*FakeField
	num int64
}

// fakeTermField is a doc-valued field whose single term is exactly term
// (FakeField would split on the 0x20 that opens a prefix coded value).
func fakeTermField(name string, term []byte) *FakeField {
	return &FakeField{N: name, V: term, DV: true, T: []*FakeTerm{{T: string(term), F: 1}}}
}

func NewFakeNumericField(name string, v int64) *FakeNumericField {
	return &FakeNumericField{FakeField: fakeTermField(name, prefixCodedInt64(v)), num: v}
}

func (f *FakeNumericField) NumericValue() (int64, bool) { return f.num, true }

func buildNumericSegment(ids []string, base int64) (*Segment, error) {
	var results []segment.Document
	for i, id := range ids {
		doc := &FakeDocument{
			NewFakeField("_id", id, true, false, false),
			fakeTermField("price", prefixCodedInt64(base+int64(i))),
		}
		doc.FakeComposite("_all", []string{"_id"})
		results = append(results, doc)
	}
	seg, _, err := newWithChunkMode(results, encodeNorm, defaultChunkMode, DefaultOptions())
	if err != nil {
		return nil, err
	}
	return seg.(*Segment), nil
}

// numericDoc keeps the typed field in EachField (FakeDocument holds *FakeField).
type numericDoc struct {
	fields []segment.Field
}

func (d *numericDoc) Analyze()         {}
func (d *numericDoc) Timestamp() int64 { return 0 }
func (d *numericDoc) EachField(vf segment.VisitField) {
	for _, f := range d.fields {
		vf(f)
	}
}

func buildTypedSegment(ids []string, base int64, multi bool) (*Segment, error) {
	var results []segment.Document
	for i, id := range ids {
		v := base + int64(i)
		d := &numericDoc{fields: []segment.Field{
			NewFakeField("_id", id, true, false, false),
			NewFakeNumericField("price", v),
		}}
		if multi {
			d.fields = append(d.fields, NewFakeNumericField("price", -v))
		}
		results = append(results, d)
	}
	seg, _, err := newWithChunkMode(results, encodeNorm, defaultChunkMode, DefaultOptions())
	if err != nil {
		return nil, err
	}
	return seg.(*Segment), nil
}

func readPrices(t *testing.T, seg *Segment, numDocs uint64) (terms map[uint64][][]byte, nums map[uint64][]int64) {
	t.Helper()
	dvr, err := seg.DocumentValueReader([]string{"price"})
	if err != nil {
		t.Fatal(err)
	}
	terms = map[uint64][][]byte{}
	nums = map[uint64][]int64{}
	for d := uint64(0); d < numDocs; d++ {
		err = dvr.VisitDocumentValues(d, func(_ string, term []byte) {
			terms[d] = append(terms[d], append([]byte(nil), term...))
		})
		if err != nil {
			t.Fatal(err)
		}
		err = dvr.(*DocumentValueReader).VisitDocumentNumbers(d, func(_ string, term []byte) {
			if v, ok := decodePrefixCodedInt64(term); ok {
				nums[d] = append(nums[d], v)
			}
		}, func(_ string, v int64) {
			nums[d] = append(nums[d], v)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return terms, nums
}

func TestNumericColumnSegment(t *testing.T) {
	ids := make([]string, 3000)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
	}
	seg, err := buildTypedSegment(ids, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := seg.fieldNumCols[seg.fieldsMap["price"]-1]; !ok {
		t.Fatal("price was not written as a numeric column")
	}
	if seg.Version() != Version {
		t.Fatalf("version %d", seg.Version())
	}
	terms, nums := readPrices(t, seg, 3000)
	for d := uint64(0); d < 3000; d++ {
		want := []int64{100 + int64(d), -(100 + int64(d))}
		if got := nums[d]; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("doc %d typed got %v want %v", d, got, want)
		}
		if got := terms[d]; len(got) != 2 || !bytes.Equal(got[0], prefixCodedInt64(want[0])) ||
			!bytes.Equal(got[1], prefixCodedInt64(want[1])) {
			t.Fatalf("doc %d terms got %x", d, got)
		}
	}
}

func TestNumericColumnMerge(t *testing.T) {
	path, cleanup := setupTestDir(t)
	// Registered first so it runs last: the mmapped segments below must be
	// closed before the directory is removed (Windows cannot unlink open files).
	t.Cleanup(cleanup)

	open := func(name string, build segmentBuilder) *Segment {
		seg, closeF, err := createDiskSegment(build, filepath.Join(path, name))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = closeF() })
		return seg
	}
	numA := open("a.ice", func() (*Segment, error) { return buildTypedSegment([]string{"a", "b", "c"}, 1, false) })
	numB := open("b.ice", func() (*Segment, error) { return buildTypedSegment([]string{"d", "e"}, 10, false) })
	termC := open("c.ice", func() (*Segment, error) { return buildNumericSegment([]string{"f"}, 20) })

	doMerge := func(name string, segs []*Segment, drops []*roaring.Bitmap) *Segment {
		var in []segment.Segment
		for _, s := range segs {
			in = append(in, s)
		}
		out := filepath.Join(path, name)
		var buf bytes.Buffer
		if _, err := Merge(in, drops, 1024).WriteTo(&buf, nil); err != nil {
			t.Fatal(err)
		}
		seg, err := load(segment.NewDataBytes(buf.Bytes()), DefaultOptions())
		if err != nil {
			t.Fatalf("%s: %v", out, err)
		}
		return seg
	}

	t.Run("numeric+numeric keeps column, drops honored", func(t *testing.T) {
		drop := roaring.New()
		drop.Add(1) // drop "b"
		m := doMerge("m1.ice", []*Segment{numA, numB}, []*roaring.Bitmap{drop, nil})
		if _, ok := m.fieldNumCols[m.fieldsMap["price"]-1]; !ok {
			t.Fatal("merged field is not a numeric column")
		}
		_, nums := readPrices(t, m, 4)
		want := [][]int64{{1}, {3}, {10}, {11}}
		for d, w := range want {
			if got := nums[uint64(d)]; len(got) != 1 || got[0] != w[0] {
				t.Fatalf("doc %d got %v want %v", d, got, w)
			}
		}
	})

	t.Run("numeric+terms degrades to terms", func(t *testing.T) {
		m := doMerge("m2.ice", []*Segment{numA, termC}, []*roaring.Bitmap{nil, nil})
		if _, ok := m.fieldNumCols[m.fieldsMap["price"]-1]; ok {
			t.Fatal("mixed merge must not produce a numeric column")
		}
		_, nums := readPrices(t, m, 4)
		for d, w := range []int64{1, 2, 3, 20} {
			if got := nums[uint64(d)]; len(got) != 1 || got[0] != w {
				t.Fatalf("doc %d got %v want %d", d, got, w)
			}
		}
	})
}

func TestMixedFieldFallsBackToTerms(t *testing.T) {
	d1 := &numericDoc{fields: []segment.Field{
		NewFakeField("_id", "a", true, false, false),
		NewFakeNumericField("price", 5),
	}}
	d2 := &numericDoc{fields: []segment.Field{
		NewFakeField("_id", "b", true, false, false),
		fakeTermField("price", []byte("cheap")),
	}}
	seg, _, err := newWithChunkMode([]segment.Document{d1, d2}, encodeNorm, defaultChunkMode, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	s := seg.(*Segment)
	if _, ok := s.fieldNumCols[s.fieldsMap["price"]-1]; ok {
		t.Fatal("mixed field must use term doc values")
	}
	terms, _ := readPrices(t, s, 2)
	if !bytes.Equal(terms[0][0], prefixCodedInt64(5)) || string(terms[1][0]) != "cheap" {
		t.Fatalf("got %x", terms)
	}
}

func TestFooterAcceptsVersion3(t *testing.T) {
	var fb bytes.Buffer
	if err := persistFooter(&footer{}, &fb); err != nil {
		t.Fatal(err)
	}
	setVersion := func(v uint32) *segment.Data {
		b := fb.Bytes()
		binary.BigEndian.PutUint32(b[len(b)-crcWidth-verWidth:], v)
		return segment.NewDataBytes(b)
	}
	if _, err := parseFooter(setVersion(versionTermDocValues)); err != nil {
		t.Fatalf("version 3 footer rejected: %v", err)
	}
	if _, err := parseFooter(setVersion(2)); err == nil {
		t.Fatal("version 2 footer accepted")
	}
}
