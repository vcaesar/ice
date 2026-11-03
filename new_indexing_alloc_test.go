// Copyright (c) 2026 The AtomAI.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
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
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

func prepareIndexingTestInterim(t testing.TB, s *interim, docs []segment.Document) *interim {
	t.Helper()
	s = setupIndexingTestInterim(t, s, docs)
	if err := s.prepareDicts(); err != nil {
		t.Fatal(err)
	}
	for _, keys := range s.DictKeys {
		sort.Strings(keys)
	}
	return s
}

// setupIndexingTestInterim defines the fields of docs, as convert does
// before prepareDicts.
func setupIndexingTestInterim(t testing.TB, s *interim, docs []segment.Document) *interim {
	t.Helper()
	if s == nil {
		s = &interim{}
		s.initVisitors()
	}
	s.results, s.chunkMode, s.normCalc = docs, defaultChunkMode, encodeNorm
	s.FieldsMap = map[string]uint16{}
	s.FieldDocs = map[uint16]uint64{}
	s.FieldFreqs = map[uint16]uint64{}
	if _, err := s.getOrDefineField(_idFieldName); err != nil {
		t.Fatal(err)
	}
	for _, doc := range docs {
		doc.EachField(func(f segment.Field) {
			if _, err := s.getOrDefineField(f.Name()); err != nil {
				t.Fatal(err)
			}
		})
	}
	sort.Strings(s.FieldsInv[1:])
	for id, name := range s.FieldsInv {
		s.FieldsMap[name] = uint16(id + 1)
	}
	s.IncludeDocValues = make([]bool, len(s.FieldsInv))
	s.NumericOnly = make([]bool, len(s.FieldsInv))
	s.NumericValues = make([][]interimNumeric, len(s.FieldsInv))
	for id := range s.NumericOnly {
		s.NumericOnly[id] = true
	}
	return s
}

func materializeIndexingTestBitmaps(s *interim) {
	for pid := range s.Postings {
		if s.Postings[pid] == nil {
			s.Postings[pid] = roaring.New()
		}
	}
}

func TestIndexingSingletonsMatchBitmapBuild(t *testing.T) {
	for _, mode := range []string{"serial", "parallel", "rollup"} {
		t.Run(mode, func(t *testing.T) {
			docs := make([]segment.Document, 9)
			for i := range docs {
				id := strconv.Itoa(i)
				doc := &FakeDocument{
					NewFakeField("_id", id, true, false, false),
					NewFakeField("terms", "shared unique"+id, false, false, true),
					NewFakeField("positions", "shared unique"+id, false, true, false),
					&FakeField{N: "frequency", T: []*FakeTerm{{T: id, F: 2}}},
					&FakeField{N: "zero_frequency", T: []*FakeTerm{{T: id, F: 0}}},
					NewFakeField("zero_norm", id, false, false, false),
					NewFakeField("high_norm", id, false, false, false),
				}
				if mode == "rollup" && i == 3 {
					*doc = append(*doc, NewFakeField("terms", "shared extra", false, false, true))
				}
				docs[i] = doc
			}
			build := func(eager bool) []byte {
				s := prepareIndexingTestInterim(t, nil, docs)
				s.normCalc = func(field string, length int) float32 {
					if field == "zero_norm" {
						return 0
					}
					if field == "high_norm" {
						return math.Float32frombits(math.MaxUint32)
					}
					return encodeNorm(field, length)
				}
				if eager {
					materializeIndexingTestBitmaps(s)
				}
				var err error
				if mode == "parallel" {
					err = s.processFieldsParallel(3)
				} else {
					err = s.processDocumentsSerial()
				}
				if err != nil {
					t.Fatal(err)
				}
				if !eager {
					for i := range docs {
						pid := s.Dicts[s.FieldsMap["_id"]-1][strconv.Itoa(i)] - 1
						if s.Postings[pid] != nil || s.FreqNorms[pid][0].docNum != uint32(i) {
							t.Fatalf("singleton %d allocated a bitmap or lost its document number", i)
						}
					}
				}
				var buf bytes.Buffer
				s.w = newCountHashWriter(&buf)
				if _, _, err := s.writeStoredFields(); err != nil {
					t.Fatal(err)
				}
				if _, _, err := s.writeDicts(); err != nil {
					t.Fatal(err)
				}
				if err := s.reset(); err != nil {
					t.Fatal(err)
				}
				return buf.Bytes()
			}
			if lazy, eager := build(false), build(true); !bytes.Equal(lazy, eager) {
				t.Fatalf("lazy postings differ from eager bitmap encoding (%d vs %d bytes)", len(lazy), len(eager))
			}
		})
	}
}

func TestIndexingDocValuesIsolation(t *testing.T) {
	docs := []segment.Document{
		&numericDoc{fields: []segment.Field{
			NewFakeField("_id", "first", true, false, false),
			NewFakeField("a_terms", "alpha shared", false, false, true),
			NewFakeField("b_uninverted", "private private", false, false, false),
			NewFakeNumericField("c_numeric", 42),
			NewFakeField("d_terms", "last", false, false, true),
		}},
		&numericDoc{fields: []segment.Field{
			NewFakeField("_id", "second", true, false, false),
			NewFakeField("a_terms", "beta shared", false, false, true),
			NewFakeNumericField("c_numeric", -7),
		}},
		&numericDoc{fields: []segment.Field{
			NewFakeField("_id", "third", true, false, false),
			NewFakeField("d_terms", "last unique", false, false, true),
		}},
	}
	seg, _, err := New(docs, encodeNorm)
	if err != nil {
		t.Fatal(err)
	}
	s := seg.(*Segment)
	for _, tc := range []struct {
		field string
		want  [][]string
	}{
		{"a_terms", [][]string{{"alpha", "shared"}, {"beta", "shared"}, nil}},
		{"d_terms", [][]string{{"last"}, nil, {"last", "unique"}}},
		{"b_uninverted", [][]string{nil, nil, nil}},
		{"_id", [][]string{nil, nil, nil}},
	} {
		t.Run(tc.field, func(t *testing.T) {
			r, err := s.DocumentValueReader([]string{tc.field})
			if err != nil {
				t.Fatal(err)
			}
			for docNum, want := range tc.want {
				var got []string
				if err := r.VisitDocumentValues(uint64(docNum), func(_ string, term []byte) {
					got = append(got, string(term))
				}); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("doc %d values %v, want %v", docNum, got, want)
				}
			}
		})
	}
	if _, ok := s.fieldNumCols[s.fieldsMap["c_numeric"]-1]; !ok {
		t.Fatal("numeric doc values were not written as a numeric column")
	}
	r, err := s.DocumentValueReader([]string{"c_numeric"})
	if err != nil {
		t.Fatal(err)
	}
	for docNum, want := range [][]int64{{42}, {-7}, nil} {
		var got []int64
		if err := r.(*DocumentValueReader).VisitDocumentNumbers(uint64(docNum), nil, func(_ string, value int64) {
			got = append(got, value)
		}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("doc %d numbers %v, want %v", docNum, got, want)
		}
	}
	for docNum, term := range []string{"first", "second", "third"} {
		got := collectPostings(t, s, "_id", term)
		if len(got) != 1 || got[0].num != uint64(docNum) || got[0].freq != 1 {
			t.Fatalf("singleton %q: %+v", term, got)
		}
	}
}

func indexingSingletonDocuments(count int) []segment.Document {
	docs := make([]segment.Document, count)
	for i := range docs {
		docs[i] = &FakeDocument{NewFakeField("_id", strconv.Itoa(i), false, false, false)}
	}
	return docs
}

func TestIndexingSingletonBitmapReuse(t *testing.T) {
	var s *interim
	for step, count := range []int{16, 32, 24, 0, 8} {
		docs := indexingSingletonDocuments(count)
		if step == 1 {
			for i := range docs {
				docs[i] = &FakeDocument{NewFakeField("_id", "shared", false, false, false)}
			}
		}
		s = prepareIndexingTestInterim(t, s, docs)
		if err := s.processDocumentsSerial(); err != nil {
			t.Fatal(err)
		}
		for i := range docs {
			term, posting := strconv.Itoa(i), 0
			if step == 1 {
				term, posting = "shared", i
			}
			pid := s.Dicts[0][term] - 1
			if got := s.FreqNorms[pid][posting].docNum; got != uint32(i) {
				t.Fatalf("step %d term %q document %d, want %d", step, term, got, i)
			}
			if bitmap := s.Postings[pid]; bitmap != nil {
				want := uint64(1)
				if step == 1 {
					want = uint64(count)
				}
				if bitmap.GetCardinality() != want || !bitmap.Contains(uint32(i)) {
					t.Fatalf("step %d term %q stale bitmap %v", step, term, bitmap)
				}
			}
		}
		if err := s.reset(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIndexingSingletonAllocations(t *testing.T) {
	docs := indexingSingletonDocuments(128)
	measure := func(eager bool) float64 {
		return testing.AllocsPerRun(3, func() {
			s := prepareIndexingTestInterim(t, nil, docs)
			if eager {
				materializeIndexingTestBitmaps(s)
			}
			if err := s.processDocumentsSerial(); err != nil {
				t.Fatal(err)
			}
		})
	}
	lazy, eager := measure(false), measure(true)
	t.Logf("128 unique terms: lazy %.0f allocations, eager %.0f allocations", lazy, eager)
	if lazy >= eager {
		t.Fatalf("singleton allocations did not decrease: lazy %v, eager %v", lazy, eager)
	}
}

func TestIndexingParallelVisitorAllocations(t *testing.T) {
	const count = 128
	docs := make([]segment.Document, count)
	for i := range docs {
		docs[i] = &FakeDocument{&FakeField{N: "body"}}
	}
	s := prepareIndexingTestInterim(t, nil, docs)
	// Empty fields isolate callback allocation from token and bitmap work.
	allocs := testing.AllocsPerRun(3, func() {
		if err := s.processFieldsParallel(2); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("%d documents, two workers: %.0f allocations", count, allocs)
	if allocs >= count/2 {
		t.Fatalf("parallel field visits allocate per document: %.0f allocations", allocs)
	}
}

// prepareDicts visits fields and terms through callbacks bound once, so
// documents of known terms cost no allocations; it still counts each field
// once per document and notices a field repeated within one.
func TestPrepareDictsPerDocument(t *testing.T) {
	docs := func(n int, repeat bool) []segment.Document {
		rv := make([]segment.Document, n)
		for i := range rv {
			d := FakeDocument{
				NewFakeField("body", "common search engine", false, true, false),
				NewFakeField("cat", "c"+strconv.Itoa(i%4), false, false, false),
			}
			if repeat && i == n-1 {
				d = append(d, NewFakeField("body", "again", false, true, false))
			}
			rv[i] = &d
		}
		return rv
	}
	measure := func(n int) float64 {
		d := docs(n, false)
		setup := testing.AllocsPerRun(5, func() { setupIndexingTestInterim(t, nil, d) })
		return testing.AllocsPerRun(5, func() { prepareIndexingTestInterim(t, nil, d) }) - setup
	}
	small, large := measure(128), measure(512)
	t.Logf("128 documents: %.0f allocations, 512: %.0f", small, large)
	// FakeTerm.Term converts its string on each of the 4 term visits
	const stubPerDoc = 4
	if perDoc := (large-small)/(512-128) - stubPerDoc; perDoc >= 0.25 {
		t.Fatalf("prepareDicts allocates %.2f times per document", perDoc)
	}

	for _, repeat := range []bool{false, true} {
		s := prepareIndexingTestInterim(t, nil, docs(10, repeat))
		if s.repeatedFields != repeat {
			t.Fatalf("repeat=%v: repeatedFields=%v", repeat, s.repeatedFields)
		}
		body := s.FieldsMap["body"] - 1
		if s.FieldDocs[body] != 10 || s.FieldDocs[s.FieldsMap["cat"]-1] != 10 {
			t.Fatalf("repeat=%v: field docs %v", repeat, s.FieldDocs)
		}
	}
}

func BenchmarkIndexingSingletonPostings(b *testing.B) {
	docs := indexingSingletonDocuments(4096)
	for _, eager := range []bool{false, true} {
		name := "lazy"
		if eager {
			name = "eager_reference"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				s := prepareIndexingTestInterim(b, nil, docs)
				if eager {
					materializeIndexingTestBitmaps(s)
				}
				if err := s.processDocumentsSerial(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
