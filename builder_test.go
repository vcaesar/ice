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
	"bytes"
	"errors"
	"math"
	"runtime"
	"strconv"
	"strings"
	"testing"

	segment "github.com/vcaesar/bluge_segment_api"
)

type timedDoc struct {
	segment.Document
	ts int64
}

func (d timedDoc) Timestamp() int64 { return d.ts }

func builderBytes(t *testing.T, docs []segment.Document) []byte {
	t.Helper()
	b := NewBuilder(encodeNorm)
	for _, d := range docs {
		if err := b.Add(d); err != nil {
			t.Fatal(err)
		}
	}
	if b.Len() != len(docs) {
		t.Fatalf("Len %d want %d", b.Len(), len(docs))
	}
	seg, n, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	out := segmentBytes(t, seg)
	if n == 0 || int(n) > len(out) {
		t.Fatalf("size %d for %d bytes", n, len(out))
	}
	return out
}

func newBytes(t *testing.T, docs []segment.Document) []byte {
	t.Helper()
	seg, _, err := newWithChunkMode(docs, encodeNorm, defaultChunkMode, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	return segmentBytes(t, seg)
}

// TestBuilderMatchesNew checks that Build writes byte for byte the segment
// New writes over the same documents: stored and doc values, numeric
// columns, repeated fields, composite locations naming other fields,
// fields only locations name, singletons and timestamps.
func TestBuilderMatchesNew(t *testing.T) {
	typed := func(n int, multi bool) []segment.Document {
		var docs []segment.Document
		for i := range n {
			d := &numericDoc{fields: []segment.Field{
				NewFakeField("_id", strconv.Itoa(i), true, false, false),
				NewFakeNumericField("price", int64(i*7-50)),
				fakeTermField("label", prefixCodedInt64(int64(i%3))),
			}}
			if multi && i%2 == 0 {
				d.fields = append(d.fields, NewFakeNumericField("price", -int64(i)))
			}
			docs = append(docs, d)
		}
		return docs
	}
	ghost := &FakeDocument{
		NewFakeField("_id", "g", true, false, false),
		{N: "body", T: []*FakeTerm{{T: "x", F: 1, L: []*FakeLocation{{F: "zz_only_loc", P: 1, E: 1}, {F: "aa_only_loc", P: 2}}}}},
	}
	timed := func(ts ...int64) []segment.Document {
		var docs []segment.Document
		for i, v := range ts {
			docs = append(docs, timedDoc{&FakeDocument{NewFakeField("_id", strconv.Itoa(i), true, false, false)}, v})
		}
		return docs
	}
	cases := map[string][]segment.Document{
		"multi":         buildTestAnalysisResultsMulti(),
		"different":     buildTestAnalysisResultsMultiWithDifferentFields(true, true),
		"onlyB":         buildTestAnalysisResultsMultiWithDifferentFields(false, true),
		"fst":           fstTestDocs(3000),
		"split":         splitDocs(400),
		"typed":         typed(300, false),
		"typedMulti":    typed(300, true),
		"ghostLocation": {ghost},
		"timed":         timed(5, -3, 9),
		"partlyUntimed": timed(5, 0, 9),
		"empty":         nil,
		"emptyDocs":     {&FakeDocument{}, &FakeDocument{}},
	}
	for name, docs := range cases {
		t.Run(name, func(t *testing.T) {
			if want, got := newBytes(t, docs), builderBytes(t, docs); !bytes.Equal(want, got) {
				t.Fatalf("Build wrote %d bytes, New %d; contents differ", len(got), len(want))
			}
		})
	}
}

// The builder must match New's parallel paths too.
func TestBuilderMatchesNewParallel(t *testing.T) {
	prev := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(prev)
	docs := splitDocs(parallelPostingsMinDocs + 10)
	if !bytes.Equal(newBytes(t, docs), builderBytes(t, docs)) {
		t.Fatal("Build differs from parallel New")
	}
}

// Build writes the stored fields while it sorts and encodes the terms; the
// postings must still follow them, on one P as on many.
func TestBuilderConcurrentStoredMatchesNew(t *testing.T) {
	docs := fstTestDocs(parallelPostingsMinDocs + 10)
	for _, procs := range []int{1, 8} {
		prev := runtime.GOMAXPROCS(procs)
		want, got := newBytes(t, docs), builderBytes(t, docs)
		runtime.GOMAXPROCS(prev)
		if !bytes.Equal(want, got) {
			t.Fatalf("GOMAXPROCS %d: Build differs from New", procs)
		}
	}
}

func TestBuilderErrors(t *testing.T) {
	bad := []segment.Document{
		&FakeDocument{{N: "f", T: []*FakeTerm{{T: "t", F: -1}}}},
		&FakeDocument{{N: "f", T: []*FakeTerm{{T: "t", F: 1, L: []*FakeLocation{{P: -1}}}}}},
	}
	for i, d := range bad {
		b := NewBuilder(encodeNorm)
		err := b.Add(d)
		if err == nil {
			t.Fatalf("case %d: Add accepted a bad document", i)
		}
		if err2 := b.Add(&FakeDocument{}); !errors.Is(err2, err) {
			t.Fatalf("case %d: Add after error returned %v", i, err2)
		}
		if _, _, err3 := b.Build(); err3 == nil {
			t.Fatalf("case %d: Build after error succeeded", i)
		}
	}
	b := NewBuilder(encodeNorm)
	if _, _, err := b.Build(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Build(); err == nil {
		t.Fatal("second Build succeeded")
	}
}

func TestBuilderSizeGrows(t *testing.T) {
	b := NewBuilder(encodeNorm)
	prev := b.Size()
	for _, d := range fstTestDocs(50) {
		if err := b.Add(d); err != nil {
			t.Fatal(err)
		}
		if s := b.Size(); s <= prev {
			t.Fatalf("Size %d after add, was %d", s, prev)
		}
		prev = b.Size()
	}
}

// Add records each term's postings varint encoded, in a fraction of a
// flat record per posting and location, and Build decodes one term at a
// time as it encodes it instead of sorting a copy of all of them.
func TestBuilderBuildKeepsOneCopy(t *testing.T) {
	prev := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	defer func() { runtime.MemProfileRate = prev }()

	b := NewBuilder(encodeNorm)
	words := make([]string, 60)
	for i := range 4000 {
		for j := range words {
			words[j] = "w" + strconv.Itoa((i*31+j%20*7)%500) // 20 terms, 3 locations each
		}
		d := &FakeDocument{
			NewFakeField("_id", strconv.Itoa(i), true, false, false),
			NewFakeField("body", strings.Join(words, " "), false, true, false),
		}
		if err := b.Add(d); err != nil {
			t.Fatal(err)
		}
	}
	posts, locs := 4000*21, 4000*len(words) // _id and 20 body terms per document
	flat := 20*posts + 16*locs              // a posting record with its norm, and interimLoc
	t.Logf("%d postings, %d locations: Size %d B, flat records %d B", posts, locs, b.Size(), flat)
	if b.Size() > flat*2/3 {
		t.Fatalf("Size %d B, flat records take %d B", b.Size(), flat)
	}
	before := sortAllocs(t)
	if _, _, err := b.Build(); err != nil {
		t.Fatal(err)
	}
	alloc := sortAllocs(t) - before
	sortedCopy := int64(16 * (posts + locs)) // interimFreqNorm and interimLoc
	t.Logf("%d postings, %d locations: invert and gather allocated %d B, a sorted copy takes %d B",
		posts, locs, alloc, sortedCopy)
	if alloc > sortedCopy/2 {
		t.Fatalf("invert and gather allocated %d B, a sorted copy takes %d B", alloc, sortedCopy)
	}
}

// sortAllocs returns the bytes allocated so far by Builder.invert and
// Builder.gather.
func sortAllocs(t *testing.T) int64 {
	t.Helper()
	runtime.GC()
	runtime.GC() // publishes the profile of the previous cycle
	recs := make([]runtime.MemProfileRecord, 4096)
	n, ok := runtime.MemProfile(recs, true)
	for !ok {
		recs = make([]runtime.MemProfileRecord, 2*n)
		n, ok = runtime.MemProfile(recs, true)
	}
	var sum int64
	for _, r := range recs[:n] {
		frames := runtime.CallersFrames(r.Stack())
		for {
			f, more := frames.Next()
			if strings.HasSuffix(f.Function, "(*Builder).invert") || strings.HasSuffix(f.Function, "(*Builder).gather") {
				sum += r.AllocBytes
				break
			}
			if !more {
				break
			}
		}
	}
	return sum
}

// The builder holds a fraction of what New needs for the same documents.
func BenchmarkBuilderVsNew(b *testing.B) {
	docs := splitDocs(20000)
	b.Run("New", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, _, err := New(docs, encodeNorm); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Builder", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			bl := NewBuilder(encodeNorm)
			for _, d := range docs {
				if err := bl.Add(d); err != nil {
					b.Fatal(err)
				}
			}
			if _, _, err := bl.Build(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

type lengthField struct {
	*FakeField
	length int
}

func (f lengthField) Length() int { return f.length }

// A field repeated in a document with lengths summing past MaxInt is an
// error, not a panic.
func TestBuilderFieldLengthOverflow(t *testing.T) {
	f := NewFakeField("body", "term", false, false, false)
	d := &numericDoc{fields: []segment.Field{lengthField{f, math.MaxInt}, lengthField{f, 1}}}
	if err := NewBuilder(encodeNorm).Add(d); err == nil {
		t.Fatal("Add accepted an overflowing field length")
	}
}

// Stored fields compress, so the segment must not keep the buffer grown
// for them uncompressed.
func TestBuilderSegmentSizeWithLargeStored(t *testing.T) {
	d := &FakeDocument{NewFakeField("body", strings.Repeat("x", 4<<20), true, false, false)}
	b := NewBuilder(encodeNorm)
	if err := b.Add(d); err != nil {
		t.Fatal(err)
	}
	s, n, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	if s.Size() > int(n)+64<<10 {
		t.Fatalf("segment Size %d B for %d serialized bytes", s.Size(), n)
	}
}
