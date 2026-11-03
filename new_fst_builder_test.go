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
	"strconv"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

func fstTestDocs(n int) []segment.Document {
	docs := make([]segment.Document, n)
	for i := range docs {
		id := strconv.Itoa(i)
		docs[i] = &FakeDocument{
			NewFakeField("_id", id, true, false, false),
			NewFakeField("body", "shared t"+id, true, true, false),
		}
	}
	return docs
}

func segmentBytes(t *testing.T, seg segment.Segment) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := seg.(*Segment).WriteTo(&buf, nil); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// checkTerms verifies every _id and body term of an n-doc fstTestDocs segment.
func checkTerms(t *testing.T, seg segment.Segment, n int) {
	t.Helper()
	for _, field := range []string{"_id", "body"} {
		dict, err := seg.Dictionary(field)
		if err != nil {
			t.Fatal(err)
		}
		for i := range n {
			term := strconv.Itoa(i)
			if field == "body" {
				term = "t" + term
			}
			pl, err := dict.PostingsList([]byte(term), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if pl.Count() != 1 {
				t.Fatalf("%s:%s count %d", field, term, pl.Count())
			}
		}
		if field == "body" {
			pl, err := dict.PostingsList([]byte("shared"), nil, nil)
			if err != nil || pl.Count() != uint64(n) {
				t.Fatalf("shared: count %v err %v", pl, err)
			}
		}
	}
}

// Small segments build their FSTs with a small node registry and large ones
// with vellum's default; the pooled interim switches between both builders
// and must still write identical, complete segments.
func TestFSTBuilderSmallAndBulk(t *testing.T) {
	small, bulk := fstTestDocs(3), fstTestDocs(3*smallSegmentTerms)
	var first []byte
	for round := range 3 {
		for _, docs := range [][]segment.Document{small, bulk, small} {
			seg, _, err := New(docs, encodeNorm)
			if err != nil {
				t.Fatal(err)
			}
			checkTerms(t, seg, len(docs))
			if len(docs) == len(small) {
				b := segmentBytes(t, seg)
				if first == nil {
					first = b
				} else if !bytes.Equal(first, b) {
					t.Fatalf("round %d: small segment bytes changed after a bulk build", round)
				}
			}
		}
	}

	// merging small segments uses the small registry as well
	a, _, err := New(fstTestDocs(3), encodeNorm)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := New(fstTestDocs(5), encodeNorm)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/merged.ice"
	if _, err := mergeSegments([]segment.Segment{a, b}, []*roaring.Bitmap{nil, nil}, path); err != nil {
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
	dict, err := merged.Dictionary("body")
	if err != nil {
		t.Fatal(err)
	}
	for term, want := range map[string]uint64{"shared": 8, "t0": 2, "t4": 1, "t9": 0} {
		pl, err := dict.PostingsList([]byte(term), nil, nil)
		if err != nil || pl.Count() != want {
			t.Fatalf("merged %s: %v %v, want %d", term, pl, err, want)
		}
	}
}
