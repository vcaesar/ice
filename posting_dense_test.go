//  Copyright (c) 2026 The AtomAI.
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
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

// denseDocs returns documents for NextPosBatch's dense runs: "all" is in
// every document once; "gap" misses a document now and then, so some
// chunks are not dense; "twice" occurs twice in some documents and
// "far" sits past a one-byte position in some, ending a run mid-chunk.
func denseDocs(numDocs int) []segment.Document {
	docs := make([]segment.Document, numDocs)
	for i := range docs {
		words := []string{"all", "w" + strconv.Itoa(i%7)}
		if i%1500 != 700 {
			words = append(words, "gap")
		}
		if i%997 == 3 {
			words = append(words, "twice", "twice")
		} else {
			words = append(words, "twice")
		}
		if i%1201 == 11 {
			words = append(words, strings.Repeat("pad ", 80))
		}
		words = append(words, "far")
		docs[i] = &FakeDocument{NewFakeField("body", strings.Join(words, " "), false, true, false)}
	}
	return docs
}

// NextPosBatch reads dense runs of postings without stepping the roaring
// iterator per document; what it reads must match Next, also on a subset
// list and after Advance.
func TestNextPosBatchDenseRuns(t *testing.T) {
	seg := buildSegment(t, denseDocs(5000), versionSplitOffsets)
	for _, term := range []string{"all", "gap", "twice", "far"} {
		want := locPostings(t, seg, "body", term)
		for _, size := range []int{1, 15, 16, 128, 1000} {
			got, _ := posBatchPostings(t, locIterator(t, seg, "body", term), size)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s size %d: read %d postings, want %d, or they differ", term, size, len(got), len(want))
			}
		}

		// a subset dense over the middle chunks, sparse elsewhere
		keep := roaring.New()
		var wantSub []locPosting
		for _, p := range want {
			if p.doc >= 1000 && p.doc < 4000 || p.doc%5 == 0 {
				keep.Add(uint32(p.doc)) // #nosec G115 -- test doc numbers are small.
				wantSub = append(wantSub, p)
			}
		}
		it := locIterator(t, seg, "body", term)
		it.ReplaceActual(keep)
		if got, _ := posBatchPostings(t, it, 128); !reflect.DeepEqual(got, wantSub) {
			t.Fatalf("%s subset: read %d postings, want %d, or they differ", term, len(got), len(wantSub))
		}

		it = locIterator(t, seg, "body", term)
		k := len(want) / 2
		if p, err := it.Advance(want[k].doc); err != nil || p == nil || p.Number() != want[k].doc {
			t.Fatalf("%s advance: %v %v", term, p, err)
		}
		if got, _ := posBatchPostings(t, it, 128); !reflect.DeepEqual(got, want[k+1:]) {
			t.Fatalf("%s after advance: read %d postings, want %d", term, len(got), len(want)-k-1)
		}
	}
}
