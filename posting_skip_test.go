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
	"math/rand"
	"strconv"
	"strings"
	"testing"

	segment "github.com/vcaesar/bluge_segment_api"
)

type plainPosting struct {
	doc  uint64
	freq int
	norm float64
}

// buildSkipSegment indexes numDocs documents where term "t" occurs in a
// random subset, 1-4 times, so its postings span many freq/norm chunks.
func buildSkipSegment(t *testing.T, numDocs int, chunkMode uint32) *Segment {
	t.Helper()
	rng := rand.New(rand.NewSource(1))
	docs := make([]segment.Document, numDocs)
	for i := range docs {
		words := []string{"x" + strconv.Itoa(i%7)}
		if rng.Intn(3) == 0 {
			for range 1 + rng.Intn(4) {
				words = append(words, "t")
			}
		}
		docs[i] = &FakeDocument{
			NewFakeField("_id", strconv.Itoa(i), true, false, false),
			NewFakeField("body", strings.Join(words, " "), false, true, false),
		}
	}
	seg, _, err := newWithChunkMode(docs, encodeNorm, chunkMode, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	return seg.(*Segment)
}

func skipIterator(t *testing.T, seg *Segment) *PostingsIterator {
	t.Helper()
	dict, err := seg.Dictionary("body")
	if err != nil {
		t.Fatal(err)
	}
	pl, err := dict.PostingsList([]byte("t"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	it, err := pl.Iterator(true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	return it.(*PostingsIterator)
}

func allPostings(t *testing.T, it *PostingsIterator) []plainPosting {
	t.Helper()
	var rv []plainPosting
	for {
		p, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if p == nil {
			return rv
		}
		rv = append(rv, plainPosting{p.Number(), p.Frequency(), p.Norm()})
	}
}

// TestPostingsAdvanceAndBatchMatchNext checks rank-skipping Advance and
// NextBatch against a plain Next walk.
func TestPostingsAdvanceAndBatchMatchNext(t *testing.T) {
	for _, chunkMode := range []uint32{16, 1024, chunkModeV1} {
		seg := buildSkipSegment(t, 20000, chunkMode)
		want := allPostings(t, skipIterator(t, seg))
		if len(want) < 1000 {
			t.Fatalf("only %d postings", len(want))
		}

		rng := rand.New(rand.NewSource(int64(chunkMode)))
		for trial := range 50 {
			it := skipIterator(t, seg)
			var target uint64
			for {
				// mix short steps, which iterate, with long jumps, which rank
				if rng.Intn(2) == 0 {
					target += uint64(rng.Intn(4))
				} else {
					target += uint64(rng.Intn(3000))
				}
				p, err := it.Advance(target)
				if err != nil {
					t.Fatal(err)
				}
				j := 0
				for j < len(want) && want[j].doc < target {
					j++
				}
				if j == len(want) {
					if p != nil {
						t.Fatalf("mode %d trial %d: Advance(%d) = %d, want end", chunkMode, trial, target, p.Number())
					}
					break
				}
				got := plainPosting{p.Number(), p.Frequency(), p.Norm()}
				if p == nil || got != want[j] {
					t.Fatalf("mode %d trial %d: Advance(%d) = %+v, want %+v", chunkMode, trial, target, got, want[j])
				}
				target = got.doc + 1
				// a Next after an Advance must stay in sync
				if rng.Intn(4) == 0 {
					p, err = it.Next()
					if err != nil {
						t.Fatal(err)
					}
					if j+1 < len(want) {
						if p == nil || (plainPosting{p.Number(), p.Frequency(), p.Norm()}) != want[j+1] {
							t.Fatalf("mode %d: Next after Advance diverged at %d", chunkMode, want[j+1].doc)
						}
						target = want[j+1].doc + 1
					}
				}
			}
		}

		it := skipIterator(t, seg)
		docs, freqs, norms := make([]uint64, 37), make([]int, 37), make([]float64, 37)
		var got []plainPosting
		for {
			n, err := it.NextBatch(docs, freqs, norms)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				break
			}
			for k := range n {
				got = append(got, plainPosting{docs[k], freqs[k], norms[k]})
			}
		}
		if len(got) != len(want) {
			t.Fatalf("mode %d: NextBatch read %d postings, want %d", chunkMode, len(got), len(want))
		}
		for k := range want {
			if got[k] != want[k] {
				t.Fatalf("mode %d: NextBatch posting %d = %+v, want %+v", chunkMode, k, got[k], want[k])
			}
		}
	}
}

// TestOrPostingsDenseMatchesTermUnion checks the one-pass range OR and the
// count-free terms iterator against per-term postings.
func TestOrPostingsDenseMatchesTermUnion(t *testing.T) {
	seg := buildSkipSegment(t, 5000, chunkModeV1)
	d, err := seg.dictionary("body")
	if err != nil {
		t.Fatal(err)
	}
	start, end := []byte("x"), []byte("y")
	want := make([]uint64, (5000+63)/64)
	var terms []string
	it := d.Iterator(nil, start, end)
	for {
		e, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if e == nil {
			break
		}
		terms = append(terms, e.Term())
		pl, err := d.PostingsList([]byte(e.Term()), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		pi, err := pl.Iterator(false, false, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		for {
			p, err := pi.Next()
			if err != nil {
				t.Fatal(err)
			}
			if p == nil {
				break
			}
			want[p.Number()/64] |= 1 << (p.Number() % 64)
		}
	}
	if len(terms) != 7 {
		t.Fatalf("got %d x terms, want 7", len(terms))
	}
	got := make([]uint64, len(want))
	ok, err := d.OrPostingsDense(nil, start, end, got)
	if err != nil || !ok {
		t.Fatalf("OrPostingsDense: %v %v", ok, err)
	}
	for k := range want {
		if got[k] != want[k] {
			t.Fatalf("word %d = %x, want %x", k, got[k], want[k])
		}
	}
	if ok, err := d.OrPostingsDense(nil, start, end, make([]uint64, 1)); err != nil || ok {
		t.Fatalf("short bitset accepted: %v %v", ok, err)
	}
	ti := d.TermsIterator(nil, start, end)
	for _, want := range terms {
		e, err := ti.Next()
		if err != nil || e == nil || e.Term() != want {
			t.Fatalf("TermsIterator got %v %v, want %q", e, err, want)
		}
	}
}
