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
	"encoding/binary"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

const versionBeforeSlim = versionSlimPostings - 1

// positionDocs returns documents indexed as a text field without offsets:
// every location of body is its own, at increasing positions, and starts
// and ends at 0. w<k> terms are rare, "common" is in every document.
func positionDocs(n int) []segment.Document {
	docs := make([]segment.Document, n)
	for i := range docs {
		body := &FakeField{N: "body", T: []*FakeTerm{
			{T: "common", F: 2, L: []*FakeLocation{{P: 1}, {P: 4 + i%300}}},
			{T: "w" + strconv.Itoa(i%40), F: 1, L: []*FakeLocation{{P: 2}}},
		}}
		if i%7 == 0 {
			body.T = append(body.T, &FakeTerm{T: "seven", F: 3, L: []*FakeLocation{{P: 3}, {P: 3}, {P: 900}}})
		}
		docs[i] = &FakeDocument{NewFakeField("_id", strconv.Itoa(i), true, false, false), body}
	}
	return docs
}

// locChunkSlim reports whether the first location chunk of a term is slim.
func locChunkSlim(t *testing.T, seg *Segment, field, term string) bool {
	t.Helper()
	pl := postingsList(t, seg, field, term)
	d, err := newChunkedIntDecoder(seg.data, pl.locOffset, nil, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.loadChunk(0); err != nil {
		t.Fatal(err)
	}
	return d.slim
}

// Version 10 reads back what version 9 stores through every read path, for
// lists with slim chunks and for the ones that cannot have them.
func TestVersion10MatchesVersion9(t *testing.T) {
	for name, tc := range map[string]struct {
		docs  []segment.Document
		terms [][2]string
	}{
		"split":     {splitDocs(2500), splitTerms},
		"positions": {positionDocs(2500), [][2]string{{"body", "common"}, {"body", "w3"}, {"body", "seven"}}},
	} {
		t.Run(name, func(t *testing.T) {
			v9, v10 := buildSegment(t, tc.docs, versionBeforeSlim), buildSegment(t, tc.docs, Version)
			if v10.data.Len() >= v9.data.Len() {
				t.Fatalf("version 10 takes %d bytes, version 9 %d", v10.data.Len(), v9.data.Len())
			}
			for _, ft := range tc.terms {
				want := locPostings(t, v9, ft[0], ft[1])
				if len(want) == 0 {
					t.Fatalf("%v: no postings", ft)
				}
				if got := locPostings(t, v10, ft[0], ft[1]); !reflect.DeepEqual(got, want) {
					t.Fatalf("%v: Next differs from version 9", ft)
				}
				for _, size := range []int{1, 7, 128} {
					if got := locBatchPostings(t, locIterator(t, v10, ft[0], ft[1]), size); !reflect.DeepEqual(got, want) {
						t.Fatalf("%v size %d: NextLocBatch differs from version 9", ft, size)
					}
					if got, _ := posBatchPostings(t, locIterator(t, v10, ft[0], ft[1]), size); !reflect.DeepEqual(got, want) {
						t.Fatalf("%v size %d: NextPosBatch differs from version 9", ft, size)
					}
				}
				// a subset skips records, then Advance and Next
				keep := roaring.New()
				var wantSub []locPosting
				for k, p := range want {
					if k%3 != 1 {
						keep.Add(uint32(p.doc)) // #nosec G115 -- test doc numbers are small.
						wantSub = append(wantSub, p)
					}
				}
				it := locIterator(t, v10, ft[0], ft[1])
				it.ReplaceActual(keep)
				if got, _ := posBatchPostings(t, it, 5); !reflect.DeepEqual(got, wantSub) {
					t.Fatalf("%v subset: read %d postings, want %d", ft, len(got), len(wantSub))
				}
				if len(want) < 4 {
					continue
				}
				it = locIterator(t, v10, ft[0], ft[1])
				if p, err := it.Advance(want[2].doc); err != nil || p == nil || !reflect.DeepEqual(locsOf(p), want[2].locs) {
					t.Fatalf("%v advance: %v %v", ft, p, err)
				}
				if got, _ := posBatchPostings(t, it, 3); !reflect.DeepEqual(got, want[3:]) {
					t.Fatalf("%v after advance: read %d postings, want %d", ft, len(got), len(want)-3)
				}
			}
		})
	}
}

func locsOf(p segment.Posting) (rv []plainLoc) {
	for _, l := range p.Locations() {
		rv = append(rv, plainLoc{l.Field(), l.Pos(), l.Start(), l.End()})
	}
	return rv
}

// A chunk is slim only while each of its records is the positions, none
// decreasing, of the list's own field, one per occurrence.
func TestSlimChunks(t *testing.T) {
	doc := func(terms ...*FakeTerm) segment.Document {
		return &FakeDocument{NewFakeField("_id", "a", true, false, false), {N: "body", T: terms}}
	}
	for name, tc := range map[string]struct {
		term *FakeTerm
		slim bool
	}{
		"own":           {&FakeTerm{T: "x", F: 2, L: []*FakeLocation{{P: 1}, {P: 300}}}, true},
		"named own":     {&FakeTerm{T: "x", F: 1, L: []*FakeLocation{{F: "body", P: 1}}}, true},
		"other field":   {&FakeTerm{T: "x", F: 2, L: []*FakeLocation{{P: 1}, {F: "title", P: 2}}}, false},
		"backwards":     {&FakeTerm{T: "x", F: 2, L: []*FakeLocation{{P: 5}, {P: 2}}}, false},
		"fewer than tf": {&FakeTerm{T: "x", F: 3, L: []*FakeLocation{{P: 1}, {P: 2}}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			docs := []segment.Document{doc(tc.term), doc(&FakeTerm{T: "x", F: 1, L: []*FakeLocation{{P: 9}}})}
			seg := buildSegment(t, docs, Version)
			if got := locChunkSlim(t, seg, "body", "x"); got != tc.slim {
				t.Fatalf("slim = %v, want %v", got, tc.slim)
			}
			want := locPostings(t, buildSegment(t, docs, versionBeforeSlim), "body", "x")
			if got := locPostings(t, seg, "body", "x"); !reflect.DeepEqual(got, want) {
				t.Fatalf("read %+v, want %+v", got, want)
			}
		})
	}
}

// Lists whose locations all start and end at 0 have no offsets stream, and
// one location with offsets brings the stream back for its list.
func TestVersion10DropsZeroOffsets(t *testing.T) {
	docs := positionDocs(300)
	docs = append(docs, &FakeDocument{
		NewFakeField("_id", "x", true, false, false),
		{N: "body", T: []*FakeTerm{{T: "seven", F: 1, L: []*FakeLocation{{P: 1, S: 4, E: 9}}}}},
	})
	v9, v10 := buildSegment(t, docs, versionBeforeSlim), buildSegment(t, docs, Version)
	if pl := postingsList(t, v9, "body", "common"); pl.offOffset == 0 {
		t.Fatal("version 9 wrote no offsets stream")
	}
	if pl := postingsList(t, v10, "body", "common"); pl.offOffset != 0 || pl.locOffset == 0 {
		t.Fatalf("offsets stream at %d, locations at %d", pl.offOffset, pl.locOffset)
	}
	if pl := postingsList(t, v10, "body", "seven"); pl.offOffset == 0 {
		t.Fatal("offsets stream dropped from a list with offsets")
	}
	got := locPostings(t, v10, "body", "seven")
	if last := got[len(got)-1].locs; !reflect.DeepEqual(last, []plainLoc{{"body", 1, 4, 9}}) {
		t.Fatalf("location with offsets read as %+v", last)
	}

	// offsets handles of a list without the stream load zeros
	it := locIterator(t, v10, "body", "common")
	docNums, freqs, norms, ends, offs := make([]uint64, 8), make([]int, 8), make([]float64, 8), make([]int, 8),
		make([]uint64, 8)
	n, _, locs, err := it.NextPosBatch(docNums, freqs, norms, ends, offs, nil, nil)
	if err != nil || n != 8 || offs[7] == NoOffsets {
		t.Fatalf("n=%d offs=%v err=%v", n, offs, err)
	}
	last := locs[3*ends[6] : 3*ends[7]]
	last[1], last[2] = 7, 7
	if err := it.LoadOffsets(offs[7], last); err != nil || last[1] != 0 || last[2] != 0 {
		t.Fatalf("loaded %v, %v", last, err)
	}
}

// Merges read slim and older segments alike and write version 10, with
// and without deletions.
func TestVersion10Merge(t *testing.T) {
	const n = 1500
	docs := positionDocs(n)
	v9, v10 := buildSegment(t, docs, versionBeforeSlim), buildSegment(t, docs, Version)
	for name, drops := range map[string][]*roaring.Bitmap{
		"all":     {nil, nil},
		"dropped": {roaring.BitmapOf(0, 7, 8), roaring.BitmapOf(14, 1499)},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "merged.ice")
			if _, err := mergeSegments([]segment.Segment{v9, v10}, drops, path); err != nil {
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
			if merged.Version() != Version || !locChunkSlim(t, merged, "body", "common") {
				t.Fatalf("merged version %d, or its chunks are not slim", merged.Version())
			}
			for _, term := range []string{"common", "w3", "seven"} {
				var want []locPosting
				var doc uint64
				for s := range 2 {
					next := 0
					for _, p := range locPostings(t, v9, "body", term) {
						for ; next <= int(p.doc); next++ { // #nosec G115 -- a small document number.
							if drops[s] == nil || !drops[s].Contains(uint32(next)) { // #nosec G115 -- see above.
								doc++
							}
						}
						if drops[s] == nil || !drops[s].Contains(uint32(p.doc)) { // #nosec G115 -- see above.
							p.doc = doc - 1
							want = append(want, p)
						}
					}
					for ; next < n; next++ {
						if drops[s] == nil || !drops[s].Contains(uint32(next)) { // #nosec G115 -- see above.
							doc++
						}
					}
				}
				if got := locPostings(t, merged, "body", term); !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: merged %d postings, want %d, or they differ", term, len(got), len(want))
				}
			}
		})
	}
}

// Short lists are written as delta lists, long ones as roaring bitmaps, and
// both read back as the bitmap written.
func TestWriteDocs(t *testing.T) {
	spread := func(n, step int) *roaring.Bitmap {
		bm := roaring.New()
		for i := range n {
			bm.Add(uint32(3 + i*step)) // #nosec G115 -- small test values.
		}
		return bm
	}
	for name, tc := range map[string]struct {
		bm   *roaring.Bitmap
		list bool
	}{
		"one":              {roaring.BitmapOf(0), true},
		"last doc":         {roaring.BitmapOf(1<<32 - 1), true},
		"sparse":           {spread(100, 37), true},
		"three containers": {spread(600, 331), true},
		"at the limit":     {spread(maxListDocs, 5), true},
		"over the limit":   {spread(maxListDocs+1, 5), false},
		"run":              {spread(1000, 1), false}, // a run container is smaller
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			var scratch []byte
			out.WriteByte(0) // offset 0 is not a postings offset
			if err := writeDocs(&out, tc.bm, &scratch); err != nil {
				t.Fatal(err)
			}
			seg := &Segment{data: segment.NewDataBytes(out.Bytes()), footer: &footer{version: Version, chunkMode: 1024}}
			d := &Dictionary{sb: seg}
			pos := uint64(1)
			head, err := readDataUvarint(seg.data, &pos)
			if err != nil || head&1 != 0 != tc.list {
				t.Fatalf("head %d, %v; want list %v", head, err, tc.list)
			}
			if tc.list && head>>1 >= tc.bm.GetSerializedSizeInBytes() {
				t.Fatalf("list of %d bytes is not smaller than the bitmap", head>>1)
			}

			// read needs the three chunk offsets in front
			data := append([]byte{0, 0, 0, 0}, out.Bytes()[1:]...)
			seg.data = segment.NewDataBytes(data)
			var pl PostingsList
			if err = pl.read(1, d); err != nil {
				t.Fatal(err)
			}
			if !pl.postings.Equals(tc.bm) {
				t.Fatalf("read %v, want %v", pl.postings, tc.bm)
			}

			buf, list, err := d.postingsBytes(1)
			if err != nil || list != tc.list {
				t.Fatalf("postingsBytes list %v, %v", list, err)
			}
			if !list {
				return
			}
			words := int(tc.bm.Maximum())/64 + 1
			if words > 1<<20 {
				return // too wide a bitset for a test
			}
			dense, want := make([]uint64, words), make([]uint64, words)
			tc.bm.WriteDenseTo(want)
			if !orListDense(dense, buf) || !reflect.DeepEqual(dense, want) {
				t.Fatal("orListDense differs from the bitmap")
			}
			if orListDense(dense[:len(dense)-1], buf) {
				t.Fatal("orListDense wrote a document past dense")
			}
		})
	}
}

func TestDocListBitmapRejectsBadLists(t *testing.T) {
	long := appendDocList(nil, roaring.BitmapOf(make([]uint32, 0)...))
	for i := range maxListDocs + 1 {
		long = append(long, byte(i%2))
	}
	for name, list := range map[string][]byte{
		"truncated varint": {5, 0x80},
		"past uint32":      {0xff, 0xff, 0xff, 0xff, 0x0f, 0},
		"too many":         long,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := docListBitmap(list); err == nil {
				t.Fatal("read a bad document list")
			}
			if name != "too many" && orListDense(make([]uint64, 4), list) {
				t.Fatal("ORed a bad document list")
			}
		})
	}
}

// The dense OR of a prefix reads delta lists and bitmaps alike.
func TestOrPostingsDenseVersion10(t *testing.T) {
	const numDocs = 9000
	docs := make([]segment.Document, numDocs)
	for i := range docs {
		// p<k> lists are short, pall is a bitmap
		docs[i] = &FakeDocument{
			NewFakeField("_id", strconv.Itoa(i), true, false, false),
			NewFakeField("body", "pall p"+strconv.Itoa(i%900), false, true, false),
		}
	}
	seg := buildSegment(t, docs, Version)
	d, err := seg.dictionary("body")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][2]string{{"p1", "p2"}, {"p", "q"}} {
		want := make([]uint64, (numDocs+63)/64)
		termUnion(t, d, []byte(r[0]), []byte(r[1]), want)
		got := make([]uint64, len(want))
		if ok, err := d.OrPostingsDense(nil, []byte(r[0]), []byte(r[1]), got); err != nil || !ok {
			t.Fatalf("%v: %v %v", r, ok, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%v: dense OR differs from the union of the terms", r)
		}
	}
	if _, list, err := d.postingsBytes(postingsList(t, seg, "body", "p7").postingsOffset); err != nil || !list {
		t.Fatalf("p7 is not a delta list: %v", err)
	}
}

// A slim record cut short is an error on every read path, not a panic.
func TestSlimRecordTruncated(t *testing.T) {
	docs := []segment.Document{
		&FakeDocument{{N: "body", T: []*FakeTerm{{T: "x", F: 2, L: []*FakeLocation{{P: 1}, {P: 300}}}}}},
		&FakeDocument{{N: "body", T: []*FakeTerm{{T: "x", F: 2, L: []*FakeLocation{{P: 2}, {P: 700}}}}}},
	}
	mem, _, err := newWithChunkMode(docs, encodeNorm, chunkModeV1, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err = mem.(*Segment).WriteTo(&buf, nil); err != nil {
		t.Fatal(err)
	}
	clean := buf.Bytes()
	seg, err := load(segment.NewDataBytes(clean), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	d, err := newChunkedIntDecoder(seg.data, postingsList(t, seg, "body", "x").locOffset, nil, false, true)
	if err != nil {
		t.Fatal(err)
	}
	at := int(d.dataStartOffset) // #nosec G115 -- a small test segment.
	// flag, then the position deltas of both postings
	if want := []byte{chunkSlim, 1, 0xab, 2, 2, 0xba, 5}; !bytes.Equal(clean[at:at+len(want)], want) {
		t.Fatalf("unexpected location chunk % x", clean[at:at+len(want)])
	}
	data := bytes.Clone(clean)
	data[at+6] = 0x80 // the last delta never ends
	seg, err = load(segment.NewDataBytes(data), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	it := locIterator(t, seg, "body", "x")
	if p, err := it.Next(); err != nil || p == nil {
		t.Fatalf("first posting: %v %v", p, err)
	}
	if _, err := it.Next(); err == nil {
		t.Fatal("Next read a truncated record")
	}
	it = locIterator(t, seg, "body", "x")
	one := make([]uint64, 2)
	if _, _, _, err := it.NextPosBatch(one, make([]int, 2), make([]float64, 2), make([]int, 2), one, nil, nil); err == nil {
		t.Fatal("NextPosBatch read a truncated record")
	}
	it = locIterator(t, seg, "body", "x")
	if _, _, _, err := it.NextOwnPosBatch(one, make([]int, 2), make([]float64, 2), make([]int, 2), make([]int, 2), one); err == nil {
		t.Fatal("NextOwnPosBatch read a truncated record")
	}
	it = locIterator(t, seg, "body", "x")
	it.ReplaceActual(roaring.BitmapOf(1))
	if _, err := it.Next(); err == nil {
		t.Fatal("Next read a truncated record after skipping one")
	}
}

// ownBatchPostings drains it with NextOwnPosBatch in batches of size,
// reading with NextPosBatch where it reports another layout, and returns
// how many batches each read.
func ownBatchPostings(t *testing.T, it *PostingsIterator, field string, size int) (rv []locPosting, own, other int) {
	t.Helper()
	docs, freqs, norms, ends := make([]uint64, size), make([]int, size), make([]float64, size), make([]int, size)
	starts, offs := make([]int, size), make([]uint64, size)
	for {
		n, recs, isOwn, err := it.NextOwnPosBatch(docs, freqs, norms, starts, ends, offs)
		if err != nil {
			t.Fatal(err)
		}
		if !isOwn {
			if n != 0 {
				t.Fatalf("read %d postings of another layout", n)
			}
			var fields []string
			var locs []int
			if n, fields, locs, err = it.NextPosBatch(docs, freqs, norms, ends, offs, nil, nil); err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				return rv, own, other
			}
			other++
			lo := 0
			for k := range n {
				if err := it.LoadOffsets(offs[k], locs[3*lo:3*ends[k]]); err != nil {
					t.Fatal(err)
				}
				lp := locPosting{doc: docs[k], freq: freqs[k], norm: norms[k]}
				for j := lo; j < ends[k]; j++ {
					lp.locs = append(lp.locs, plainLoc{fields[j], locs[3*j], locs[3*j+1], locs[3*j+2]})
				}
				lo = ends[k]
				rv = append(rv, lp)
			}
			continue
		}
		if n == 0 {
			return rv, own, other
		}
		own++
		for k := range n {
			lp := locPosting{doc: docs[k], freq: freqs[k], norm: norms[k]}
			var triple []int
			pos := 0
			for rec := recs[starts[k]:ends[k]]; len(rec) > 0; {
				delta, w := binary.Uvarint(rec)
				if w <= 0 || w > maxDeltaLen {
					t.Fatalf("doc %d: bad delta in % x", docs[k], rec)
				}
				pos, rec = pos+int(delta), rec[w:] // #nosec G115 -- small test positions.
				triple = append(triple, pos, 0, 0)
			}
			if err := it.LoadOffsets(offs[k], triple); err != nil {
				t.Fatal(err)
			}
			for j := 0; j < len(triple); j += 3 {
				lp.locs = append(lp.locs, plainLoc{field, triple[j], triple[j+1], triple[j+2]})
			}
			rv = append(rv, lp)
		}
	}
}

// NextOwnPosBatch reads the postings of slim chunks as positions alone and
// leaves the others to NextPosBatch; together they read what Next does,
// also on a subset list and after Advance.
//
//nolint:gocyclo // one table of read paths compared side by side.
func TestNextOwnPosBatchMatchesNext(t *testing.T) {
	mixed := positionDocs(2500)
	// one record the slim layout cannot hold makes its chunk, and only
	// that one, a plain chunk
	(*mixed[1000].(*FakeDocument))[1].T[0].L = []*FakeLocation{{P: 9}, {P: 2}}
	for name, tc := range map[string]struct {
		docs       []segment.Document
		terms      [][2]string
		own, other bool // layouts "common" or "quick" is read in
	}{
		"slim":  {positionDocs(2500), [][2]string{{"body", "common"}, {"body", "w3"}, {"body", "seven"}}, true, false},
		"mixed": {mixed, [][2]string{{"body", "common"}, {"body", "w3"}, {"body", "seven"}}, true, true},
		"split": {splitDocs(2500), [][2]string{{"_mix", "quick"}, {"_mix", "fox"}, {"body", "quick"}, {"tag", "quick"}}, false, true},
	} {
		t.Run(name, func(t *testing.T) {
			seg := buildSegment(t, tc.docs, Version)
			for k, ft := range tc.terms {
				want := locPostings(t, seg, ft[0], ft[1])
				if len(want) == 0 {
					t.Fatalf("%v: no postings", ft)
				}
				for _, size := range []int{1, 7, 16, 128, 1000} {
					got, own, other := ownBatchPostings(t, locIterator(t, seg, ft[0], ft[1]), ft[0], size)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("%v size %d: read %d postings, want %d, or they differ", ft, size, len(got), len(want))
					}
					if k == 0 && (own > 0 != tc.own || other > 0 != tc.other) {
						t.Fatalf("%v size %d: %d own batches, %d others", ft, size, own, other)
					}
				}
				// a subset skips records
				keep := roaring.New()
				var wantSub []locPosting
				for k, p := range want {
					if k%3 != 1 || p.doc >= 900 && p.doc < 1500 {
						keep.Add(uint32(p.doc)) // #nosec G115 -- test doc numbers are small.
						wantSub = append(wantSub, p)
					}
				}
				for _, size := range []int{5, 128} {
					it := locIterator(t, seg, ft[0], ft[1])
					it.ReplaceActual(keep)
					if got, _, _ := ownBatchPostings(t, it, ft[0], size); !reflect.DeepEqual(got, wantSub) {
						t.Fatalf("%v subset size %d: read %d postings, want %d, or they differ", ft, size, len(got), len(wantSub))
					}
				}
				if len(want) < 4 {
					continue
				}
				// Advance and Next, then the batches
				it := locIterator(t, seg, ft[0], ft[1])
				at := len(want) / 2
				if p, err := it.Advance(want[at].doc); err != nil || p == nil || !reflect.DeepEqual(locsOf(p), want[at].locs) {
					t.Fatalf("%v advance: %v %v", ft, p, err)
				}
				if p, err := it.Next(); err != nil || p == nil || p.Number() != want[at+1].doc {
					t.Fatalf("%v next: %v %v", ft, p, err)
				}
				if got, _, _ := ownBatchPostings(t, it, ft[0], 3); !reflect.DeepEqual(got, want[at+2:]) {
					t.Fatalf("%v after advance: read %d postings, want %d", ft, len(got), len(want)-at-2)
				}
			}
		})
	}

	// lists without batches of positions
	seg := buildSegment(t, positionDocs(50), Version)
	one := make([]uint64, 4)
	for _, tc := range []struct {
		it  *PostingsIterator
		own bool
	}{
		{emptyPostingsIterator, true},
		{locIterator(t, buildSegment(t, positionDocs(50), versionBeforeSlim), "body", "common"), false},
		{locIterator(t, seg, "_id", "7"), false}, // 1-hit
	} {
		n, _, own, err := tc.it.NextOwnPosBatch(one, make([]int, 4), make([]float64, 4), make([]int, 4), make([]int, 4), one)
		if err != nil || n != 0 || own != tc.own {
			t.Fatalf("n=%d own=%v err=%v, want 0 %v", n, own, err, tc.own)
		}
	}
}

func BenchmarkNextPosBatchSlim(b *testing.B) {
	docs := positionDocs(100_000)
	for _, ver := range []uint32{versionBeforeSlim, Version} {
		seg := buildSegment(b, docs, ver)
		b.Run("v"+strconv.Itoa(int(ver)), func(b *testing.B) { benchPosBatch(b, postingsList(b, seg, "body", "common")) })
	}
	pl := postingsList(b, buildSegment(b, docs, Version), "body", "common")
	b.Run("own", func(b *testing.B) {
		const size = 128
		ids, freqs, norms, starts, ends, offs := make([]uint64, size), make([]int, size), make([]float64, size),
			make([]int, size), make([]int, size), make([]uint64, size)
		var it *PostingsIterator
		for b.Loop() {
			var err error
			if it, err = pl.iterator(true, true, true, it); err != nil {
				b.Fatal(err)
			}
			for {
				var n int
				if n, _, _, err = it.NextOwnPosBatch(ids, freqs, norms, starts, ends, offs); err != nil {
					b.Fatal(err)
				}
				if n == 0 {
					break
				}
			}
		}
	})
}

// BenchmarkShortListRead reads a 62-document list with its freqs: a
// roaring bitmap used in place (version 9) or a delta list decoded on each
// read (version 10).
func BenchmarkShortListRead(b *testing.B) {
	docs := positionDocs(2500)
	for _, ver := range []uint32{versionBeforeSlim, Version} {
		seg := buildSegment(b, docs, ver)
		d, err := seg.dictionary("body")
		if err != nil {
			b.Fatal(err)
		}
		b.Run("v"+strconv.Itoa(int(ver)), func(b *testing.B) {
			var pl *PostingsList
			var it *PostingsIterator
			ids, freqs, norms := make([]uint64, 128), make([]int, 128), make([]float64, 128)
			b.ReportAllocs()
			for b.Loop() {
				if pl, err = d.postingsList([]byte("w3"), nil, pl); err != nil {
					b.Fatal(err)
				}
				if it, err = pl.iterator(true, true, false, it); err != nil {
					b.Fatal(err)
				}
				if n, err := it.NextBatch(ids, freqs, norms); err != nil || n == 0 {
					b.Fatal(n, err)
				}
			}
		})
	}
}

// A version 10 postings header stores its freq/norm offset as the distance
// back from the header, which reads back as the absolute offset.
func TestVersion10RelativeFreqOffset(t *testing.T) {
	seg := buildSegment(t, positionDocs(1500), Version)
	d, err := seg.dictionary("body")
	if err != nil {
		t.Fatal(err)
	}
	for _, term := range []string{"common", "w3", "seven"} {
		at, ok, err := d.fst.Get([]byte(term))
		if err != nil || !ok {
			t.Fatal(term, ok, err)
		}
		pos := at
		stored, err := readDataUvarint(seg.data, &pos)
		if err != nil {
			t.Fatal(err)
		}
		pl := postingsList(t, seg, "body", term)
		if pl.freqOffset == 0 || stored != at-pl.freqOffset || stored >= at {
			t.Fatalf("%s: stored %d, header at %d, freq offset %d", term, stored, at, pl.freqOffset)
		}
	}
}
