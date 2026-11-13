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
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

// splitDocs returns documents whose postings mix one-byte and multi-byte
// positions and offsets, composite fields with locations from other fields
// (going backwards between fields), a composite term whose freq exceeds
// its locations and postings without locations among ones with.
func splitDocs(numDocs int) []segment.Document {
	docs := make([]segment.Document, numDocs)
	for i := range docs {
		filler := strings.Repeat("pad ", (i%9)*12)
		if i%97 == 0 {
			filler = strings.Repeat("padding ", 2500) // starts past 16K
		}
		text := filler + "quick brown fox w" + strconv.Itoa(i%11) + " quick brown"
		doc := &FakeDocument{
			NewFakeField("_id", strconv.Itoa(i), true, false, false),
			NewFakeField("body", text, false, true, false),
			NewFakeField("title", "brown quick", false, true, false),
			NewFakeField("tag", "quick", false, false, false),
		}
		doc.FakeComposite("_all", []string{"_id", "tag"})
		mix := &FakeField{N: "_mix", T: []*FakeTerm{{T: "quick", F: 4, L: []*FakeLocation{
			{F: "body", P: 1 + (i%9)*12, S: len(filler), E: len(filler) + 5},
			{F: "title", P: 2, S: 6, E: 11},
			{P: 7, S: 30, E: 35}, // own field
		}}}}
		switch {
		case i%5 == 0:
			mix.T = append(mix.T, &FakeTerm{T: "fox", F: 2}) // no locations
		case i%2 == 0:
			mix.T = append(mix.T, &FakeTerm{T: "fox", F: 1, L: []*FakeLocation{{P: 3 + i%70, S: 12, E: 15}}})
		default:
			mix.T = append(mix.T, &FakeTerm{T: "fox", F: 1, L: []*FakeLocation{{F: "body", P: 3, S: 12, E: 15}}})
		}
		*doc = append(*doc, mix)
		docs[i] = doc
	}
	return docs
}

// buildSegment writes docs as version ver and opens the persisted segment.
func buildSegment(t testing.TB, docs []segment.Document, ver uint32) *Segment {
	t.Helper()
	prev := writeVersion
	writeVersion = ver
	defer func() { writeVersion = prev }()
	mem, _, err := newWithChunkMode(docs, encodeNorm, chunkModeV1, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "seg.ice")
	if err = persistToFile(mem.(*Segment), path); err != nil {
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

var splitTerms = [][2]string{
	{"body", "quick"}, {"body", "brown"}, {"body", "pad"}, {"body", "padding"}, {"body", "w3"},
	{"_all", "quick"}, {"_all", "brown"}, {"_mix", "quick"}, {"_mix", "fox"}, {"title", "quick"},
	{"tag", "quick"}, {"_id", "17"},
}

// posBatchPostings drains it with NextPosBatch in batches of size,
// loading the offsets of each batch last posting first.
func posBatchPostings(t *testing.T, it *PostingsIterator, size int) (rv []locPosting, lazy int) {
	t.Helper()
	docs, freqs, norms, ends := make([]uint64, size), make([]int, size), make([]float64, size), make([]int, size)
	offs := make([]uint64, size)
	var fields []string
	var locs []int
	for {
		n, f, l, err := it.NextPosBatch(docs, freqs, norms, ends, offs, fields[:0], locs[:0])
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return rv, lazy
		}
		fields, locs = f, l
		for k := n - 1; k >= 0; k-- {
			lo := 0
			if k > 0 {
				lo = ends[k-1]
			}
			if offs[k] != NoOffsets {
				lazy++
				for j := lo; j < ends[k]; j++ {
					if locs[3*j+1] != 0 || locs[3*j+2] != 0 {
						t.Fatalf("doc %d: offsets read before LoadOffsets", docs[k])
					}
				}
			}
			if err := it.LoadOffsets(offs[k], locs[3*lo:3*ends[k]]); err != nil {
				t.Fatal(err)
			}
		}
		lo := 0
		for k := range n {
			lp := locPosting{doc: docs[k], freq: freqs[k], norm: norms[k]}
			for j := lo; j < ends[k]; j++ {
				lp.locs = append(lp.locs, plainLoc{fields[j], locs[3*j], locs[3*j+1], locs[3*j+2]})
			}
			lo = ends[k]
			rv = append(rv, lp)
		}
	}
}

// Version 7 must read back exactly what version 6 stores, through Next,
// NextLocBatch and NextPosBatch with lazily loaded offsets.
//
//nolint:gocyclo // one table of read paths compared side by side.
func TestVersion7MatchesVersion6(t *testing.T) {
	docs := splitDocs(2500)
	v6 := buildSegment(t, docs, versionCompactPostings)
	v7 := buildSegment(t, docs, versionSplitOffsets)
	for _, ft := range splitTerms {
		want := locPostings(t, v6, ft[0], ft[1])
		if len(want) == 0 {
			t.Fatalf("%v: no postings", ft)
		}
		if got := locPostings(t, v7, ft[0], ft[1]); !reflect.DeepEqual(got, want) {
			t.Fatalf("%v: v7 Next read %d postings, v6 %d, or they differ", ft, len(got), len(want))
		}
		for _, size := range []int{1, 7, 128} {
			if got := locBatchPostings(t, locIterator(t, v7, ft[0], ft[1]), size); !reflect.DeepEqual(got, want) {
				t.Fatalf("%v size %d: v7 NextLocBatch differs from v6 Next", ft, size)
			}
			got, lazy := posBatchPostings(t, locIterator(t, v7, ft[0], ft[1]), size)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%v size %d: v7 NextPosBatch differs from v6 Next", ft, size)
			}
			if size > 1 && len(want[0].locs) > 0 && lazy == 0 {
				t.Fatalf("%v size %d: no lazy offsets", ft, size)
			}
			if got, lazy := posBatchPostings(t, locIterator(t, v6, ft[0], ft[1]), size); lazy != 0 ||
				!reflect.DeepEqual(got, want) {
				t.Fatalf("%v size %d: v6 NextPosBatch lazy %d or differs", ft, size, lazy)
			}
		}

		// subset, every third posting dropped, then Advance and Next
		keep := roaring.New()
		var wantSub []locPosting
		for k, p := range want {
			if k%3 != 1 {
				keep.Add(uint32(p.doc)) // #nosec G115 -- test doc numbers are small.
				wantSub = append(wantSub, p)
			}
		}
		it := locIterator(t, v7, ft[0], ft[1])
		it.ReplaceActual(keep)
		if got, _ := posBatchPostings(t, it, 5); !reflect.DeepEqual(got, wantSub) {
			t.Fatalf("%v subset: read %d postings, want %d", ft, len(got), len(wantSub))
		}
		if len(want) < 4 {
			continue
		}
		it = locIterator(t, v7, ft[0], ft[1])
		if p, err := it.Advance(want[1].doc); err != nil || p == nil || p.Number() != want[1].doc {
			t.Fatalf("advance: %v %v", p, err)
		}
		if p, err := it.Next(); err != nil || p == nil || p.Number() != want[2].doc {
			t.Fatalf("next after advance: %v %v", p, err)
		}
		if got, _ := posBatchPostings(t, it, 3); !reflect.DeepEqual(got, want[3:]) {
			t.Fatalf("%v after advance: read %d postings, want %d", ft, len(got), len(want)-3)
		}
	}

	// the freq > locations composite case
	if p := locPostings(t, v7, "_mix", "quick")[0]; p.freq != 4 || len(p.locs) != 3 || p.locs[1].field != "title" ||
		p.locs[2].field != "_mix" {
		t.Fatalf("composite posting %+v", p)
	}
}

// Offsets of earlier postings load after later ones, and after the
// iterator moved on, as the handles stay valid.
func TestLoadOffsetsOutOfOrder(t *testing.T) {
	seg := buildSegment(t, splitDocs(3000), Version)
	want := locPostings(t, seg, "body", "quick")
	it := locIterator(t, seg, "body", "quick")
	const size = 64
	docs, freqs, norms, ends := make([]uint64, size), make([]int, size), make([]float64, size), make([]int, size)
	type pending struct {
		k    int
		h    uint64
		locs []int
	}
	var all []pending
	k := 0
	for {
		offs := make([]uint64, size)
		n, _, locs, err := it.NextPosBatch(docs, freqs, norms, ends, offs, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		lo := 0
		for j := range n {
			all = append(all, pending{k, offs[j], locs[3*lo : 3*ends[j]]})
			lo = ends[j]
			k++
		}
	}
	if len(all) != len(want) {
		t.Fatalf("read %d postings, want %d", len(all), len(want))
	}
	for i := len(all) - 1; i >= 0; i -= 7 {
		p := all[i]
		if err := it.LoadOffsets(p.h, p.locs); err != nil {
			t.Fatal(err)
		}
		for j, l := range want[p.k].locs {
			if p.locs[3*j+1] != l.start || p.locs[3*j+2] != l.end {
				t.Fatalf("posting %d location %d: offsets %v, want %+v", p.k, j, p.locs[3*j:3*j+3], l)
			}
		}
	}
	for _, p := range all {
		if p.h != NoOffsets {
			if err := it.LoadOffsets(p.h, make([]int, len(p.locs)+3)); err == nil {
				t.Fatal("loaded offsets for more locations than the record holds")
			}
			break
		}
	}
}

// Merging version 6 with version 7 writes version 7, and writing version 6
// from version 7 still works, both keeping every location.
func TestVersion7Merge(t *testing.T) {
	const n = 1500
	docs := splitDocs(n)
	v6 := buildSegment(t, docs, versionCompactPostings)
	v7 := buildSegment(t, docs, versionSplitOffsets)
	for _, tc := range []struct {
		ver  uint32
		segs []segment.Segment
	}{
		{versionSplitOffsets, []segment.Segment{v6, v7}},
		{versionCompactPostings, []segment.Segment{v7, v6}},
	} {
		prev := writeVersion
		writeVersion = tc.ver
		path := filepath.Join(t.TempDir(), "merged.ice")
		_, err := mergeSegments(tc.segs, []*roaring.Bitmap{nil, nil}, path)
		writeVersion = prev
		if err != nil {
			t.Fatal(err)
		}
		merged, closeF, err := openFromFile(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := closeF(); err != nil {
				t.Error(err)
			}
		})
		if merged.Version() != tc.ver {
			t.Fatalf("merged version %d, want %d", merged.Version(), tc.ver)
		}
		for _, ft := range splitTerms {
			want := locPostings(t, v6, ft[0], ft[1])
			got := locPostings(t, merged, ft[0], ft[1])
			if len(got) != 2*len(want) {
				t.Fatalf("v%d %v: merged %d postings, want %d", tc.ver, ft, len(got), 2*len(want))
			}
			for k, p := range want {
				q := got[len(want)+k]
				q.doc -= n
				if !reflect.DeepEqual(got[k], p) || !reflect.DeepEqual(q, p) {
					t.Fatalf("v%d %v: merged posting %d = %+v / %+v, want %+v", tc.ver, ft, k, got[k], q, p)
				}
			}
			if batch, _ := posBatchPostings(t, locIterator(t, merged, ft[0], ft[1]), 32); !reflect.DeepEqual(batch, got) {
				t.Fatalf("v%d %v: merged NextPosBatch differs from Next", tc.ver, ft)
			}
		}
	}
}

// offsetsChunk returns the bytes of a two-document version 7 segment and
// the offset of the flag byte of its "alpha" offsets chunk.
func offsetsChunk(t *testing.T) (data []byte, flagAt int) {
	t.Helper()
	docs := []segment.Document{
		&FakeDocument{NewFakeField("body", "alpha beta alpha", false, true, false)},
		&FakeDocument{NewFakeField("body", "alpha beta alpha", false, true, false)},
	}
	mem, _, err := newWithChunkMode(docs, encodeNorm, chunkModeV1, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err = mem.(*Segment).WriteTo(&buf, nil); err != nil {
		t.Fatal(err)
	}
	seg, err := load(segment.NewDataBytes(buf.Bytes()), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	pl := postingsList(t, seg, "body", "alpha")
	if pl.offOffset == 0 {
		t.Fatal("no offsets stream")
	}
	d, err := newChunkedIntDecoder(seg.data, pl.offOffset, nil, false, true)
	if err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), int(d.dataStartOffset) // #nosec G115 -- a small test segment.
}

// A corrupted offsets stream fails Next and LoadOffsets with an error,
// while positions, kept apart from it, still read.
func TestVersion7CorruptOffsets(t *testing.T) {
	clean, at := offsetsChunk(t)
	// raw chunk: per posting, length 4, then (start delta, length) twice
	record := []byte{4, 0, 5, 22, 5}
	if clean[at] != chunkRaw || !bytes.Equal(clean[at+1:at+11], append(bytes.Clone(record), record...)) {
		t.Fatalf("unexpected offsets chunk % x", clean[at:at+11])
	}
	corrupt := func(t *testing.T, i int, b byte) *Segment {
		data := bytes.Clone(clean)
		data[at+i] = b
		seg, err := load(segment.NewDataBytes(data), DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		return seg
	}
	t.Run("flag", func(t *testing.T) {
		seg := corrupt(t, 0, 9)
		if _, err := locIterator(t, seg, "body", "alpha").Next(); err == nil {
			t.Fatal("Next read a chunk with an unknown flag")
		}
		// freq and norm still read
		if p, err := postingsList(t, seg, "body", "alpha").Iterator(true, true, false, nil); err != nil {
			t.Fatal(err)
		} else if q, err := p.Next(); err != nil || q == nil {
			t.Fatalf("freq/norm read: %v %v", q, err)
		}
	})
	for _, tc := range []struct {
		name string
		at   int // in the second posting's record
		b    byte
	}{
		{"record-length", 0, 0x7f},
		{"short-record", 0, 2},
		{"long-record", 0, 5},
		{"start-before-zero", 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seg := corrupt(t, 1+len(record)+tc.at, tc.b)
			it := locIterator(t, seg, "body", "alpha")
			if p, err := it.Next(); err != nil || p == nil {
				t.Fatalf("first posting: %v %v", p, err)
			}
			if _, err := it.Next(); err == nil {
				t.Fatal("Next read corrupted offsets")
			}
			it = locIterator(t, seg, "body", "alpha")
			docs, freqs, norms, ends, offs := make([]uint64, 2), make([]int, 2), make([]float64, 2), make([]int, 2),
				make([]uint64, 2)
			n, _, locs, err := it.NextPosBatch(docs, freqs, norms, ends, offs, nil, nil)
			if err != nil || n != 2 || offs[0] != NoOffsets || offs[1] == NoOffsets ||
				!reflect.DeepEqual(locs, []int{1, 0, 5, 3, 11, 16, 1, 0, 0, 3, 0, 0}) {
				t.Fatalf("positions: n=%d locs=%v offs=%v err=%v", n, locs, offs, err)
			}
			if err := it.LoadOffsets(offs[1], locs[6:]); err == nil {
				t.Fatalf("loaded corrupted offsets %v", locs[6:])
			}
		})
	}
}

// The inline decode of version 7 records validates them as Next does.
func TestNextPosBatchRejectsBadRecords(t *testing.T) {
	for _, tc := range []struct {
		name string
		locs []byte
		pos  int
		bad  bool
	}{
		{"valid", []byte{2, 0, 4}, 2, false},
		{"multi-byte", []byte{3, 0, 0x80, 1}, 64, false},
		{"field", []byte{2, 1, 4}, 0, true},
		{"negative-position", []byte{2, 0, 3}, 0, true},
		{"count-exceeds-freq", []byte{4, 0, 4, 0, 4}, 0, true},
		{"bytes-exceed-chunk", []byte{5, 0, 4}, 0, true},
		{"truncated-varint", []byte{2, 0, 0x80}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn := binary.AppendUvarint(nil, encodeFreqHasLocs(1, true))
			fn = binary.AppendUvarint(fn, uint64(math.Float32bits(1)))
			bm := roaring.BitmapOf(0, 1)
			actual := bm.Iterator()
			actual.Next()
			it := &PostingsIterator{
				postings: &PostingsList{
					sb:        &Segment{footer: &footer{version: versionSplitOffsets}, fieldsInv: []string{"body"}},
					postings:  bm,
					chunkSize: 1024,
				},
				ActualBM: bm, Actual: actual, started: true,
				includeFreqNorm: true, includeLocs: true, hasLast: true, chunkEnd: 1024, locRec: 1,
				freqNormReader: &chunkedIntDecoder{r: newMemUvarintReader(fn)},
				locReader:      &chunkedIntDecoder{r: newMemUvarintReader(tc.locs)},
			}
			docs, freqs, norms, ends, offs := make([]uint64, 1), make([]int, 1), make([]float64, 1), make([]int, 1),
				make([]uint64, 1)
			n, fields, locs, err := it.NextPosBatch(docs, freqs, norms, ends, offs, nil, nil)
			if (err != nil) != tc.bad {
				t.Fatalf("error=%v want error=%v", err, tc.bad)
			}
			if err == nil && (n != 1 || docs[0] != 1 || fields[0] != "body" || offs[0] != 1 ||
				!reflect.DeepEqual(locs, []int{tc.pos, 0, 0})) {
				t.Fatalf("n=%d docs=%v fields=%v locs=%v offs=%v", n, docs, fields, locs, offs)
			}
		})
	}
}

// Version 7 location streams are smaller than version 6 ones, the
// offsets going apart.
func TestVersion7SplitsLocationBytes(t *testing.T) {
	docs := splitDocs(500)
	v6 := buildSegment(t, docs, versionCompactPostings)
	v7 := buildSegment(t, docs, versionSplitOffsets)
	pl6 := postingsList(t, v6, "body", "quick")
	pl7 := postingsList(t, v7, "body", "quick")
	if pl6.offOffset != 0 || pl7.offOffset <= pl7.locOffset || pl7.locOffset <= pl7.freqOffset {
		t.Fatalf("offsets: v6 %+v v7 %+v", pl6, pl7)
	}
	if loc6, loc7 := pl6.postingsOffset-pl6.locOffset, pl7.offOffset-pl7.locOffset; loc7 >= loc6 {
		t.Fatalf("v7 location stream %d bytes, v6 %d", loc7, loc6)
	}
}

func postingsList(tb testing.TB, seg *Segment, field, term string) *PostingsList {
	tb.Helper()
	dict, err := seg.Dictionary(field)
	if err != nil {
		tb.Fatal(err)
	}
	pl, err := dict.PostingsList([]byte(term), nil, nil)
	if err != nil {
		tb.Fatal(err)
	}
	return pl.(*PostingsList)
}

// benchPhraseSegment indexes n documents shaped like a phrase workload:
// every one holds "search engine" once, near the start.
func benchPhraseSegment(b *testing.B, n int, ver uint32) *Segment {
	b.Helper()
	prev := writeVersion
	writeVersion = ver
	defer func() { writeVersion = prev }()
	docs := make([]segment.Document, n)
	for i := range docs {
		text := "common search engine zig document term" + strconv.Itoa(i%2048) + " term" +
			strconv.Itoa((i*31+7)%2048) + " category" + strconv.Itoa(i%16)
		docs[i] = &FakeDocument{NewFakeField("body", text, false, true, false)}
	}
	mem, _, err := newWithChunkMode(docs, encodeNorm, chunkModeV1, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	return mem.(*Segment)
}

// BenchmarkNextPosBatch reads every posting of a term in every document,
// with positions only, as a phrase match does, per segment version.
func BenchmarkNextPosBatch(b *testing.B) {
	for _, ver := range []uint32{versionCompactPostings, versionSplitOffsets, versionBeforeSlim, Version} {
		seg := benchPhraseSegment(b, 100_000, ver)
		b.Run("v"+strconv.Itoa(int(ver)), func(b *testing.B) { benchPosBatch(b, postingsList(b, seg, "body", "search")) })
	}
}

// benchPosBatch drains pl with NextPosBatch once per iteration.
func benchPosBatch(b *testing.B, pl *PostingsList) {
	const size = 128
	docs, freqs, norms, ends, offs := make([]uint64, size), make([]int, size), make([]float64, size),
		make([]int, size), make([]uint64, size)
	var fields []string
	var locs []int
	var it segment.PostingsIterator
	for b.Loop() {
		var err error
		if it, err = pl.Iterator(true, true, true, it); err != nil {
			b.Fatal(err)
		}
		pi := it.(*PostingsIterator)
		for {
			var n int
			n, fields, locs, err = pi.NextPosBatch(docs, freqs, norms, ends, offs, fields[:0], locs[:0])
			if err != nil {
				b.Fatal(err)
			}
			if n == 0 {
				break
			}
		}
	}
}
