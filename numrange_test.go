package ice

import (
	"encoding/binary"
	"math"
	"slices"
	"strconv"
	"testing"

	segment "github.com/vcaesar/bluge_segment_api"
)

// NumericRange must match a value-by-value filter over dense (one value per
// doc), sparse and multi-valued columns, including block-boundary ranges.
func TestNumericRange(t *testing.T) {
	const n = 3000 // spans three column blocks
	ids := make([]string, n)
	for i := range ids {
		ids[i] = strconv.Itoa(i)
	}
	for _, multi := range []bool{false, true} {
		seg, err := buildTypedSegment(ids, -500, multi)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range [][2]int64{{-500, 2499}, {0, 0}, {-10, 10}, {523, 1547}, {-9999, -501}, {2499, 9999}, {5, 4}} {
			bm, ok, err := seg.NumericRange("price", r[0], r[1])
			if err != nil || !ok {
				t.Fatalf("range %v: ok=%v err=%v", r, ok, err)
			}
			var want []uint32
			for i := range n {
				v := int64(i) - 500
				if (v >= r[0] && v <= r[1]) || (multi && -v >= r[0] && -v <= r[1]) {
					want = append(want, uint32(i))
				}
			}
			got := bm.ToArray()
			if len(got) != len(want) {
				t.Fatalf("multi=%v range %v: %d docs want %d", multi, r, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("multi=%v range %v: doc %d want %d", multi, r, got[i], want[i])
				}
			}
		}
		if _, ok, err := seg.NumericRange("_id", 0, 1); ok || err != nil {
			t.Fatalf("non-numeric field: ok=%v err=%v", ok, err)
		}
	}
}

// VisitNumericColumn yields every value of the column in doc order.
func TestVisitNumericColumn(t *testing.T) {
	ids := make([]string, 2500)
	for i := range ids {
		ids[i] = strconv.Itoa(i)
	}
	for _, multi := range []bool{false, true} {
		seg, err := buildTypedSegment(ids, 10, multi)
		if err != nil {
			t.Fatal(err)
		}
		var sum int64
		n, last := 0, uint64(0)
		ok, err := seg.VisitNumericColumn("price", func(doc uint64, v int64) error {
			if doc < last {
				t.Fatalf("doc %d after %d", doc, last)
			}
			last = doc
			sum += v
			n++
			return nil
		})
		if !ok || err != nil {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
		wantN, wantSum := 2500, int64(2500*10+2500*2499/2)
		if multi {
			wantN, wantSum = 5000, 0
		}
		if n != wantN || sum != wantSum {
			t.Fatalf("multi=%v: n=%d sum=%d want %d %d", multi, n, sum, wantN, wantSum)
		}
		if colOK, _ := seg.VisitNumericColumn("_id", nil); colOK {
			t.Fatal("non-numeric field reported a column")
		}
		// the block scan yields the same (doc, value) sequence
		var perValue, perBlock [][2]int64
		if _, err = seg.VisitNumericColumn("price", func(doc uint64, v int64) error {
			perValue = append(perValue, [2]int64{int64(doc), v}) // #nosec G115 -- test doc numbers are small.
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		ok, err = seg.VisitNumericBlocks("price", func(base uint64, docs []uint64, vals []int64) error {
			if docs != nil && len(docs) != len(vals) {
				t.Fatalf("docs %d vals %d", len(docs), len(vals))
			}
			for i, v := range vals {
				doc := base + uint64(i)
				if docs != nil {
					doc = docs[i]
				}
				perBlock = append(perBlock, [2]int64{int64(doc), v}) // #nosec G115 -- test doc numbers are small.
			}
			return nil
		})
		if !ok || err != nil || !slices.Equal(perValue, perBlock) {
			t.Fatalf("multi=%v: blocks ok=%v err=%v n=%d want %d", multi, ok, err, len(perBlock), len(perValue))
		}
	}
}

// decodeNumericBlock handles every byte width.
func TestDecodeNumericBlockWidths(t *testing.T) {
	for _, tc := range []struct {
		minVal, maxVal int64
		width          uint8
	}{
		{-5, -5, widthNone}, {-3, 200, widthByte}, {-70000, -10, widthDword},
		{1000, 60000, widthWord}, {math.MinInt64 / 2, math.MaxInt64 / 2, widthQword},
	} {
		if w := numericWidth(tc.minVal, tc.maxVal); w != tc.width {
			t.Fatalf("width(%d,%d)=%d want %d", tc.minVal, tc.maxVal, w, tc.width)
		}
		want := []int64{tc.minVal, tc.maxVal, tc.minVal + (tc.maxVal-tc.minVal)/3}
		var data []byte
		for _, v := range want {
			// #nosec G115 -- two's-complement offset from minVal, as the encoder writes it.
			data = binary.LittleEndian.AppendUint64(data, uint64(v)-uint64(tc.minVal))[:len(data)+int(tc.width)]
		}
		got := make([]int64, len(want))
		decodeNumericBlock(got, data, tc.width, 0, tc.minVal)
		if !slices.Equal(got, want) {
			t.Fatalf("width %d: got %v want %v", tc.width, got, want)
		}
	}
}

// A block that fails to load must fail again on the next visit, not pass
// from the cursor's cache as a block without values.
func TestNumericCursorLoadErrorNotCached(t *testing.T) {
	s := &Segment{data: segment.NewDataBytes(make([]byte, 4))}
	col := &numericColumn{field: "n", dataEnd: 64, blocks: []numericBlockMeta{{count: 2, width: widthByte, max: 1}}}
	cur := newNumericColumnCursor(col)
	for range 2 {
		if err := cur.visit(s, 0, func(string, int64) { t.Fatal("value from a failed block") }); err == nil {
			t.Fatal("visit of an out of range block succeeded")
		}
	}
}

// NumericRange declines a column whose documents differ from the field's
// indexed ones, so a range matches as the field's terms do.
func TestNumericRangeColumnMustMatchIndex(t *testing.T) {
	notIndexed := NewFakeNumericField("price", 5)
	notIndexed.T = nil
	termOnly := fakeTermField("price", prefixCodedInt64(5))
	termOnly.DV = false
	for name, docs := range map[string][]segment.Document{
		"not indexed": {&numericDoc{fields: []segment.Field{NewFakeField("_id", "a", true, false, false), notIndexed}}},
		"term only": {
			&numericDoc{fields: []segment.Field{NewFakeField("_id", "a", true, false, false), NewFakeNumericField("price", 5)}},
			&numericDoc{fields: []segment.Field{NewFakeField("_id", "b", true, false, false), termOnly}},
		},
	} {
		s, _, err := newWithChunkMode(docs, encodeNorm, defaultChunkMode, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		seg := s.(*Segment)
		for range 2 { // the second answer comes from the cached check
			if _, ok, err := seg.NumericRange("price", 0, 10); ok || err != nil {
				t.Fatalf("%s: ok=%v err=%v, want the column declined", name, ok, err)
			}
		}
		if ok, err := seg.VisitNumericColumn("price", func(uint64, int64) error { return nil }); !ok || err != nil {
			t.Fatalf("%s: column missing: ok=%v err=%v", name, ok, err)
		}
	}
}

// NumericRange on a large segment: a narrow range adds its decoded hits
// directly rather than through a bitset of the whole segment, and a wide
// one joins whole blocks into runs, not 8KB bitmap containers. Both match
// a value filter. (TestNumericRange covers the bitset path.)
func TestNumericRangeNarrowAndWide(t *testing.T) {
	const n = numericRangeDenseRatio*numericBlockSize + 5000
	ids := make([]string, n)
	for i := range ids {
		ids[i] = strconv.Itoa(i)
	}
	seg, err := buildTypedSegment(ids, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][2]int64{{5000, 5100}, {0, 0}, {n - 1, n + 5}, {0, n}, {100, n - 100}} {
		bm, ok, err := seg.NumericRange("price", r[0], r[1])
		if err != nil || !ok {
			t.Fatalf("range %v: ok=%v err=%v", r, ok, err)
		}
		lo, hi := max(r[0], 0), min(r[1], n-1)
		// #nosec G115 -- test values are small and nonnegative.
		if bm.GetCardinality() != uint64(hi-lo+1) || bm.Minimum() != uint32(lo) || bm.Maximum() != uint32(hi) {
			t.Fatalf("range %v: %d docs in [%d, %d]", r, bm.GetCardinality(), bm.Minimum(), bm.Maximum())
		}
		if size := bm.GetSizeInBytes(); size > 1024 {
			t.Fatalf("range %v: %d bytes for one span, want runs", r, size)
		}
	}
}

// setDenseRange sets exactly the bits of [from, to), across word edges.
func TestSetDenseRange(t *testing.T) {
	for _, r := range [][2]uint64{{0, 0}, {0, 1}, {3, 64}, {0, 128}, {63, 65}, {5, 200}, {64, 192}, {130, 131}} {
		dense := make([]uint64, 4)
		setDenseRange(dense, r[0], r[1])
		for i := range uint64(256) {
			got := dense[i/64]&(1<<(i%64)) != 0
			if want := i >= r[0] && i < r[1]; got != want {
				t.Fatalf("range %v: bit %d = %v", r, i, got)
			}
		}
	}
}

// BenchmarkNumericRange covers a wide range, which takes the dense bitset,
// and a narrow one, which adds its few documents directly.
func BenchmarkNumericRange(b *testing.B) {
	const n = 1 << 20
	ids := make([]string, n)
	for i := range ids {
		ids[i] = strconv.Itoa(i)
	}
	seg, err := buildTypedSegment(ids, 0, false)
	if err != nil {
		b.Fatal(err)
	}
	for _, r := range []struct {
		name   string
		lo, hi int64
	}{{"all", 0, n}, {"half", n / 4, 3 * n / 4}, {"narrow", 5000, 5100}} {
		b.Run(r.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := seg.NumericRange("price", r.lo, r.hi); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestNumericCursorPoolReuse alternates segments whose blocks differ in
// count and density, so a pooled cursor reused from the other segment
// must not leak its docs or values into the result.
func TestNumericCursorPoolReuse(t *testing.T) {
	segs := make([]*Segment, 2)
	sizes := []int{3000, 700}
	for k, n := range sizes {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = strconv.Itoa(i)
		}
		seg, err := buildTypedSegment(ids, int64(k*10000), k == 1)
		if err != nil {
			t.Fatal(err)
		}
		segs[k] = seg
	}
	for round := range 20 {
		k := round % 2
		base := int64(k * 10000)
		bm, ok, err := segs[k].NumericRange("price", base+100, base+650)
		if err != nil || !ok {
			t.Fatalf("round %d: ok=%v err=%v", round, ok, err)
		}
		if got := bm.GetCardinality(); got != 551 {
			t.Fatalf("round %d: %d docs, want 551", round, got)
		}
		var count int
		var sum int64
		ok, err = segs[k].VisitNumericColumn("price", func(_ uint64, v int64) error {
			count++
			sum += v
			return nil
		})
		if err != nil || !ok {
			t.Fatalf("round %d: visit ok=%v err=%v", round, ok, err)
		}
		n := int64(sizes[k])
		wantCount, wantSum := n, n*base+n*(n-1)/2
		if k == 1 { // multi: each doc also holds -v
			wantCount, wantSum = 2*n, 0
		}
		if int64(count) != wantCount || sum != wantSum {
			t.Fatalf("round %d: %d values sum %d, want %d sum %d", round, count, sum, wantCount, wantSum)
		}
	}
}

// TestNumericShiftShrinksFloatIDs writes float64-encoded integer ids, whose
// deltas share many trailing zero bits, and reads them back exactly from a
// version 9 column (shifted) and a version 8 one (not shifted).
func TestNumericShiftShrinksFloatIDs(t *testing.T) {
	const n = 5000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(math.Float64bits(float64(i))) // #nosec G115 -- nonnegative floats.
	}
	vals[17] = -3 // a block mixing a small value, which still decodes
	sizes := map[uint32]int{}
	for _, ver := range []uint32{8, Version} {
		docs := make([]segment.Document, n)
		for i := range docs {
			docs[i] = &numericDoc{fields: []segment.Field{
				NewFakeField("_id", strconv.Itoa(i), true, false, false), NewFakeNumericField("n", vals[i])}}
		}
		s := buildSegment(t, docs, ver)
		col := s.fieldNumCols[s.fieldsMap["n"]-1]
		if col == nil {
			t.Fatalf("version %d: no numeric column", ver)
		}
		got := make([]int64, n)
		ok, err := s.VisitNumericColumn("n", func(doc uint64, v int64) error { got[doc] = v; return nil })
		if err != nil || !ok || !slices.Equal(got, vals) {
			t.Fatalf("version %d: values differ (ok=%v err=%v)", ver, ok, err)
		}
		sizes[ver] = int(col.dataEnd) // #nosec G115 -- a small test column.
		if ver == Version {
			// block 0 holds -3, so its deltas are odd: no shift, 8 bytes
			if b := col.blocks[0]; b.shift != 0 || b.width != widthQword {
				t.Fatalf("block 0 shift %d width %d, want 0 and 8", b.shift, b.width)
			}
			for i, b := range col.blocks[1:] {
				if b.shift < 40 || b.width != widthWord {
					t.Fatalf("block %d shift %d width %d, want >= 40 and 2", i+1, b.shift, b.width)
				}
			}
		}
	}
	if sizes[Version]*2 > sizes[8] {
		t.Fatalf("shifted column %d bytes, want < half of %d", sizes[Version], sizes[8])
	}
}
