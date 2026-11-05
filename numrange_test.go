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
		decodeNumericBlock(got, data, tc.width, tc.minVal)
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
