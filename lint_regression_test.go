package ice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

func TestIntDecoderHeaderBounds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		data   []byte
		offset uint64
	}{
		{"offset overflow", []byte{0}, math.MaxUint64},
		{"offset at end", []byte{0}, 1},
		{"truncated count", []byte{0, 0x80}, 1},
		{"overflowing count varint", append([]byte{0}, bytes.Repeat([]byte{0xff}, 10)...), 1},
		{"count exceeds data", binary.AppendUvarint([]byte{0}, math.MaxUint64), 1},
		{"truncated offset", []byte{0, 1, 0x80}, 1},
		{"overflowing offset varint", append([]byte{0, 1}, bytes.Repeat([]byte{0xff}, 10)...), 1},
		{"offset exceeds data", binary.AppendUvarint([]byte{0, 1}, math.MaxUint64), 1},
		{"decreasing offsets", []byte{0, 2, 1, 0, 0}, 1},
		{"missing second offset", []byte{0, 2, 0x80, 1}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newChunkedIntDecoder(segment.NewDataBytes(tc.data), tc.offset, nil); err == nil {
				t.Fatal("accepted invalid integer chunk header")
			}
		})
	}
}

func TestIntDecoderShortHeaderAndReuse(t *testing.T) {
	var decoder *chunkedIntDecoder
	for _, offsets := range [][]byte{{0, 0}, {0}, {}, {0, 0, 0}} {
		data := append(binary.AppendUvarint([]byte{0}, uint64(len(offsets))), offsets...)
		var err error
		decoder, err = newChunkedIntDecoder(segment.NewDataBytes(data), 1, decoder)
		if err != nil {
			t.Fatal(err)
		}
		if len(decoder.chunkOffsets) != len(offsets) || decoder.dataStartOffset != uint64(len(data)) {
			t.Fatalf("incorrect decoded header: %+v", decoder)
		}
	}
	decoder, err := newChunkedIntDecoder(nil, termNotEncoded, decoder)
	if err != nil || len(decoder.chunkOffsets) != 0 || decoder.dataStartOffset != 0 {
		t.Fatalf("incorrect absent header: %+v, %v", decoder, err)
	}
}

func TestIntCoderChunkSizeBoundsAndReuse(t *testing.T) {
	coder := newChunkedIntCoder(1, 0)
	for _, count := range []uint64{4, 2, 1} {
		coder.SetChunkSize(1, count-1)
		if uint64(len(coder.chunkLens)) != count {
			t.Fatalf("incorrect chunk count: %d", len(coder.chunkLens))
		}
	}
	for _, maxDocNum := range []uint64{math.MaxInt, math.MaxUint64} {
		t.Run(fmt.Sprint(maxDocNum), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("accepted overflowing chunk count")
				}
			}()
			coder.SetChunkSize(1, maxDocNum)
		})
	}
}

func TestEncodeNormBounds(t *testing.T) {
	for _, count := range []int{0, 1, math.MaxInt32} {
		if got := decodeNorm("", encodeNorm("", count)); got != count {
			t.Fatalf("term count %d became %d", count, got)
		}
	}
	for _, count := range []int{-1, math.MaxInt} {
		if count >= 0 && uint64(count) <= math.MaxUint32 {
			continue
		}
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("accepted out-of-range term count")
				}
			}()
			encodeNorm("", count)
		})
	}
}

func TestIntDecoderChunkBounds(t *testing.T) {
	for _, tc := range []struct {
		base    uint64
		offsets []uint64
		chunk   int
	}{
		{0, []uint64{0}, -1},
		{0, []uint64{0}, 1},
		{math.MaxUint64, []uint64{1}, 0},
		{1, []uint64{math.MaxUint64}, 0},
		{0, []uint64{1, 0}, 1},
	} {
		d := &chunkedIntDecoder{
			startOffset: 1, dataStartOffset: tc.base, chunkOffsets: tc.offsets,
			data: segment.NewDataBytes([]byte{0}),
		}
		if err := d.loadChunk(tc.chunk); err == nil {
			t.Fatalf("accepted invalid chunk: %+v", tc)
		}
	}
}

func TestLoadFieldsIndexBounds(t *testing.T) {
	for _, offset := range []uint64{math.MaxUint64, 9, 1} {
		s := &Segment{data: segment.NewDataBytes(make([]byte, 8)), footer: &footer{fieldsIndexOffset: offset}}
		if err := s.loadFields(); err == nil {
			t.Fatalf("accepted invalid index offset %d", offset)
		}
	}
	s := &Segment{data: segment.NewDataBytes([]byte{}), footer: &footer{}}
	if err := s.loadFields(); err != nil {
		t.Fatalf("empty index: %v", err)
	}
}

func TestPostingsExcludedChunkSizeBounds(t *testing.T) {
	for _, size := range []uint64{0, 1, math.MaxUint32, math.MaxUint32 + 1, math.MaxUint64} {
		list := &PostingsList{postings: roaring.BitmapOf(0, 1), except: roaring.BitmapOf(0), chunkSize: size}
		itr, err := list.Iterator(false, false, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		hit, err := itr.Next()
		if size == 0 {
			if err == nil {
				t.Fatal("accepted zero chunk size")
			}
		} else if err != nil || hit == nil || hit.Number() != 1 {
			t.Fatalf("chunk size %d: hit %v, err %v", size, hit, err)
		}
	}
}

func TestPostingsNormBitsBounds(t *testing.T) {
	for _, norm := range []uint64{math.MaxUint32, math.MaxUint32 + 1, math.MaxUint64} {
		coder := newChunkedIntCoder(1, 0)
		if err := coder.Add(0, encodeFreqHasLocs(1, false), norm); err != nil {
			t.Fatal(err)
		}
		if err := coder.Close(); err != nil {
			t.Fatal(err)
		}
		buf := bytes.NewBuffer([]byte{0})
		if _, err := coder.Write(buf); err != nil {
			t.Fatal(err)
		}
		list := &PostingsList{
			postings: roaring.BitmapOf(0), chunkSize: 1, freqOffset: 1,
			sb: &Segment{data: segment.NewDataBytes(buf.Bytes())},
		}
		itr, err := list.Iterator(true, true, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		hit, err := itr.Next()
		if norm > math.MaxUint32 {
			if err == nil {
				t.Fatal("accepted overflowing norm bits")
			}
		} else if err != nil || hit == nil {
			t.Fatalf("valid norm bits rejected: %v", err)
		}
	}
}

func TestSetupTestDirCleanup(t *testing.T) {
	path, cleanup := setupTestDir(t)
	if err := os.WriteFile(filepath.Join(path, "fixture"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	cleanup()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleanup did not remove directory: %v", err)
	}
	cleanup() // Preserve the idempotent explicit cleanup interface.
}

func TestTimestampWireRoundTrip(t *testing.T) {
	for _, timestamp := range []int64{math.MinInt64, -1, 0, 1, math.MaxInt64} {
		doc := timestampDocument{&FakeDocument{NewFakeField(_idFieldName, "a", true, false, false)}, timestamp}
		s, _, err := New([]segment.Document{doc}, encodeNorm)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if _, err = s.WriteTo(&buf, nil); err != nil {
			t.Fatal(err)
		}
		f, err := parseFooter(segment.NewDataBytes(buf.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		minimum, maximum := (&Segment{footer: f}).Timestamp()
		if minimum != timestamp || maximum != timestamp {
			t.Fatalf("timestamp %d became (%d, %d)", timestamp, minimum, maximum)
		}
	}
}

func TestPostingsAdvanceDocumentNumberBounds(t *testing.T) {
	for _, except := range []*roaring.Bitmap{nil, roaring.BitmapOf(0)} {
		for _, target := range []uint64{math.MaxUint32, math.MaxUint32 + 1, math.MaxUint64} {
			list := &PostingsList{
				postings: roaring.BitmapOf(0, 1, math.MaxUint32), except: except, chunkSize: 1,
			}
			itr, err := list.Iterator(false, false, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			hit, err := itr.Advance(target)
			if err != nil {
				t.Fatal(err)
			}
			if target == math.MaxUint32 {
				if hit == nil || hit.Number() != target {
					t.Fatalf("missing maximum document number: %v", hit)
				}
			} else if hit != nil {
				t.Fatalf("Advance(%d) wrapped to document %d", target, hit.Number())
			}
			if hit, err = itr.Next(); err != nil || hit != nil {
				t.Fatalf("exhausted iterator returned %v, %v", hit, err)
			}
		}
	}
}

func TestFieldIDBounds(t *testing.T) {
	s := &interim{FieldsMap: make(map[string]uint16)}
	for n := 0; n < math.MaxUint16; n++ {
		id, err := s.getOrDefineField(fmt.Sprint(n))
		if err != nil || int(id) != n {
			t.Fatalf("field %d: id=%d, err=%v", n, id, err)
		}
	}
	if _, err := s.getOrDefineField("overflow"); err == nil {
		t.Fatal("accepted overflowing field ID")
	}
	if id, err := s.getOrDefineField("0"); err != nil || id != 0 {
		t.Fatalf("existing field at capacity: %d, %v", id, err)
	}
	fields, err := mapFields(s.FieldsInv)
	if err != nil || fields[fmt.Sprint(math.MaxUint16-1)] != math.MaxUint16 {
		t.Fatalf("maximum merged field ID: %v", err)
	}
	if _, err = mapFields(append(s.FieldsInv, "overflow")); err == nil {
		t.Fatal("accepted overflowing merged field ID")
	}
}

func TestMergeDocumentNumberBounds(t *testing.T) {
	s := &Segment{footer: &footer{numDocs: uint64(math.MaxUint32) + 2}}
	if _, _, err := mergeToWriter([]*Segment{s}, []*roaring.Bitmap{nil}, defaultChunkMode,
		newCountHashWriter(io.Discard), nil); err == nil {
		t.Fatal("accepted too many merged documents")
	}
	for _, docNum := range []uint64{math.MaxUint32, math.MaxUint32 + 1} {
		itr := &PostingsIterator{normBits1Hit: 1, includeFreqNorm: true}
		bitmap := roaring.New()
		lastDoc, freq, norm, locs, err := mergeTermFreqNormLocs(nil, itr, []uint64{docNum}, bitmap,
			newChunkedIntCoder(uint64(math.MaxUint32), docNum),
			newChunkedIntCoder(uint64(math.MaxUint32), docNum), nil, roaring.New())
		if docNum == math.MaxUint32 {
			if err != nil || !bitmap.Contains(math.MaxUint32) {
				t.Fatalf("maximum document number rejected: %v", err)
			}
			if lastDoc != docNum || freq != 1 || norm != 1 || len(locs) != 0 {
				t.Fatalf("incorrect merged posting: %d, %d, %d, %v", lastDoc, freq, norm, locs)
			}
		} else if err == nil || !bitmap.IsEmpty() {
			t.Fatalf("overflowing document number accepted: %v", err)
		}
	}
}

func TestMergedDocumentCountBounds(t *testing.T) {
	for _, tc := range []struct {
		counts  []uint64
		drops   []*roaring.Bitmap
		want    uint64
		wantErr bool
	}{
		{[]uint64{math.MaxUint32, 1}, []*roaring.Bitmap{nil, nil}, uint64(math.MaxUint32) + 1, false},
		{[]uint64{math.MaxUint32, 2}, []*roaring.Bitmap{nil, nil}, 0, true},
		{[]uint64{math.MaxUint32, 2}, []*roaring.Bitmap{nil, roaring.BitmapOf(0)}, uint64(math.MaxUint32) + 1, false},
		{[]uint64{math.MaxUint64, 1}, []*roaring.Bitmap{nil, nil}, 0, true},
		{[]uint64{0}, []*roaring.Bitmap{roaring.BitmapOf(0)}, 0, true},
	} {
		segments := make([]*Segment, len(tc.counts))
		for n, count := range tc.counts {
			segments[n] = &Segment{footer: &footer{numDocs: count}}
		}
		got, err := computeNewDocCount(segments, tc.drops)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Fatalf("counts %v: got %d, %v; want %d, error=%v", tc.counts, got, err, tc.want, tc.wantErr)
		}
	}
}

func TestDocValueFieldBounds(t *testing.T) {
	for _, bounds := range [][2]uint64{{1, 0}, {0, math.MaxUint64}, {0, fieldDvStartEndWidth}} {
		s := &Segment{data: segment.NewDataBytes(make([]byte, 32))}
		if _, err := s.loadFieldDocValueReader("field", bounds[0], bounds[1]); err == nil {
			t.Fatalf("accepted field range %v", bounds)
		}
	}
	for _, tc := range []struct {
		name          string
		length, count uint64
		offsets       []byte
	}{
		{"length overflow", math.MaxUint64, 1, []byte{1}},
		{"count overflow", 1, math.MaxUint64, []byte{1}},
		{"offset overflow", 10, 1, binary.AppendUvarint(nil, math.MaxUint64)},
		{"truncated varint", 1, 1, []byte{0x80}},
		{"decreasing offsets", 2, 2, []byte{1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := append([]byte{0}, tc.offsets...)
			data = binary.BigEndian.AppendUint64(data, tc.length)
			data = binary.BigEndian.AppendUint64(data, tc.count)
			s := &Segment{data: segment.NewDataBytes(data)}
			if _, err := s.loadFieldDocValueReader("field", 0, uint64(len(data))); err == nil {
				t.Fatal("accepted corrupt chunk offsets")
			}
		})
	}
}

func TestDocValueChunkBounds(t *testing.T) {
	for _, data := range [][]byte{
		{0x80}, binary.AppendUvarint(nil, math.MaxUint64), {1, 0x80, 0x80},
		{1, 0, 0x80},
		append(append([]byte{2}, binary.AppendUvarint(nil, math.MaxUint64)...), 0, 1, 0),
		append(append([]byte{2, 0}, binary.AppendUvarint(nil, math.MaxUint64)...), 0, 1),
	} {
		s := &Segment{data: segment.NewDataBytes(data)}
		d := &docValueReader{chunkOffsets: []uint64{uint64(len(data))}}
		if err := d.loadDvChunk(0, s); err == nil {
			t.Fatalf("accepted corrupt chunk %x", data)
		}
	}
	s := &Segment{data: segment.NewDataBytes([]byte{0})}
	for _, offsets := range [][]uint64{nil, {math.MaxUint64}, {1, 0}} {
		d := &docValueReader{chunkOffsets: offsets}
		chunk := uint64(0)
		if len(offsets) == 2 {
			chunk = 1
		}
		if err := d.loadDvChunk(chunk, s); err == nil {
			t.Fatalf("accepted invalid chunk boundary %v", offsets)
		}
	}
}

func TestContentCoderOffsetsAndReuse(t *testing.T) {
	var out bytes.Buffer
	c := newChunkedContentCoder(2, 1, &out, false)
	c.SetChunkSize(2, 5)
	c.SetChunkSize(2, 3)
	for n, data := range [][]byte{[]byte("a"), []byte("bc")} {
		if err := c.Add(uint64(n), data); err != nil {
			t.Fatal(err)
		}
	}
	if c.chunkMeta[0].DocDvOffset != 1 || c.chunkMeta[1].DocDvOffset != 3 {
		t.Fatalf("incorrect offsets: %v", c.chunkMeta)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(); err != nil {
		t.Fatal(err)
	}
	s := &Segment{data: segment.NewDataBytes(out.Bytes())}
	d, err := s.loadFieldDocValueReader("field", 0, uint64(len(out.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	var values []string
	if err = d.iterateAllDocValues(s, func(_ uint64, terms []byte) error {
		values = append(values, string(terms))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(values) != "[a bc]" {
		t.Fatalf("round trip: %v", values)
	}
}

func TestNewRejectsTooManyFields(t *testing.T) {
	doc := make(FakeDocument, math.MaxUint16)
	for n := range doc {
		doc[n] = NewFakeField(fmt.Sprint(n), "", false, false, false)
	}
	// New also defines _id, which takes the field count over the limit.
	if _, _, err := New([]segment.Document{&doc}, encodeNorm); err == nil {
		t.Fatal("accepted too many fields")
	}
}

func TestDocValueVisitPropagatesBoundsError(t *testing.T) {
	d := &docValueReader{
		curChunkHeader: []metaData{{DocNum: 0, DocDvOffset: 2}},
		uncompressed:   []byte{1},
	}
	s := &Segment{fieldsMap: map[string]uint16{"field": 1}}
	state := &docVisitState{segment: s, dvrs: map[uint16]*docValueReader{0: d}}
	if _, err := s.visitDocumentFieldTerms(0, []string{"field"}, func(_ string, _ []byte) {
		t.Error("visitor called for invalid data")
	}, state); err == nil {
		t.Fatal("doc-value bounds error was discarded")
	}
}

type failingChunkWriter struct{}

func (failingChunkWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func TestDocumentCoderWriterAndOffsets(t *testing.T) {
	c := newChunkedDocumentCoder(1, failingChunkWriter{})
	if _, err := c.Add(0, nil, []byte("data")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writer error: %v", err)
	}
	var out bytes.Buffer
	c = newChunkedDocumentCoder(1, &out)
	if _, err := c.Add(0, nil, []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(); err != nil {
		t.Fatal(err)
	}
	data := out.Bytes()
	if len(data) < 8 {
		t.Fatal("missing chunk trailer")
	}
	count := binary.BigEndian.Uint32(data[len(data)-4:])
	size := binary.BigEndian.Uint32(data[len(data)-8:])
	// #nosec G115 -- the trailer length check above ensures len(data) >= 8.
	if uint64(count) != uint64(len(c.offsets)) || size == 0 || uint64(size) > uint64(len(data)-8) {
		t.Fatalf("incorrect chunk trailer: count=%d size=%d", count, size)
	}
}

func TestPostingChunkBounds(t *testing.T) {
	i := &PostingsIterator{}
	if err := i.loadChunk(-1); err == nil {
		t.Fatal("accepted negative chunk")
	}
	if err := i.loadChunk(1); err != nil || i.currChunk != 1 {
		t.Fatalf("valid chunk: %v", err)
	}
}

func TestChunkedIntDecoderIsNil(t *testing.T) {
	for _, data := range [][]byte{nil, {}, {1}} {
		d := &chunkedIntDecoder{curChunkBytes: data}
		if got, want := d.isNil(), len(data) == 0; got != want {
			t.Fatalf("isNil(%v) = %v, want %v", data, got, want)
		}
	}
}
