package ice

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"

	segment "github.com/vcaesar/bluge_segment_api"
)

func TestSegmentDataReadBounds(t *testing.T) {
	for _, data := range []*segment.Data{segment.NewDataBytes([]byte{0, 1}), optimizationFileData(t, []byte{0, 1})} {
		for _, bounds := range [][2]uint64{{math.MaxUint64, 1}, {1, math.MaxUint64}, {2, 1}, {1, 2}} {
			if _, err := readDataAt(data, bounds[0], bounds[1]); err == nil {
				t.Fatalf("accepted range %v", bounds)
			}
		}
		if got, err := readDataAt(data, 1, 1); err != nil || !bytes.Equal(got, []byte{1}) {
			t.Fatalf("valid read: %x, %v", got, err)
		}
		pos := uint64(1)
		if got, err := readDataUvarint(data, &pos); err != nil || got != 1 || pos != 2 {
			t.Fatalf("short varint: %d, %v, position %d", got, err, pos)
		}
		for _, offset := range []uint64{2, math.MaxUint64} {
			if _, err := readDataUvarint(data, &offset); err == nil {
				t.Fatal("accepted out-of-range varint")
			}
		}
	}
	for _, buf := range [][]byte{{0x80}, bytes.Repeat([]byte{0xff}, 10)} {
		pos := uint64(0)
		if _, err := readDataUvarint(segment.NewDataBytes(buf), &pos); err == nil || pos != 0 {
			t.Fatalf("invalid varint advanced cursor: %d, %v", pos, err)
		}
	}
}

func TestStoredChunkTrailerBounds(t *testing.T) {
	for _, index := range []uint64{0, 7, math.MaxUint64} {
		s := &Segment{data: segment.NewDataBytes(make([]byte, 8)), footer: &footer{storedIndexOffset: index}}
		if err := s.loadStoredFieldChunk(); err == nil {
			t.Fatalf("accepted trailer offset %d", index)
		}
	}
	for _, tc := range []struct {
		offsets []byte
		length  uint32
		count   uint32
	}{
		{[]byte{0}, math.MaxUint32, 1},
		{[]byte{0}, 1, math.MaxUint32},
		{[]byte{0x80}, 1, 1},
		{[]byte{1, 0}, 2, 2},
		{[]byte{2}, 1, 1},
	} {
		data := append([]byte{0}, tc.offsets...)
		data = binary.BigEndian.AppendUint32(data, tc.length)
		data = binary.BigEndian.AppendUint32(data, tc.count)
		s := &Segment{data: segment.NewDataBytes(data), footer: &footer{storedIndexOffset: uint64(len(data))}}
		if err := s.loadStoredFieldChunk(); err == nil {
			t.Fatalf("accepted chunk trailer %+v", tc)
		}
	}
}

func TestStoredDocumentNumberBounds(t *testing.T) {
	s := &Segment{data: segment.NewDataBytes(make([]byte, 8)), footer: &footer{numDocs: 1}}
	if index, offset, err := s.getDocStoredOffsetsOnly(0); err != nil || index != 0 || offset != 0 {
		t.Fatalf("valid stored offsets: %d, %d, %v", index, offset, err)
	}
	for _, doc := range []uint64{1, math.MaxUint32 + 1, math.MaxUint64} {
		if _, _, err := s.getDocStoredOffsetsOnly(doc); err == nil {
			t.Fatalf("accepted document %d", doc)
		}
	}
	if _, _, err := s.getDocStoredMetaAndUnCompressed(0); err == nil {
		t.Fatal("accepted missing stored chunk")
	}
	s.footer.storedIndexOffset = math.MaxUint64
	if _, _, err := s.getDocStoredOffsetsOnly(0); err == nil {
		t.Fatal("accepted overflowing stored index")
	}
}

func TestPostingHeaderBounds(t *testing.T) {
	for _, data := range [][]byte{
		{0, 0x80},
		binary.AppendUvarint([]byte{0}, math.MaxUint64),
		binary.AppendUvarint(binary.AppendUvarint([]byte{0}, math.MaxUint64), 1),
		binary.AppendUvarint([]byte{0, 0, 0}, math.MaxUint64),
	} {
		p := &PostingsList{}
		d := &Dictionary{sb: &Segment{data: segment.NewDataBytes(data)}}
		if err := p.read(1, d); err == nil {
			t.Fatalf("accepted posting header %x", data)
		}
	}
}

func TestLocationIntegerBounds(t *testing.T) {
	for n := range 3 {
		vals := []uint64{0, 1, 1, 1}
		vals[n+1] = math.MaxUint64
		var data []byte
		for _, value := range vals {
			data = binary.AppendUvarint(data, value)
		}
		itr := &PostingsIterator{
			postings:  &PostingsList{sb: &Segment{fieldsInv: []string{"field"}}},
			locReader: &chunkedIntDecoder{r: newMemUvarintReader(data)},
		}
		if err := itr.readLocation(&Location{}); err == nil {
			t.Fatalf("accepted overflowing location component %d", n)
		}
	}
}

func TestCountWriterOverflow(t *testing.T) {
	var buf bytes.Buffer
	w := newCountHashWriter(&buf)
	w.n = math.MaxInt - 1
	if n, err := w.Write([]byte{1}); err != nil || n != 1 || w.Count() != math.MaxInt {
		t.Fatalf("maximum count: %d, %v", n, err)
	}
	if n, err := w.Write([]byte{2}); err == nil || n != 0 || buf.Len() != 1 || w.Count() != math.MaxInt {
		t.Fatalf("overflow changed writer state: %d, %v", n, err)
	}
}

func TestStoredFieldEncodingBounds(t *testing.T) {
	for _, tc := range [][2]int{{-1, 0}, {0, -1}, {0, math.MaxInt}} {
		called := false
		encode := func(uint64) (int, error) { called = true; return 0, nil }
		if _, _, err := encodeStoredFieldValues(tc[0], [][]byte{{1}}, tc[1], encode, nil); err == nil || called {
			t.Fatalf("invalid input wrote metadata: %v, %v", tc, err)
		}
	}
}

type negativeLengthField struct{ *FakeField }

func (negativeLengthField) Length() int { return -1 }

type negativeLengthDocument struct{ FakeDocument }

func (negativeLengthDocument) EachField(visit segment.VisitField) {
	visit(negativeLengthField{NewFakeField("field", "term", false, false, false)})
}

func TestNewRejectsNegativeAnalysisValues(t *testing.T) {
	if _, _, err := New([]segment.Document{&negativeLengthDocument{}}, encodeNorm); err == nil {
		t.Fatal("accepted negative field length")
	}
	for _, term := range []*FakeTerm{
		{T: "term", F: -1},
		{T: "term", F: 1, L: []*FakeLocation{{P: -1}}},
		{T: "term", F: 1, L: []*FakeLocation{{S: -1}}},
		{T: "term", F: 1, L: []*FakeLocation{{E: -1}}},
	} {
		doc := FakeDocument{&FakeField{N: "field", T: []*FakeTerm{term}}}
		if _, _, err := New([]segment.Document{&doc}, encodeNorm); err == nil {
			t.Fatalf("accepted negative analysis value: %+v", term)
		}
	}
}

func TestSegmentReaderMetadataBounds(t *testing.T) {
	for _, data := range [][]byte{
		binary.AppendUvarint(nil, math.MaxUint64),
		binary.AppendUvarint([]byte{0}, math.MaxUint64),
	} {
		s := &Segment{data: segment.NewDataBytes(binary.BigEndian.AppendUint64(data, math.MaxUint64)),
			footer: &footer{fieldsIndexOffset: uint64(len(data))}}
		if err := s.loadFields(); err == nil {
			t.Fatal("accepted overflowing field address")
		}
	}
	s := &Segment{
		data: segment.NewDataBytes([]byte{0, 0x80}), fieldsMap: map[string]uint16{"field": 1},
		dictLocs: []uint64{1}, footer: &footer{numDocs: 1, docValueOffset: math.MaxUint64 - 1},
		fieldsInv: []string{"field"},
	}
	if _, err := s.dictionary("field"); err == nil {
		t.Fatal("accepted truncated dictionary length")
	}
	if err := s.loadDvReaders(); err == nil {
		t.Fatal("accepted overflowing doc-value index")
	}
	w := newCountHashWriter(failingChunkWriter{})
	if _, err := w.Write([]byte{0}); err != io.ErrClosedPipe || w.Count() != 0 {
		t.Fatalf("writer error propagation: %v", err)
	}
}
