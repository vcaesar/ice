package ice

import (
	"bytes"
	"testing"

	"github.com/blugelabs/ice/compress"
	segment "github.com/vcaesar/bluge_segment_api"
)

func TestDocumentValueStateReuse(t *testing.T) {
	s := &Segment{}
	state, err := s.visitDocumentFieldTerms(0, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if state.segment != s {
		t.Fatal("new state is not bound to segment")
	}
	marker := &docValueReader{}
	state.dvrs[42] = marker
	next, err := s.visitDocumentFieldTerms(0, nil, nil, state)
	if err != nil {
		t.Fatal(err)
	}
	if next != state || next.dvrs[42] != marker {
		t.Fatal("reader state rebuilt on reuse")
	}
	other := &Segment{}
	next, err = other.visitDocumentFieldTerms(0, nil, nil, state)
	if err != nil {
		t.Fatal(err)
	}
	if next.segment != other || next.dvrs[42] != nil {
		t.Fatal("reader state not reset across segments")
	}
}

func TestCopyStoredDocsMalformedPayload(t *testing.T) {
	useCompression(t, compress.S2)
	for _, payload := range [][]byte{
		{0x80}, bytes.Repeat([]byte{0xff}, 11), {0}, {0, 0x80},
		append([]byte{0}, bytes.Repeat([]byte{0xff}, 11)...), {4, 0}, {0, 4}, {0, 0, 0, 0},
	} {
		encoded, err := compress.Compress(nil, payload)
		if err != nil {
			t.Fatal(err)
		}
		s := &Segment{data: segment.NewDataBytes(encoded), footer: &footer{numDocs: 1}, storedFieldChunkOffsets: []uint64{0, uint64(len(encoded))}}
		var output bytes.Buffer
		coder := newChunkedDocumentCoder(128, &output)
		if err := s.copyStoredDocs(0, make([]uint64, 1), coder); err == nil {
			t.Errorf("payload %x: expected error", payload)
		}
	}
}

func BenchmarkDocumentValueStateReuse(b *testing.B) {
	s := &Segment{}
	state, err := s.visitDocumentFieldTerms(0, nil, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state, err = s.visitDocumentFieldTerms(0, nil, nil, state)
		if err != nil {
			b.Fatal(err)
		}
	}
}
