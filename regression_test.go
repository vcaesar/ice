package ice

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/vellum"
	"github.com/blugelabs/ice/compress"
	segment "github.com/vcaesar/bluge_segment_api"
)

func useCompression(t *testing.T, algorithm int) {
	t.Helper()
	previous := compress.Algorithm
	compress.Algorithm = algorithm
	t.Cleanup(func() { compress.Algorithm = previous })
}

func TestChunkedDocumentCoderReset(t *testing.T) {
	for _, chunkSize := range []uint64{1, 2} {
		var buf bytes.Buffer
		coder := newChunkedDocumentCoder(chunkSize, &buf)
		var expected []byte
		for cycle := 0; cycle < 3; cycle++ {
			if _, err := coder.Add(0, []byte{0}, []byte("document")); err != nil {
				t.Fatal(err)
			}
			if err := coder.Write(); err != nil {
				t.Fatal(err)
			}
			if cycle == 0 {
				expected = append([]byte(nil), buf.Bytes()...)
			} else if !bytes.Equal(expected, buf.Bytes()) {
				t.Errorf("chunk size %d: reset output differs from fresh coder", chunkSize)
			}
			coder.Reset()
			buf.Reset()
		}
	}
}

func TestChunkedIntCoderCompressionError(t *testing.T) {
	useCompression(t, -1)
	coder := newChunkedIntCoder(1, 1)
	if err := coder.Add(0, 3); err != nil {
		t.Fatal(err)
	}
	if err := coder.Add(1, 7); err == nil {
		t.Fatal("expected compression error when changing chunks")
	}
	if coder.currChunk != 0 || coder.chunkBuf.Len() == 0 {
		t.Fatal("failed compression discarded the pending chunk")
	}
}

func TestTermFinalizationCompressionError(t *testing.T) {
	useCompression(t, -1)
	for _, merging := range []bool{false, true} {
		var output, dictionary bytes.Buffer
		writer := newCountHashWriter(&output)
		builder, err := vellum.New(&dictionary, nil)
		if err != nil {
			t.Fatal(err)
		}
		tf, loc := newChunkedIntCoder(1, 0), newChunkedIntCoder(1, 0)
		postings := roaring.BitmapOf(0)
		buf := make([]byte, binary.MaxVarintLen64)
		if merging {
			var lastDoc, lastFreq, lastNorm uint64
			err = finishTerm(writer, postings, tf, loc, builder, buf, []byte("term"), &lastDoc, &lastFreq, &lastNorm)
		} else {
			s := &interim{
				results:   []segment.Document{&FakeDocument{}},
				chunkMode: defaultChunkMode, w: writer, builder: builder,
				Postings:  []*roaring.Bitmap{postings},
				FreqNorms: [][]interimFreqNorm{{{freq: 1}}},
				Locs:      [][]interimLoc{nil},
			}
			err = s.writeDictsTermField(make([][]byte, 1), map[string]uint64{"term": 1}, "term", tf, loc, buf)
		}
		if err == nil {
			t.Errorf("merging=%v: expected final compression error", merging)
		}
		if output.Len() != 0 {
			t.Errorf("merging=%v: wrote postings after failed compression", merging)
		}
		if err := builder.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStoredFieldsDecompressionErrorUnlocks(t *testing.T) {
	useCompression(t, compress.S2)
	data := make([]byte, 9)
	data[0] = 0xff
	s := &Segment{
		data:                    segment.NewDataBytes(data),
		footer:                  &footer{storedIndexOffset: 1, numDocs: 1},
		storedFieldChunkOffsets: []uint64{0, 1},
	}
	s.initDecompressedStoredFieldChunks(1)
	if _, _, err := s.getDocStoredMetaAndUnCompressed(0); err == nil {
		t.Fatal("expected corrupt compression error")
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := s.getDocStoredMetaAndUnCompressed(0)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected repeated corruption error")
		}
	case <-time.After(time.Second):
		t.Fatal("stored-field cache mutex remained locked after error")
	}
}

func TestStoredFieldsMalformedPayload(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		offset  uint64
	}{
		{"offset past end", []byte{0, 0}, 3},
		{"missing metadata length", []byte{}, 0},
		{"truncated metadata length", []byte{0x80}, 0},
		{"overflowing metadata length", bytes.Repeat([]byte{0xff}, 11), 0},
		{"missing data length", []byte{0}, 0},
		{"truncated data length", []byte{0, 0x80}, 0},
		{"metadata past end", []byte{4, 0}, 0},
		{"data past end", []byte{0, 4}, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := make([]byte, 8)
			binary.BigEndian.PutUint64(index, test.offset)
			s := &Segment{data: segment.NewDataBytes(index), footer: &footer{numDocs: 1}}
			s.initDecompressedStoredFieldChunks(1)
			s.decompressedStoredFieldChunks[0].data = test.payload
			if _, _, err := s.getDocStoredMetaAndUnCompressed(0); err == nil {
				t.Fatal("expected malformed stored-field error")
			}
		})
	}
}
