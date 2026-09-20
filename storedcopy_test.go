// Copyright 2026 The Bluge Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"

	"github.com/vcaesar/ice/compress"
)

func storedCopyFixture(tb testing.TB, count int) *Segment {
	tb.Helper()
	var buf bytes.Buffer
	coder := newChunkedDocumentCoder(uint64(defaultDocumentChunkSize), &buf)
	offsets := make([]uint64, count)
	for i := range offsets {
		value := []byte(fmt.Sprintf("document-%04d-%s", i, bytes.Repeat([]byte("stored value "), 32)))
		meta := optimizationVarints(0, 0, uint64(len(value)))
		offsets[i] = coder.Size()
		if _, err := coder.Add(uint64(i), meta, value); err != nil {
			tb.Fatal(err)
		}
	}
	if err := coder.Write(); err != nil {
		tb.Fatal(err)
	}
	index := uint64(buf.Len()) // #nosec G115 -- bytes.Buffer.Len is nonnegative.
	for _, offset := range offsets {
		if err := binary.Write(&buf, binary.BigEndian, offset); err != nil {
			tb.Fatal(err)
		}
	}
	return &Segment{
		data:                    segment.NewDataBytes(buf.Bytes()),
		footer:                  &footer{numDocs: uint64(len(offsets)), storedIndexOffset: index},
		storedFieldChunkOffsets: coder.Offsets(),
		fieldsInv:               []string{"_id"}, fieldsMap: map[string]uint16{"_id": 1},
	}
}

func TestStoredCopyMergeEquivalence(t *testing.T) {
	for _, algorithm := range []int{compress.SNAPPY, compress.S2, compress.ZSTD} {
		t.Run(fmt.Sprint(algorithm), func(t *testing.T) {
			useCompression(t, algorithm)
			for _, tc := range []struct {
				name   string
				counts []int
				drop   *roaring.Bitmap
			}{
				{"aligned", []int{128, 256}, nil},
				{"aligned-tail", []int{128, 129, 127, 128}, nil},
				{"unaligned", []int{1, 256}, nil},
				{"partial", []int{127, 1}, nil},
				{"deletion", []int{256, 128}, roaring.BitmapOf(0, 127, 128)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var segments []*Segment
					drops := make([]*roaring.Bitmap, len(tc.counts))
					drops[0] = tc.drop
					for _, count := range tc.counts {
						segments = append(segments, storedCopyFixture(t, count))
					}
					n, err := computeNewDocCount(segments, drops)
					if err != nil {
						t.Fatal(err)
					}
					var fast, fallback bytes.Buffer
					index, mapping, err := mergeStoredAndRemap(segments, drops, segments[0].fieldsMap,
						segments[0].fieldsInv, true, n, newCountHashWriter(&fast), nil)
					if err != nil {
						t.Fatal(err)
					}
					wantIndex, wantMapping, err := mergeStoredAndRemap(segments, drops, segments[0].fieldsMap,
						segments[0].fieldsInv, false, n, newCountHashWriter(&fallback), nil)
					if err != nil {
						t.Fatal(err)
					}
					if index != wantIndex || !reflect.DeepEqual(mapping, wantMapping) || !bytes.Equal(fast.Bytes(), fallback.Bytes()) {
						t.Fatal("raw copy differs from document-by-document merge")
					}
					merged := &Segment{data: segment.NewDataBytes(fast.Bytes()), footer: &footer{numDocs: n, storedIndexOffset: index}}
					if err := merged.loadStoredFieldChunk(); err != nil {
						t.Fatal(err)
					}
					for si, source := range segments {
						for doc, dest := range mapping[si] {
							if dest == docDropped {
								continue
							}
							meta, data, err := merged.getDocStoredMetaAndUnCompressed(dest)
							if err != nil {
								t.Fatal(err)
							}
							wantMeta, wantData, err := source.getDocStoredMetaAndUnCompressed(uint64(doc))
							if err != nil {
								t.Fatal(err)
							}
							if !bytes.Equal(meta, wantMeta) || !bytes.Equal(data, wantData) {
								t.Fatalf("document %d/%d differs", si, doc)
							}
						}
					}
				})
			}
		})
	}
}

type storedCopyWriter struct{ err error }

func (w storedCopyWriter) Write(p []byte) (int, error) { return len(p) - 1, w.err }

func TestStoredCopyErrors(t *testing.T) {
	useCompression(t, compress.S2)
	for _, tc := range []struct {
		name     string
		mutate   func(*Segment)
		capacity int
	}{
		{"capacity", func(*Segment) {}, 127},
		{"chunk-range", func(s *Segment) {
			// #nosec G115 -- fixture data has a small positive length.
			s.storedFieldChunkOffsets[1] = uint64(s.data.Len()) + 1
		}, 128},
		{"index-range", func(s *Segment) {
			// #nosec G115 -- fixture data has a small positive length.
			s.footer.storedIndexOffset = uint64(s.data.Len())
		}, 128},
		{"offset-order", func(s *Segment) {
			data, err := readDataAt(s.data, s.footer.storedIndexOffset+8, 8)
			if err != nil {
				t.Fatal(err)
			}
			clear(data)
		}, 128},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := storedCopyFixture(t, 128)
			tc.mutate(s)
			coder := newChunkedDocumentCoder(128, io.Discard)
			if err := s.copyStoredDocs(0, make([]uint64, tc.capacity), coder); err == nil {
				t.Fatal("expected error")
			}
			if coder.n != 0 || coder.bytes != 0 {
				t.Fatal("coder advanced after error")
			}
		})
	}
	sentinel := errors.New("write failure")
	for _, want := range []error{sentinel, io.ErrShortWrite} {
		writer := storedCopyWriter{}
		if want == sentinel {
			writer.err = sentinel
		}
		coder := newChunkedDocumentCoder(128, writer)
		if err := storedCopyFixture(t, 128).copyStoredDocs(0, make([]uint64, 128), coder); !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
		if coder.n != 0 || coder.bytes != 0 || len(coder.offsets) != 1 {
			t.Fatal("coder advanced after failed write")
		}
	}
	coder := newChunkedDocumentCoder(128, io.Discard)
	if _, err := coder.Add(0, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := coder.copyChunk([]byte("x")); err == nil {
		t.Fatal("accepted unaligned chunk")
	}
}

func TestStoredCopyPreservesCompressedBytes(t *testing.T) {
	useCompression(t, compress.S2)
	s := storedCopyFixture(t, 256)
	var out bytes.Buffer
	coder := newChunkedDocumentCoder(128, &out)
	if err := s.copyStoredDocs(0, make([]uint64, 256), coder); err != nil {
		t.Fatal(err)
	}
	if coder.n != 256 || coder.Size() != 0 || coder.compressed != nil {
		t.Fatal("full chunks were re-encoded")
	}
	original, err := readDataAt(s.data, 0, s.storedFieldChunkOffsets[2])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), original) {
		t.Fatal("compressed bytes changed")
	}
	coder.Reset()
	if coder.n != 0 || coder.bytes != 0 || !reflect.DeepEqual(coder.Offsets(), []uint64{0}) {
		t.Fatal("reset after copy failed")
	}
}

func BenchmarkStoredCopy(b *testing.B) {
	s := storedCopyFixture(b, 1024)
	for _, prefix := range []uint64{0, 1} {
		name := "aligned"
		if prefix != 0 {
			name = "unaligned-fallback"
		}
		b.Run(name, func(b *testing.B) {
			offsets := make([]uint64, 1024+prefix)
			coder := newChunkedDocumentCoder(128, io.Discard)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				coder.Reset()
				if prefix != 0 {
					if _, err := coder.Add(0, nil, nil); err != nil {
						b.Fatal(err)
					}
				}
				if err := s.copyStoredDocs(prefix, offsets, coder); err != nil {
					b.Fatal(err)
				}
				if err := coder.Write(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
