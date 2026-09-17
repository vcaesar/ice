//  Copyright (c) 2020 Couchbase, Inc.
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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/vellum"
	segment "github.com/vcaesar/bluge_segment_api"
)

const Version uint32 = 4

// versionTermDocValues is the last version whose doc values are all term
// encoded; it is still readable, and merges rewrite it as Version.
const versionTermDocValues uint32 = 3

const Type string = "ice"

type Segment struct {
	data   *segment.Data
	footer *footer

	fieldsMap  map[string]uint16 // fieldName -> fieldID+1
	fieldsInv  []string          // fieldID -> fieldName
	fieldDocs  map[uint16]uint64 // fieldID -> # docs with value in field
	fieldFreqs map[uint16]uint64 // fieldID -> # total tokens in field
	fieldStats []CollectionStats // fieldID -> CollectionStats, see initFieldStats

	storedFieldChunkOffsets []uint64 // stored field chunk offset

	dictLocs       []uint64
	fieldDvReaders map[uint16]*docValueReader // naive chunk cache per field
	fieldNumCols   map[uint16]*numericColumn  // typed numeric doc values per field
	fieldDvNames   []string                   // field names cached in fieldDvReaders
	size           int

	// state loaded dynamically
	m            sync.RWMutex
	fieldFSTs    map[uint16]*vellum.FST
	storedChunks storedChunkCache
}

func (s *Segment) WriteTo(w io.Writer, _ chan struct{}) (int64, error) {
	bw := bufio.NewWriter(w)

	n, err := s.data.WriteTo(w)
	if err != nil {
		return n, fmt.Errorf("error persisting segment: %w", err)
	}

	err = persistFooter(s.footer, bw)
	if err != nil {
		return n, fmt.Errorf("error persisting segment footer: %w", err)
	}

	err = bw.Flush()
	if err != nil {
		return n, err
	}

	return n + footerLen, nil
}

func (s *Segment) Type() string {
	return Type
}

// Version returns the file version in the file footer
func (s *Segment) Version() uint32 {
	return s.footer.version
}

func (s *Segment) Timestamp() (minimum, maximum int64) {
	if s.footer != nil {
		// The uint64 wire fields contain signed, two's-complement timestamps.
		return int64(s.footer.docTimeMin), int64(s.footer.docTimeMax) // #nosec G115 -- intentional signed timestamp bit reinterpretation.
	}
	return 0, 0
}

func (s *Segment) Size() int {
	// Chunk data is loaded lazily and is not included in the static size.
	return s.size + s.storedChunks.size()
}

// StoredChunkCacheStats returns hits, misses and resident entries of the
// decompressed stored-field chunk cache.
func (s *Segment) StoredChunkCacheStats() (hits, misses uint64, entries int) {
	return s.storedChunks.stats()
}

func (s *Segment) updateSize() {
	sizeInBytes := reflectStaticSizeSegment +
		s.data.Size()

	// fieldsMap
	for k := range s.fieldsMap {
		sizeInBytes += (len(k) + sizeOfString) + sizeOfUint16
	}

	// fieldsInv, dictLocs
	for _, entry := range s.fieldsInv {
		sizeInBytes += len(entry) + sizeOfString
	}
	sizeInBytes += len(s.dictLocs) * sizeOfUint64

	// fieldDvReaders
	for _, v := range s.fieldDvReaders {
		sizeInBytes += sizeOfUint16 + sizeOfPtr
		if v != nil {
			sizeInBytes += v.size()
		}
	}
	for _, v := range s.fieldNumCols {
		sizeInBytes += sizeOfUint16 + sizeOfPtr + v.size()
	}

	sizeInBytes += cap(s.storedFieldChunkOffsets) * sizeOfUint64

	s.size = sizeInBytes
}

// DictionaryReader returns the term dictionary for the specified field
func (s *Segment) Dictionary(field string) (segment.Dictionary, error) {
	dict, err := s.dictionary(field)
	if err == nil && dict == nil {
		return emptyDictionary, nil
	}
	return dict, err
}

func (s *Segment) dictionary(field string) (rv *Dictionary, err error) {
	fieldIDPlus1 := s.fieldsMap[field]
	if fieldIDPlus1 > 0 {
		rv = &Dictionary{
			sb:      s,
			field:   field,
			fieldID: fieldIDPlus1 - 1,
		}

		dictStart := s.dictLocs[rv.fieldID]
		if dictStart > 0 {
			var ok bool
			s.m.Lock()
			if rv.fst, ok = s.fieldFSTs[rv.fieldID]; !ok {
				// read the length of the vellum data
				var vellumLen uint64
				vellumLen, err = readDataUvarint(s.data, &dictStart)
				if err != nil {
					s.m.Unlock()
					return nil, err
				}
				var fstBytes []byte
				fstBytes, err = readDataAt(s.data, dictStart, vellumLen)
				if err != nil {
					s.m.Unlock()
					return nil, err
				}
				rv.fst, err = vellum.Load(fstBytes)
				if err != nil {
					s.m.Unlock()
					return nil, fmt.Errorf("dictionary field %s vellum err: %v", field, err)
				}

				s.fieldFSTs[rv.fieldID] = rv.fst
			}

			s.m.Unlock()
			rv.fstReader, err = rv.fst.Reader()
			if err != nil {
				return nil, fmt.Errorf("dictionary field %s vellum reader err: %v", field, err)
			}
		}
	}

	return rv, nil
}

// visitDocumentCtx holds data structures that are reusable across
// multiple VisitStoredFields() calls to avoid memory allocations
type visitDocumentCtx struct {
	// buf    []byte
	reader bytes.Reader
}

var visitDocumentCtxPool = sync.Pool{
	New: func() interface{} {
		reuse := &visitDocumentCtx{}
		return reuse
	},
}

// VisitStoredFields invokes the DocFieldValueVistor for each stored field
// for the specified doc number
func (s *Segment) VisitStoredFields(num uint64, visitor segment.StoredFieldVisitor) error {
	vdc := visitDocumentCtxPool.Get().(*visitDocumentCtx)
	defer visitDocumentCtxPool.Put(vdc)
	return s.visitDocument(vdc, num, visitor)
}

func (s *Segment) visitDocument(vdc *visitDocumentCtx, num uint64,
	visitor segment.StoredFieldVisitor) error {
	defer vdc.reader.Reset(nil)
	// first make sure this is a valid number in this segment
	if num < s.footer.numDocs {
		meta, uncompressed, err := s.getDocStoredMetaAndUnCompressed(num)
		if err != nil {
			return err
		}

		vdc.reader.Reset(meta)

		var keepGoing = true
		for keepGoing {
			field, err := binary.ReadUvarint(&vdc.reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			offset, err := binary.ReadUvarint(&vdc.reader)
			if err != nil {
				return err
			}
			l, err := binary.ReadUvarint(&vdc.reader)
			if err != nil {
				return err
			}

			if field >= uint64(len(s.fieldsInv)) {
				return fmt.Errorf("invalid stored-field ID %d", field)
			}
			if offset > uint64(len(uncompressed)) || l > uint64(len(uncompressed))-offset {
				return fmt.Errorf("stored-field range exceeds document data")
			}

			value := uncompressed[offset : offset+l]
			keepGoing = visitor(s.fieldsInv[field], value)
		}
	}
	return nil
}

// Count returns the number of documents in this segment.
func (s *Segment) Count() uint64 {
	return s.footer.numDocs
}

func (s *Segment) DocsMatchingTerms(terms []segment.Term) (*roaring.Bitmap, error) {
	rv := roaring.New()

	if len(s.fieldsMap) > 0 {
		// we expect the common case to be the same field for all
		// so we optimize for that, but allow it to work if that
		// isn't the case
		var err error
		var lastField string
		var dict *Dictionary
		for i, term := range terms {
			thisField := term.Field()
			if thisField != lastField {
				dict, err = s.dictionary(term.Field())
				if err != nil {
					return nil, err
				}
				lastField = thisField
			}
			term := terms[i]
			postingsList := emptyPostingsList
			postingsList, err = dict.postingsList(term.Term(), nil, postingsList)
			if err != nil {
				return nil, err
			}
			postingsList.OrInto(rv)
		}
	}
	return rv, nil
}

// Fields returns the field names used in this segment
func (s *Segment) Fields() []string {
	return s.fieldsInv
}

// CRC returns the CRC value stored in the file footer
func (s *Segment) CRC() uint32 {
	return s.footer.crc
}

// ChunkFactor returns the chunk factor in the file footer
func (s *Segment) ChunkMode() uint32 {
	return s.footer.chunkMode
}

// FieldsIndexOffset returns the fields index offset in the file footer
func (s *Segment) FieldsIndexOffset() uint64 {
	return s.footer.fieldsIndexOffset
}

// StoredIndexOffset returns the stored value index offset in the file footer
func (s *Segment) StoredIndexOffset() uint64 {
	return s.footer.storedIndexOffset
}

// DocValueOffset returns the docValue offset in the file footer
func (s *Segment) DocValueOffset() uint64 {
	return s.footer.docValueOffset
}

// NumDocs returns the number of documents in the file footer
func (s *Segment) NumDocs() uint64 {
	return s.footer.numDocs
}

// DocTimeMin returns the minimum timestamp of documents in the file footer
func (s *Segment) DocTimeMin() uint64 {
	return s.footer.docTimeMin
}

// DocTimeMax returns the maximum timestamp of documents in the file footer
func (s *Segment) DocTimeMax() uint64 {
	return s.footer.docTimeMax
}

func (s *Segment) loadDvReaders() error {
	if s.footer.docValueOffset == fieldNotUninverted || s.footer.numDocs == 0 {
		return nil
	}

	pos := s.footer.docValueOffset
	for fieldID, field := range s.fieldsInv {
		kind := dvKindTerms
		var err error
		if s.footer.version >= Version {
			if kind, err = readDataUvarint(s.data, &pos); err != nil {
				return err
			}
		}
		fieldLocStart, err := readDataUvarint(s.data, &pos)
		if err != nil {
			return err
		}
		fieldLocEnd, err := readDataUvarint(s.data, &pos)
		if err != nil {
			return err
		}

		switch kind {
		case dvKindTerms:
			fieldDvReader, err := s.loadFieldDocValueReader(field, fieldLocStart, fieldLocEnd)
			if err != nil {
				return err
			}
			if fieldDvReader != nil {
				// #nosec G115 -- fieldsInv is capped at MaxUint16 entries.
				s.fieldDvReaders[uint16(fieldID)] = fieldDvReader
				s.fieldDvNames = append(s.fieldDvNames, field)
			}
		case dvKindNumeric:
			col, err := s.loadNumericColumn(field, fieldLocStart, fieldLocEnd)
			if err != nil {
				return err
			}
			// #nosec G115 -- fieldsInv is capped at MaxUint16 entries.
			s.fieldNumCols[uint16(fieldID)] = col
			s.fieldDvNames = append(s.fieldDvNames, field)
		default:
			return fmt.Errorf("unknown doc value kind %d for field %s", kind, field)
		}
	}

	return nil
}
