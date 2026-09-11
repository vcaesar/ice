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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	segment "github.com/vcaesar/bluge_segment_api"
	"github.com/vcaesar/ice/compress"
)

type docNumTermsVisitor func(docNum uint64, terms []byte) error

type docVisitState struct {
	dvrs    map[uint16]*docValueReader
	segment *Segment
}

type docValueReader struct {
	field          string
	curChunkNum    uint64
	chunkOffsets   []uint64
	dvDataLoc      uint64
	curChunkHeader []metaData
	curChunkData   []byte // compressed data cache
	uncompressed   []byte // temp buf for decompression
}

func (di *docValueReader) size() int {
	return reflectStaticSizedocValueReader + sizeOfPtr +
		len(di.field) +
		len(di.chunkOffsets)*sizeOfUint64 +
		len(di.curChunkHeader)*reflectStaticSizeMetaData +
		len(di.curChunkData)
}

func (di *docValueReader) cloneInto(rv *docValueReader) *docValueReader {
	if rv == nil {
		rv = &docValueReader{}
	}

	rv.field = di.field
	rv.curChunkNum = math.MaxInt64
	rv.chunkOffsets = di.chunkOffsets // immutable, so it's sharable
	rv.dvDataLoc = di.dvDataLoc
	rv.curChunkHeader = rv.curChunkHeader[:0]
	rv.curChunkData = nil
	rv.uncompressed = rv.uncompressed[:0]

	return rv
}

func (di *docValueReader) curChunkNumber() uint64 {
	return di.curChunkNum
}

const fieldDvStartWidth = 8
const fieldDvEndWidth = 8
const fieldDvStartEndWidth = fieldDvStartWidth + fieldDvEndWidth

func (s *Segment) loadFieldDocValueReader(field string,
	fieldDvLocStart, fieldDvLocEnd uint64) (*docValueReader, error) {
	// get the docValue offset for the given fields
	if fieldDvLocStart == fieldNotUninverted {
		// no docValues found, nothing to do
		return nil, nil
	}

	if fieldDvLocStart > fieldDvLocEnd || fieldDvLocEnd > uint64(s.data.Len()) ||
		fieldDvLocEnd-fieldDvLocStart <= fieldDvStartEndWidth {
		return nil, fmt.Errorf("invalid doc-value range: %d-%d", fieldDvLocStart, fieldDvLocEnd)
	}
	// #nosec G115 -- both offsets are bounded by data.Len(), an int.
	start, end := int(fieldDvLocStart), int(fieldDvLocEnd)
	tail, err := s.data.Read(end-fieldDvStartEndWidth, end)
	if err != nil {
		return nil, err
	}
	chunkOffsetsLen := binary.BigEndian.Uint64(tail[:fieldDvStartWidth])
	numChunks := binary.BigEndian.Uint64(tail[fieldDvStartWidth:])
	if chunkOffsetsLen > uint64(end-start-fieldDvStartEndWidth) || numChunks > chunkOffsetsLen {
		return nil, fmt.Errorf("invalid doc-value chunk offset length or count")
	}
	// #nosec G115 -- chunkOffsetsLen is bounded by the int-sized field range above.
	chunkOffsetsPosition := end - fieldDvStartEndWidth - int(chunkOffsetsLen)
	locData, err := s.data.Read(chunkOffsetsPosition, end-fieldDvStartEndWidth)
	if err != nil {
		return nil, err
	}
	fdvIter := &docValueReader{
		curChunkNum: math.MaxInt64,
		field:       field,
		// #nosec G115 -- numChunks <= chunkOffsetsLen <= the int-sized field range.
		chunkOffsets: make([]uint64, int(numChunks)),
	}
	var previous uint64
	for i := range fdvIter.chunkOffsets {
		loc, read := binary.Uvarint(locData)
		if read <= 0 || loc < previous || loc > uint64(chunkOffsetsPosition-start) {
			return nil, fmt.Errorf("corrupted chunk offset during segment load")
		}
		fdvIter.chunkOffsets[i] = loc
		previous = loc
		locData = locData[read:]
	}

	// set the data offset
	fdvIter.dvDataLoc = fieldDvLocStart

	return fdvIter, nil
}

func (di *docValueReader) loadDvChunk(chunkNumber uint64, s *Segment) error {
	if chunkNumber >= uint64(len(di.chunkOffsets)) {
		return fmt.Errorf("doc-value chunk number out of range: %d", chunkNumber)
	}
	// #nosec G115 -- chunkNumber is less than the int-sized offsets slice length.
	start, end := readChunkBoundary(int(chunkNumber), di.chunkOffsets)
	if start > end || di.dvDataLoc > uint64(s.data.Len()) || end > uint64(s.data.Len())-di.dvDataLoc {
		return fmt.Errorf("doc-value chunk offsets out of range")
	}
	if start >= end {
		di.curChunkHeader = di.curChunkHeader[:0]
		di.curChunkData = nil
		di.curChunkNum = chunkNumber
		di.uncompressed = di.uncompressed[:0]
		return nil
	}

	// #nosec G115 -- the sums are bounded by data.Len() above, without uint64 overflow.
	data, err := s.data.Read(int(di.dvDataLoc+start), int(di.dvDataLoc+end))
	if err != nil {
		return err
	}
	numDocs, read := binary.Uvarint(data)
	if read <= 0 {
		return fmt.Errorf("failed to read the chunk")
	}
	data = data[read:]
	if numDocs > uint64(len(data)/2) { // Each metadata entry needs at least two varint bytes.
		return fmt.Errorf("invalid doc-value document count")
	}
	// #nosec G115 -- numDocs <= len(data)/2, so it fits int.
	count := int(numDocs)
	if cap(di.curChunkHeader) < count {
		di.curChunkHeader = make([]metaData, count)
	} else {
		di.curChunkHeader = di.curChunkHeader[:count]
	}

	var docNum, dvOffset uint64
	for i := range di.curChunkHeader {
		delta, n := binary.Uvarint(data)
		if n <= 0 || delta > math.MaxUint64-docNum {
			return fmt.Errorf("invalid doc-value document delta")
		}
		docNum += delta
		data = data[n:]
		delta, n = binary.Uvarint(data)
		if n <= 0 || delta > math.MaxUint64-dvOffset {
			return fmt.Errorf("invalid doc-value offset delta")
		}
		dvOffset += delta
		data = data[n:]
		di.curChunkHeader[i] = metaData{DocNum: docNum, DocDvOffset: dvOffset}
	}

	di.curChunkData = data
	di.curChunkNum = chunkNumber
	di.uncompressed = di.uncompressed[:0]
	return nil
}

func (di *docValueReader) iterateAllDocValues(s *Segment, visitor docNumTermsVisitor) error {
	for i := 0; i < len(di.chunkOffsets); i++ {
		err := di.loadDvChunk(uint64(i), s)
		if err != nil {
			return err
		}
		if di.curChunkData == nil || len(di.curChunkHeader) == 0 {
			continue
		}

		// uncompress the already loaded data
		uncompressed, err := compress.Decompress(di.uncompressed[:cap(di.uncompressed)], di.curChunkData)
		if err != nil {
			return err
		}
		di.uncompressed = uncompressed

		start := uint64(0)
		for _, entry := range di.curChunkHeader {
			if entry.DocDvOffset < start || entry.DocDvOffset > uint64(len(uncompressed)) {
				return fmt.Errorf("doc-value offset exceeds uncompressed data")
			}
			err = visitor(entry.DocNum, uncompressed[start:entry.DocDvOffset])
			if err != nil {
				return err
			}

			start = entry.DocDvOffset
		}
	}

	return nil
}

func (di *docValueReader) visitDocValues(docNum uint64,
	visitor segment.DocumentValueVisitor) error {
	// binary search the term locations for the docNum
	start, end := di.getDocValueLocs(docNum)
	if start == math.MaxUint64 || end == math.MaxUint64 || start == end {
		return nil
	}

	var uncompressed []byte
	var err error
	// use the uncompressed copy if available
	if len(di.uncompressed) > 0 {
		uncompressed = di.uncompressed
	} else {
		// uncompress the already loaded data
		uncompressed, err = compress.Decompress(di.uncompressed[:cap(di.uncompressed)], di.curChunkData)
		if err != nil {
			return err
		}
		di.uncompressed = uncompressed
	}

	// pick the terms for the given docNum
	if start > end || end > uint64(len(uncompressed)) {
		return fmt.Errorf("doc-value offsets exceed uncompressed data")
	}
	uncompressed = uncompressed[start:end]
	for {
		i := bytes.Index(uncompressed, termSeparatorSplitSlice)
		if i < 0 {
			break
		}

		visitor(di.field, uncompressed[0:i])
		uncompressed = uncompressed[i+1:]
	}

	return nil
}

func (di *docValueReader) getDocValueLocs(docNum uint64) (start, end uint64) {
	i := sort.Search(len(di.curChunkHeader), func(i int) bool {
		return di.curChunkHeader[i].DocNum >= docNum
	})
	if i < len(di.curChunkHeader) && di.curChunkHeader[i].DocNum == docNum {
		return readDocValueBoundary(i, di.curChunkHeader)
	}
	return math.MaxUint64, math.MaxUint64
}

// VisitDocumentFieldTerms is an implementation of the
// DocumentFieldTermVisitable interface
func (s *Segment) visitDocumentFieldTerms(localDocNum uint64, fields []string,
	visitor segment.DocumentValueVisitor, dvs *docVisitState) (
	*docVisitState, error) {
	if dvs == nil {
		dvs = &docVisitState{segment: s}
	} else if dvs.segment != s {
		dvs.segment = s
		dvs.dvrs = nil
	}

	if dvs.dvrs == nil {
		var ok bool
		var fieldIDPlus1 uint16
		dvs.dvrs = make(map[uint16]*docValueReader, len(fields))
		for _, field := range fields {
			if fieldIDPlus1, ok = s.fieldsMap[field]; !ok {
				continue
			}
			fieldID := fieldIDPlus1 - 1
			if dvIter, exists := s.fieldDvReaders[fieldID]; exists &&
				dvIter != nil {
				dvs.dvrs[fieldID] = dvIter.cloneInto(dvs.dvrs[fieldID])
			}
		}
	}

	// find the chunkNumber where the docValues are stored
	// NOTE: doc values continue to use legacy chunk mode
	chunkFactor, err := getChunkSize(legacyChunkMode, 0, 0)
	if err != nil {
		return nil, err
	}
	docInChunk := localDocNum / chunkFactor
	var dvr *docValueReader
	for _, field := range fields {
		var ok bool
		var fieldIDPlus1 uint16
		if fieldIDPlus1, ok = s.fieldsMap[field]; !ok {
			continue
		}
		fieldID := fieldIDPlus1 - 1
		if dvr, ok = dvs.dvrs[fieldID]; ok && dvr != nil {
			// check if the chunk is already loaded
			if docInChunk != dvr.curChunkNumber() {
				err := dvr.loadDvChunk(docInChunk, s)
				if err != nil {
					return dvs, err
				}
			}

			if err := dvr.visitDocValues(localDocNum, visitor); err != nil {
				return dvs, err
			}
		}
	}
	return dvs, nil
}

type DocumentValueReader struct {
	fields  []string
	state   *docVisitState
	segment *Segment
}

func (d *DocumentValueReader) VisitDocumentValues(number uint64, visitor segment.DocumentValueVisitor) error {
	state, err := d.segment.visitDocumentFieldTerms(number, d.fields, visitor, d.state)
	if err != nil {
		return err
	}
	d.state = state
	return nil
}

func (s *Segment) DocumentValueReader(fields []string) (segment.DocumentValueReader, error) {
	return &DocumentValueReader{
		fields:  fields,
		segment: s,
	}, nil
}
