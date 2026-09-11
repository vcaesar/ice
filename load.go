//  Copyright (c) 2020 The Bluge Authors.
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
	"encoding/binary"
	"fmt"

	"github.com/blevesearch/vellum"
	segment "github.com/vcaesar/bluge_segment_api"
)

// Open returns an impl of a segment
func Load(data *segment.Data) (segment.Segment, error) {
	return load(data)
}

func load(data *segment.Data) (*Segment, error) {
	footer, err := parseFooter(data)
	if err != nil {
		return nil, fmt.Errorf("error parsing footer: %w", err)
	}
	rv := &Segment{
		data:           data.Slice(0, data.Len()-footerLen),
		footer:         footer,
		fieldsMap:      make(map[string]uint16),
		fieldDvReaders: make(map[uint16]*docValueReader),
		fieldFSTs:      make(map[uint16]*vellum.FST),
		fieldDocs:      make(map[uint16]uint64),
		fieldFreqs:     make(map[uint16]uint64),
	}

	// FIXME temporarily map to existing footer fields
	// rv.memCRC = footer.crc
	// rv.chunkMode = footer.chunkMode
	// rv.numDocs = footer.numDocs
	// rv.storedIndexOffset = footer.storedIndexOffset
	// rv.fieldsIndexOffset = footer.fieldsIndexOffset
	// rv.docValueOffset = footer.docValueOffset

	err = rv.loadFields()
	if err != nil {
		return nil, err
	}

	err = rv.loadStoredFieldChunk()
	if err != nil {
		return nil, err
	}

	rv.initDecompressedStoredFieldChunks(len(rv.storedFieldChunkOffsets))

	err = rv.loadDvReaders()
	if err != nil {
		return nil, err
	}

	rv.updateSize()

	return rv, nil
}

const fileAddrWidth = 8

func (s *Segment) loadFields() error {
	// NOTE for now we assume the fields index immediately precedes
	// the footer, and if this changes, need to adjust accordingly (or
	// store explicit length), where s.mem was sliced from s.mm in Open().
	dataLen := s.data.Len()
	if dataLen < 0 || s.footer.fieldsIndexOffset > uint64(dataLen) {
		return fmt.Errorf("fields index offset out of range")
	}
	fieldsIndexEnd := uint64(dataLen)
	if (fieldsIndexEnd-s.footer.fieldsIndexOffset)%fileAddrWidth != 0 {
		return fmt.Errorf("truncated fields index address")
	}

	// iterate through fields index
	var fieldID uint64
	for s.footer.fieldsIndexOffset+(fileAddrWidth*fieldID) < fieldsIndexEnd {
		// #nosec G115 -- the aligned index range and loop bound keep both addresses within dataLen.
		addrData, err := s.data.Read(int(s.footer.fieldsIndexOffset+(fileAddrWidth*fieldID)),
			int(s.footer.fieldsIndexOffset+(fileAddrWidth*fieldID)+fileAddrWidth))
		if err != nil {
			return err
		}
		addr := binary.BigEndian.Uint64(addrData)
		dictLoc, err := readDataUvarint(s.data, &addr)
		if err != nil {
			return err
		}
		s.dictLocs = append(s.dictLocs, dictLoc)

		nameLen, err := readDataUvarint(s.data, &addr)
		if err != nil {
			return err
		}
		nameData, err := readDataAt(s.data, addr, nameLen)
		if err != nil {
			return err
		}
		addr += nameLen
		fieldDocVal, err := readDataUvarint(s.data, &addr)
		if err != nil {
			return err
		}
		fieldFreqVal, err := readDataUvarint(s.data, &addr)
		if err != nil {
			return err
		}

		name := string(nameData)
		s.fieldsInv = append(s.fieldsInv, name)
		s.fieldsMap[name] = uint16(fieldID + 1)
		s.fieldDocs[uint16(fieldID)] = fieldDocVal
		s.fieldFreqs[uint16(fieldID)] = fieldFreqVal

		fieldID++
	}
	return nil
}

// loadStoredFieldChunk load storedField chunk offsets
func (s *Segment) loadStoredFieldChunk() error {
	const trailerSize = 8 // Two uint32 fields: offset byte length and chunk count.
	if s.footer.storedIndexOffset < trailerSize {
		return fmt.Errorf("stored chunk trailer out of bounds")
	}
	chunkOffsetPos := s.footer.storedIndexOffset - trailerSize
	chunkData, err := readDataAt(s.data, chunkOffsetPos, trailerSize)
	if err != nil {
		return err
	}
	// read chunk num
	chunkNum := binary.BigEndian.Uint32(chunkData[sizeOfUint32:])
	// read chunk offsets length
	chunkOffsetsLen := uint64(binary.BigEndian.Uint32(chunkData))
	if chunkOffsetsLen > chunkOffsetPos || uint64(chunkNum) > chunkOffsetsLen {
		return fmt.Errorf("invalid stored chunk offset length or count")
	}
	// read chunk offsets
	chunkOffsetPos -= chunkOffsetsLen
	offsets, err := readDataAt(s.data, chunkOffsetPos, chunkOffsetsLen)
	if err != nil {
		return err
	}
	s.storedFieldChunkOffsets = make([]uint64, chunkNum)
	var previous uint64
	for i := range s.storedFieldChunkOffsets {
		offset, n := binary.Uvarint(offsets)
		if n <= 0 || offset < previous || offset > chunkOffsetPos {
			return fmt.Errorf("invalid stored chunk offset")
		}
		s.storedFieldChunkOffsets[i] = offset
		previous = offset
		offsets = offsets[n:]
	}

	return nil
}
