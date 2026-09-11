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
	"encoding/binary"
	"fmt"
	"math"

	segment "github.com/vcaesar/bluge_segment_api"

	"github.com/vcaesar/ice/compress"
)

func readDataAt(data *segment.Data, offset, length uint64) ([]byte, error) {
	size := data.Len()
	if size < 0 || offset > uint64(size) || length > uint64(size)-offset {
		return nil, fmt.Errorf("segment data range out of bounds: offset %d, length %d", offset, length)
	}
	// #nosec G115 -- offset and offset+length are bounded by the nonnegative int-sized data length.
	return data.Read(int(offset), int(offset+length))
}

func readDataUvarint(data *segment.Data, offset *uint64) (uint64, error) {
	size := data.Len()
	if size < 0 || *offset >= uint64(size) {
		return 0, fmt.Errorf("segment varint offset out of bounds: %d", *offset)
	}
	buf, err := readDataAt(data, *offset, min(binary.MaxVarintLen64, uint64(size)-*offset))
	if err != nil {
		return 0, err
	}
	value, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("invalid segment varint")
	}
	*offset += uint64(n)
	return value, nil
}

func (s *Segment) initDecompressedStoredFieldChunks(n int) {
	s.m.Lock()
	s.decompressedStoredFieldChunks = make([]segmentCacheData, n)
	s.m.Unlock()
}

func (s *Segment) getDocStoredMetaAndUnCompressed(docNum uint64) (meta, data []byte, err error) {
	_, storedOffset, err := s.getDocStoredOffsetsOnly(docNum)
	if err != nil {
		return nil, nil, err
	}

	// document chunk coder
	var uncompressed []byte
	chunkI := docNum / uint64(defaultDocumentChunkSize)
	if chunkI >= uint64(len(s.decompressedStoredFieldChunks)) {
		return nil, nil, fmt.Errorf("stored-field chunk out of bounds")
	}
	storedFieldDecompressed := &s.decompressedStoredFieldChunks[chunkI]
	storedFieldDecompressed.m.Lock()
	if storedFieldDecompressed.data == nil {
		if chunkI+1 >= uint64(len(s.storedFieldChunkOffsets)) {
			storedFieldDecompressed.m.Unlock()
			return nil, nil, fmt.Errorf("stored-field chunk offsets out of bounds")
		}
		// we haven't already loaded and decompressed this chunk
		chunkOffsetStart := s.storedFieldChunkOffsets[chunkI]
		chunkOffsetEnd := s.storedFieldChunkOffsets[chunkI+1]
		compressed, err := readDataAt(s.data, chunkOffsetStart, chunkOffsetEnd-chunkOffsetStart)
		if err != nil {
			storedFieldDecompressed.m.Unlock()
			return nil, nil, err
		}

		// decompress it
		decoded, err := compress.Decompress(nil, compressed)
		if err != nil {
			storedFieldDecompressed.m.Unlock()
			return nil, nil, err
		}
		storedFieldDecompressed.data = decoded
	}
	// once initialized it wouldn't change, so we can unlock the mutex
	uncompressed = storedFieldDecompressed.data
	storedFieldDecompressed.m.Unlock()

	if storedOffset > uint64(len(uncompressed)) {
		return nil, nil, fmt.Errorf("invalid stored-field offset %d", storedOffset)
	}
	payload := uncompressed[storedOffset:]
	metaLen, read := binary.Uvarint(payload)
	if read <= 0 {
		return nil, nil, fmt.Errorf("invalid stored-field metadata length")
	}
	payload = payload[read:]
	dataLen, read := binary.Uvarint(payload)
	if read <= 0 {
		return nil, nil, fmt.Errorf("invalid stored-field data length")
	}
	payload = payload[read:]
	if metaLen > uint64(len(payload)) || dataLen > uint64(len(payload))-metaLen {
		return nil, nil, fmt.Errorf("stored-field lengths exceed chunk data")
	}
	meta = payload[:metaLen]
	data = payload[metaLen : metaLen+dataLen]
	return meta, data, nil
}

func (s *Segment) getDocStoredOffsetsOnly(docNum uint64) (indexOffset, storedOffset uint64, err error) {
	if docNum >= s.footer.numDocs || docNum > (math.MaxUint64-s.footer.storedIndexOffset)/fileAddrWidth {
		return 0, 0, fmt.Errorf("stored document number out of bounds: %d", docNum)
	}
	indexOffset = s.footer.storedIndexOffset + (fileAddrWidth * docNum)
	storedOffsetData, err := readDataAt(s.data, indexOffset, fileAddrWidth)
	if err != nil {
		return 0, 0, err
	}
	storedOffset = binary.BigEndian.Uint64(storedOffsetData)
	return indexOffset, storedOffset, nil
}
