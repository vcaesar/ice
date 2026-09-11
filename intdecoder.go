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

	segment "github.com/vcaesar/bluge_segment_api"

	"github.com/vcaesar/ice/compress"
)

type chunkedIntDecoder struct {
	startOffset     uint64
	dataStartOffset uint64
	chunkOffsets    []uint64
	curChunkBytes   []byte
	uncompressed    []byte // temp buf for decompression
	data            *segment.Data
	r               *memUvarintReader
}

func newChunkedIntDecoder(data *segment.Data, offset uint64, rv *chunkedIntDecoder) (*chunkedIntDecoder, error) {
	if rv == nil {
		rv = &chunkedIntDecoder{startOffset: offset, data: data}
	} else {
		rv.startOffset = offset
		rv.data = data
	}
	if offset == termNotEncoded {
		rv.chunkOffsets = rv.chunkOffsets[:0]
		rv.dataStartOffset = 0
		return rv, nil
	}
	dataLen := data.Len()
	if dataLen < 0 || offset >= uint64(dataLen) {
		return nil, fmt.Errorf("integer chunk header offset out of range: %d", offset)
	}
	// #nosec G115 -- offset is less than the nonnegative int-sized data length.
	pos := int(offset)
	numChunks, err := readChunkHeaderUvarint(data, &pos)
	if err != nil {
		return nil, err
	}
	if numChunks > uint64(dataLen-pos) {
		return nil, fmt.Errorf("invalid integer chunk count: %d", numChunks)
	}
	// #nosec G115 -- each chunk needs at least one byte in the remaining int-sized data.
	count := int(numChunks)
	if cap(rv.chunkOffsets) >= count {
		rv.chunkOffsets = rv.chunkOffsets[:count]
	} else {
		rv.chunkOffsets = make([]uint64, count)
	}
	var previous uint64
	for i := range rv.chunkOffsets {
		chunkOffset, readErr := readChunkHeaderUvarint(data, &pos)
		if readErr != nil {
			return nil, readErr
		}
		if chunkOffset < previous {
			return nil, fmt.Errorf("decreasing integer chunk offset")
		}
		rv.chunkOffsets[i] = chunkOffset
		previous = chunkOffset
	}
	if previous > uint64(dataLen-pos) {
		return nil, fmt.Errorf("integer chunk offset exceeds data length")
	}
	// #nosec G115 -- pos starts nonnegative and advances only within data.Len().
	rv.dataStartOffset = uint64(pos)
	return rv, nil
}

func readChunkHeaderUvarint(data *segment.Data, pos *int) (uint64, error) {
	if *pos < 0 || *pos >= data.Len() {
		return 0, fmt.Errorf("integer chunk header truncated")
	}
	end := *pos + min(binary.MaxVarintLen64, data.Len()-*pos)
	buf, err := data.Read(*pos, end)
	if err != nil {
		return 0, err
	}
	value, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("invalid integer chunk header varint")
	}
	*pos += n
	return value, nil
}

func (d *chunkedIntDecoder) loadChunk(chunk int) error {
	if d.startOffset == termNotEncoded {
		d.r = newMemUvarintReader([]byte(nil))
		return nil
	}

	if chunk < 0 || chunk >= len(d.chunkOffsets) {
		return fmt.Errorf("tried to load freq chunk that doesn't exist %d/(%d)",
			chunk, len(d.chunkOffsets))
	}

	end, start := d.dataStartOffset, d.dataStartOffset
	s, e := readChunkBoundary(chunk, d.chunkOffsets)
	dataLen := d.data.Len()
	if dataLen < 0 || s > e || start > uint64(dataLen) || e > uint64(dataLen)-start {
		return fmt.Errorf("integer chunk offsets out of range")
	}
	start += s
	end += e
	// #nosec G115 -- both sums are bounded by the int-sized data length above.
	curChunkBytesData, err := d.data.Read(int(start), int(end))
	if err != nil {
		return err
	}
	if len(curChunkBytesData) == 0 {
		return nil
	}
	d.uncompressed, err = compress.Decompress(d.uncompressed[:cap(d.uncompressed)], curChunkBytesData)
	if err != nil {
		return err
	}
	d.curChunkBytes = d.uncompressed
	if d.r == nil {
		d.r = newMemUvarintReader(d.curChunkBytes)
	} else {
		d.r.Reset(d.curChunkBytes)
	}

	return nil
}

func (d *chunkedIntDecoder) reset() {
	d.startOffset = 0
	d.dataStartOffset = 0
	d.chunkOffsets = d.chunkOffsets[:0]
	d.curChunkBytes = d.curChunkBytes[:0]
	d.uncompressed = d.uncompressed[:0]

	// FIXME what?
	// d.data = d.data[:0]
	d.data = nil
	if d.r != nil {
		d.r.Reset([]byte(nil))
	}
}

func (d *chunkedIntDecoder) isNil() bool {
	return len(d.curChunkBytes) == 0
}

func (d *chunkedIntDecoder) readUvarint() (uint64, error) {
	return d.r.ReadUvarint()
}

func (d *chunkedIntDecoder) SkipUvarint() {
	d.r.SkipUvarint()
}

func (d *chunkedIntDecoder) SkipBytes(count int) {
	d.r.SkipBytes(count)
}

func (d *chunkedIntDecoder) Len() int {
	return d.r.Len()
}
