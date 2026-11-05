//  Copyright (c) 2026 The AtomAI.
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
)

// roaring portable serialization format constants
const (
	roaringCookieNoRuns = 12346
	roaringCookieRuns   = 12347
	roaringCookieMask   = 0xFFFF
	roaringCookieShift  = 16
	roaringHeaderBytes  = 8
	roaringNoOffsetMax  = 4
	roaringMaxSize      = 1 << 16
	roaringKeyShift     = 16
	roaringBitmapWords  = 1 << 16 / wordBits
)

// postingsBytes returns the serialized postings bitmap of a non 1-hit
// postings list at offset, skipping the freq, location and offsets headers
// in front of it.
func (d *Dictionary) postingsBytes(offset uint64) ([]byte, error) {
	n := 3
	if d.sb.footer != nil && d.sb.footer.version >= versionSplitOffsets {
		n = 4
	}
	// #nosec G115 -- a segment's data length is nonnegative.
	size := uint64(d.sb.data.Len())
	if offset >= size {
		return nil, fmt.Errorf("segment varint offset out of bounds: %d", offset)
	}
	hdr, err := readDataAt(d.sb.data, offset, min(uint64(n*binary.MaxVarintLen64), size-offset))
	if err != nil {
		return nil, err
	}
	pos := 0
	var postingsLen uint64
	for range n {
		v, k := binary.Uvarint(hdr[pos:])
		if k <= 0 {
			return nil, fmt.Errorf("invalid segment varint")
		}
		postingsLen, pos = v, pos+k
	}
	return readDataAt(d.sb.data, offset+uint64(pos), postingsLen)
}

// orRoaringDense ORs the portable-format roaring bitmap buf into dense
// without decoding it into a roaring.Bitmap. It returns false when buf is not a well-formed bitmap or holds a value
// past dense, leaving dense partly written; the caller then decodes buf
// with roaring.
func orRoaringDense(dense []uint64, buf []byte) bool {
	if len(buf) < roaringHeaderBytes {
		return false
	}
	cookie := binary.LittleEndian.Uint32(buf)
	var size, pos int
	var runs []byte
	switch {
	case cookie&roaringCookieMask == roaringCookieRuns:
		size = int(cookie>>roaringCookieShift) + 1
		pos = 4 + (size+7)/8
		if pos > len(buf) {
			return false
		}
		runs = buf[4:pos]
	case cookie == roaringCookieNoRuns:
		// bound before int conversion, which wraps negative on 32-bit
		n := binary.LittleEndian.Uint32(buf[4:])
		if n > roaringMaxSize {
			return false
		}
		size = int(n)
		pos = roaringHeaderBytes
	default:
		return false
	}
	hdr := pos
	pos += 4 * size
	if len(runs) == 0 || size >= roaringNoOffsetMax {
		pos += 4 * size
	}
	if pos > len(buf) {
		return false
	}
	for i := range size {
		base := uint64(binary.LittleEndian.Uint16(buf[hdr+4*i:])) << roaringKeyShift
		card := int(binary.LittleEndian.Uint16(buf[hdr+4*i+2:])) + 1
		var ok bool
		switch {
		case len(runs) > i/8 && runs[i/8]&(1<<(i%8)) != 0:
			pos, ok = orRunsDense(dense, buf, pos, base)
		case card > maxArrayContainer:
			pos, ok = orBitmapDense(dense, buf, pos, base)
		default:
			pos, ok = orArrayDense(dense, buf, pos, base, card)
		}
		if !ok {
			return false
		}
	}
	return true
}

// orRunsDense ORs the run container at buf[pos:] into dense, returning
// the position after it.
func orRunsDense(dense []uint64, buf []byte, pos int, base uint64) (int, bool) {
	if pos+2 > len(buf) {
		return 0, false
	}
	nr := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2
	if pos+4*nr > len(buf) {
		return 0, false
	}
	limit := uint64(len(dense)) * wordBits
	for r := range nr {
		start := base + uint64(binary.LittleEndian.Uint16(buf[pos+4*r:]))
		end := start + uint64(binary.LittleEndian.Uint16(buf[pos+4*r+2:])) + 1
		if end > limit {
			return 0, false
		}
		setDenseRange(dense, start, end)
	}
	return pos + 4*nr, true
}

// orBitmapDense ORs the bitmap container at buf[pos:] into dense,
// returning the position after it.
func orBitmapDense(dense []uint64, buf []byte, pos int, base uint64) (int, bool) {
	if pos+8*roaringBitmapWords > len(buf) {
		return 0, false
	}
	w := base / wordBits
	for j := range uint64(roaringBitmapWords) {
		x := binary.LittleEndian.Uint64(buf[pos+8*int(j):])
		if x == 0 {
			continue
		}
		if w+j >= uint64(len(dense)) {
			return 0, false
		}
		dense[w+j] |= x
	}
	return pos + 8*roaringBitmapWords, true
}

// orArrayDense ORs the card-value array container at buf[pos:] into dense,
// returning the position after it.
func orArrayDense(dense []uint64, buf []byte, pos int, base uint64, card int) (int, bool) {
	if pos+2*card > len(buf) {
		return 0, false
	}
	limit := uint64(len(dense)) * wordBits
	for j := range card {
		v := base + uint64(binary.LittleEndian.Uint16(buf[pos+2*j:]))
		if v >= limit {
			return 0, false
		}
		dense[v/wordBits] |= 1 << (v % wordBits)
	}
	return pos + 2*card, true
}

// denseFull reports whether the first numDocs bits of dense are all set.
// *from counts the leading words already found full; bits are only ever
// added, so repeated calls scan each word about once.
func denseFull(dense []uint64, numDocs uint64, from *int) bool {
	if uint64(len(dense))*wordBits < numDocs {
		return false
	}
	// #nosec G115 -- numDocs/64 is at most len(dense), an int.
	last := int(numDocs / wordBits)
	for *from < last && dense[*from] == ^uint64(0) {
		*from++
	}
	if *from < last {
		return false
	}
	tail := uint64(1)<<(numDocs%wordBits) - 1
	return tail == 0 || dense[last]&tail == tail
}
