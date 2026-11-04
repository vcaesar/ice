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

package compress

import (
	"encoding/binary"

	s2c "github.com/klauspost/compress/s2"
)

// repeatRatio is the decoded to encoded size ratio from which S2Decompress
// uses decodeRepeats: such blocks are mostly long copies of a short
// pattern, which s2's assembly decoder copies a byte at a time when they
// reach the end of the block (verySlowForwardCopy).
const repeatRatio = 16

// s2 block element tags (the low two bits of an element's first byte).
const (
	tagLiteral = 0x00
	tagCopy1   = 0x01
	tagCopy2   = 0x02
	tagCopy4   = 0x03
	tagMask    = 0x03
)

// s2 block element layout.
const (
	literalInline  = 60   // literal lengths from here on follow the tag, in x-59 bytes
	copy1LenMask   = 0x07 // tag bits 2-4: copy1 length - 4
	copy1OffHigh   = 0xe0 // tag bits 5-7: high bits of the copy1 offset
	repeatInline   = 5    // repeat lengths from here on follow the tag, in length-4 bytes
	copy1Size      = 2
	copy2Size      = 3
	copy4Size      = 5
	copyMinLength  = 4 // added to copy1 and repeat lengths
	lenByteBias    = literalInline - 1
	repeatByteBias = repeatInline - 1
)

// decodeRepeats is s2c.Decode, block format with s2's repeat offsets,
// that expands an overlapping copy by doubling the span it copies.
func decodeRepeats(dst, src []byte) ([]byte, error) {
	dLen, err := s2c.DecodedLen(src)
	if err != nil {
		return nil, err
	}
	_, s := binary.Uvarint(src)
	if dLen <= cap(dst) {
		dst = dst[:dLen]
	} else {
		dst = make([]byte, dLen)
	}
	var d, offset int
	for s < len(src) {
		var length int
		var ok bool
		if src[s]&tagMask == tagLiteral {
			if s, length, ok = readLiteral(src, s); !ok || length > len(dst)-d {
				return nil, s2c.ErrCorrupt
			}
			d += copy(dst[d:], src[s:s+length])
			s += length
			continue
		}
		if s, length, offset, ok = readCopy(src, s, offset); !ok || offset <= 0 || d < offset || length > len(dst)-d {
			return nil, s2c.ErrCorrupt
		}
		// dst[from:d] repeats with period offset, so copying it forward
		// keeps the period and doubles what the next round can copy
		from, end := d-offset, d+length
		for d < end {
			d += copy(dst[d:end], dst[from:d])
		}
	}
	if d != len(dst) {
		return nil, s2c.ErrCorrupt
	}
	return dst, nil
}

// readLiteral reads the literal element tagged at src[s]: its bytes are
// src[next:next+length]. ok is false when src ends first.
func readLiteral(src []byte, s int) (next, length int, ok bool) {
	x := int(src[s] >> 2)
	s++
	if x >= literalInline {
		n := x - lenByteBias
		if x, ok = uintLE(src, s, n); !ok {
			return 0, 0, false
		}
		s += n
	}
	length = x + 1
	return s, length, length <= len(src)-s
}

// readCopy reads the copy element tagged at src[s], given the offset of
// the last copy, which an s2 repeat reuses. ok is false when src ends
// first; the caller checks offset and length against the output.
func readCopy(src []byte, s, lastOffset int) (next, length, offset int, ok bool) {
	tag := src[s]
	switch tag & tagMask {
	case tagCopy1:
		if len(src)-s < copy1Size {
			return 0, 0, 0, false
		}
		length, offset = int(tag>>2&copy1LenMask), int(tag&copy1OffHigh)<<3|int(src[s+1])
		s += copy1Size
		if offset == 0 {
			offset = lastOffset
			if length >= repeatInline { // the repeat length follows
				n := length - repeatByteBias
				x, ok := uintLE(src, s, n)
				if !ok {
					return 0, 0, 0, false
				}
				s += n
				length = x + [...]int{copyMinLength, 1 << 8, 1 << 16}[n-1]
			}
		}
		return s, length + copyMinLength, offset, true
	case tagCopy2:
		if len(src)-s < copy2Size {
			return 0, 0, 0, false
		}
		return s + copy2Size, 1 + int(tag>>2), int(binary.LittleEndian.Uint16(src[s+1:])), true
	default: // tagCopy4
		if len(src)-s < copy4Size {
			return 0, 0, 0, false
		}
		return s + copy4Size, 1 + int(tag>>2), int(binary.LittleEndian.Uint32(src[s+1:])), true
	}
}

// uintLE returns the n-byte little-endian value at src[s:]; ok is false
// when src ends first.
func uintLE(src []byte, s, n int) (x int, ok bool) {
	if n > len(src)-s {
		return 0, false
	}
	for i := n - 1; i >= 0; i-- {
		x = x<<8 | int(src[s+i])
	}
	return x, true
}
