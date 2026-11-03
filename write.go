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
	"io"
	"math"

	"github.com/RoaringBitmap/roaring/v2"
)

const fieldNotUninverted = math.MaxUint64

type varintEncoder func(uint64) (int, error)

func encodeStoredFieldValues(fieldID int,
	storedFieldValues [][]byte,
	curr int, metaEncode varintEncoder, data []byte) (
	newCurr int, newData []byte, err error) {
	if fieldID < 0 || curr < 0 {
		return 0, nil, fmt.Errorf("negative stored field ID or offset")
	}
	for i := 0; i < len(storedFieldValues); i++ {
		if len(storedFieldValues[i]) > math.MaxInt-curr {
			return 0, nil, fmt.Errorf("stored field offset overflow")
		}
		// encode field
		_, err := metaEncode(uint64(fieldID))
		if err != nil {
			return 0, nil, err
		}
		// encode start offset
		_, err = metaEncode(uint64(curr))
		if err != nil {
			return 0, nil, err
		}
		// end len
		_, err = metaEncode(uint64(len(storedFieldValues[i])))
		if err != nil {
			return 0, nil, err
		}

		data = append(data, storedFieldValues[i]...)
		curr += len(storedFieldValues[i])
	}

	return curr, data, nil
}

// writePostings writes the chunks of tfEncoder, locEncoder and, when not
// nil (version 7+), offEncoder, then the postings header and bitmap.
func writePostings(postings *roaring.Bitmap, tfEncoder, locEncoder, offEncoder *chunkedIntCoder,
	use1HitEncoding func(uint64) (bool, uint64, uint64),
	w *countHashWriter, bufMaxVarintLen64 []byte) (
	offset uint64, err error) {
	termCardinality := postings.GetCardinality()
	if termCardinality <= 0 {
		return 0, nil
	}

	if use1HitEncoding != nil {
		encodeAs1Hit, docNum1Hit, normBits1Hit := use1HitEncoding(termCardinality)
		if encodeAs1Hit {
			return fSTValEncode1Hit(docNum1Hit, normBits1Hit), nil
		}
	}

	var tfOffset uint64
	tfOffset, err = tfEncoder.writeAt(w)
	if err != nil {
		return 0, err
	}

	var locOffset uint64
	locOffset, err = locEncoder.writeAt(w)
	if err != nil {
		return 0, err
	}

	var offOffset uint64
	if offEncoder != nil {
		if offOffset, err = offEncoder.writeAt(w); err != nil {
			return 0, err
		}
	}

	postingsOffset := uint64(w.Count()) // #nosec G115 -- countHashWriter checks overflow and counts nonnegative writes.

	var header [3 * binary.MaxVarintLen64]byte
	if _, err = w.Write(appendPostingsHeader(header[:0], tfOffset, locOffset, offOffset, offEncoder != nil)); err != nil {
		return 0, err
	}

	_, err = writeRoaringWithLen(postings, w, bufMaxVarintLen64)
	if err != nil {
		return 0, err
	}

	return postingsOffset, nil
}

// appendPostingsHeader appends the chunk offsets of a postings header:
// tfOffset, then locOffset and, when split (version 7+), offOffset, each
// relative to the one before when both are set.
func appendPostingsHeader(h []byte, tfOffset, locOffset, offOffset uint64, split bool) []byte {
	h = binary.AppendUvarint(h, tfOffset)
	h = binary.AppendUvarint(h, relOffset(locOffset, tfOffset))
	if split {
		h = binary.AppendUvarint(h, relOffset(offOffset, locOffset))
	}
	return h
}

// relOffset is offset relative to prev when both are set, else offset.
func relOffset(offset, prev uint64) uint64 {
	if offset > 0 && prev > 0 {
		return offset - prev
	}
	return offset
}

// recordLocation appends the locationVals of a location to the pending
// record of locEncoder, or, when offEncoder is not nil (version 7+), its
// field and pos to locEncoder's and its start and end to offEncoder's.
func recordLocation(locEncoder, offEncoder *chunkedIntCoder, fieldID, pos, start, end, prevPos, prevStart uint64) {
	a, b, c, d := locationVals(locEncoder.compact, fieldID, pos, start, end, prevPos, prevStart)
	if offEncoder == nil {
		locEncoder.recordVals(a, b, c, d)
		return
	}
	locEncoder.recordVals(a, b)
	offEncoder.recordVals(c, d)
}

// addLocationRecords adds the records recordLocation built for docNum.
func addLocationRecords(docNum uint64, locEncoder, offEncoder *chunkedIntCoder) error {
	if err := locEncoder.addRecord(docNum); err != nil {
		return err
	}
	if offEncoder != nil {
		return offEncoder.addRecord(docNum)
	}
	return nil
}

// locationVals returns the four values stored for a location. Compact
// (version 6+) segments store pos and start as zigzag deltas from the
// previous location of the same posting (prevPos, prevStart; 0 for the
// first) and end as the length end-start.
func locationVals(compact bool, fieldID, pos, start, end, prevPos, prevStart uint64) (a, b, c, d uint64) {
	if !compact {
		return fieldID, pos, start, end
	}
	// #nosec G115 -- two's-complement deltas, inverted exactly by unzigzag.
	return fieldID, zigzag(int64(pos - prevPos)), zigzag(int64(start - prevStart)), end - start
}

func zigzag(v int64) uint64 {
	// #nosec G115 -- zigzag maps the full int64 range onto uint64.
	return uint64((v << 1) ^ (v >> 63))
}

func unzigzag(v uint64) int64 {
	// #nosec G115 -- inverse of zigzag.
	return int64(v>>1) ^ -int64(v&1)
}

// writes out the length of the roaring bitmap in bytes as varint
// then writes out the roaring bitmap itself
func writeRoaringWithLen(r *roaring.Bitmap, w io.Writer,
	reuseBufVarint []byte) (int, error) {
	r.RunOptimize()
	buf, err := r.ToBytes()
	if err != nil {
		return 0, err
	}

	var tw int

	// write out the length
	n := binary.PutUvarint(reuseBufVarint, uint64(len(buf)))
	nw, err := w.Write(reuseBufVarint[:n])
	tw += nw
	if err != nil {
		return tw, err
	}

	// write out the roaring bytes
	nw, err = w.Write(buf)
	tw += nw
	if err != nil {
		return tw, err
	}

	return tw, nil
}

func persistFields(fieldsInv []string, fieldDocs, fieldFreqs map[uint16]uint64,
	w *countHashWriter, dictLocs []uint64) (uint64, error) {
	var rv uint64
	var fieldsOffsets []uint64

	for fieldID, fieldName := range fieldsInv {
		// record start of this field
		// #nosec G115 -- countHashWriter checks overflow and counts nonnegative writes.
		fieldsOffsets = append(fieldsOffsets, uint64(w.Count()))

		// write out the dict location and field name length
		err := writeUvarints(w, dictLocs[fieldID], uint64(len(fieldName)))
		if err != nil {
			return 0, err
		}

		// write out the field name
		_, err = w.Write([]byte(fieldName))
		if err != nil {
			return 0, err
		}

		// write out the number of docs using this field
		// and the number of total tokens
		err = writeUvarints(w, fieldDocs[uint16(fieldID)], fieldFreqs[uint16(fieldID)])
		if err != nil {
			return 0, err
		}
	}

	// now write out the fields index
	rv = uint64(w.Count()) // #nosec G115 -- countHashWriter checks overflow and counts nonnegative writes.
	for fieldID := range fieldsInv {
		err := binary.Write(w, binary.BigEndian, fieldsOffsets[fieldID])
		if err != nil {
			return 0, err
		}
	}

	return rv, nil
}

func persistFooter(footer *footer, writerIn io.Writer) error {
	w := newCountHashWriter(writerIn)
	w.crc = footer.crc

	// write out the number of docs
	err := binary.Write(w, binary.BigEndian, footer.numDocs)
	if err != nil {
		return err
	}
	// write out the stored field index location:
	err = binary.Write(w, binary.BigEndian, footer.storedIndexOffset)
	if err != nil {
		return err
	}
	// write out the field index location
	err = binary.Write(w, binary.BigEndian, footer.fieldsIndexOffset)
	if err != nil {
		return err
	}
	// write out the fieldDocValue location
	err = binary.Write(w, binary.BigEndian, footer.docValueOffset)
	if err != nil {
		return err
	}
	// write out 32-bit chunk factor
	err = binary.Write(w, binary.BigEndian, footer.chunkMode)
	if err != nil {
		return err
	}
	// write out timestamp
	err = binary.Write(w, binary.BigEndian, footer.docTimeMin)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, footer.docTimeMax)
	if err != nil {
		return err
	}
	// write out 32-bit version
	err = binary.Write(w, binary.BigEndian, footer.version)
	if err != nil {
		return err
	}
	// write out CRC-32 of everything upto but not including this CRC
	err = binary.Write(w, binary.BigEndian, w.crc)
	if err != nil {
		return err
	}
	return nil
}

func writeUvarints(w io.Writer, vals ...uint64) (err error) {
	buf := make([]byte, binary.MaxVarintLen64)
	for _, val := range vals {
		n := binary.PutUvarint(buf, val)
		_, err = w.Write(buf[:n])
		if err != nil {
			return err
		}
	}
	return err
}
