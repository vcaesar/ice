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
	"errors"
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
	if hasOffsets(offEncoder) {
		if offOffset, err = offEncoder.writeAt(w); err != nil {
			return 0, err
		}
	}

	postingsOffset := uint64(w.Count()) // #nosec G115 -- countHashWriter checks overflow and counts nonnegative writes.

	var header [3 * binary.MaxVarintLen64]byte
	if _, err = w.Write(appendPostingsHeader(header[:0], postingsOffset, tfOffset, locOffset, offOffset, offEncoder != nil)); err != nil {
		return 0, err
	}

	if writeVersion >= versionSlimPostings {
		err = writeDocs(w, postings, &tfEncoder.list)
	} else {
		_, err = writeRoaringWithLen(postings, w, bufMaxVarintLen64)
	}
	if err != nil {
		return 0, err
	}

	return postingsOffset, nil
}

// hasOffsets reports whether offEncoder (nil before version 7) holds an
// offsets stream to write: from version 10 one of only zeros is left out.
func hasOffsets(offEncoder *chunkedIntCoder) bool {
	return offEncoder != nil && len(offEncoder.final) > 0 &&
		(offEncoder.nonZero || writeVersion < versionSlimPostings)
}

// maxListDocs is the most documents a version 10+ postings list keeps as a
// delta list, which every read decodes whole.
const maxListDocs = 1024

// writeDocs writes the documents that end a version 10+ postings header:
// their byte count shifted left over a bit, then the roaring bitmap or,
// with the bit set, the delta list (see appendDocList) that a list of up
// to maxListDocs documents is written as when smaller. scratch is reused.
func writeDocs(w io.Writer, bm *roaring.Bitmap, scratch *[]byte) error {
	bm.RunOptimize()
	size := bm.GetSerializedSizeInBytes()
	buf := (*scratch)[:0]
	if bm.GetCardinality() <= maxListDocs {
		buf = appendDocList(buf, bm)
		if n := len(buf); uint64(n) < size {
			// the head goes after the list in buf and before it in w
			buf = binary.AppendUvarint(buf, uint64(n)<<1|1)
			*scratch = buf
			if _, err := w.Write(buf[n:]); err != nil {
				return err
			}
			_, err := w.Write(buf[:n])
			return err
		}
	}
	buf = binary.AppendUvarint(buf[:0], size<<1)
	*scratch = buf
	if _, err := w.Write(buf); err != nil {
		return err
	}
	_, err := bm.WriteTo(w)
	return err
}

// appendDocList appends the documents of bm as uvarints: the first one,
// then the gap of each to the one before it, less 1.
func appendDocList(dst []byte, bm *roaring.Bitmap) []byte {
	var it roaring.IntIterator
	it.Initialize(bm)
	next := uint32(0) // the least the next document can be
	for it.HasNext() {
		d := it.Next()
		dst = binary.AppendUvarint(dst, uint64(d-next))
		next = d + 1
	}
	return dst
}

// docListBitmap returns the documents of a delta list (see appendDocList)
// as a serialized roaring bitmap of array containers. The buffer is new,
// as the bitmap read from it, and any bitmap ORed with that one, keep it.
func docListBitmap(list []byte) ([]byte, error) {
	var docs [maxListDocs]uint32
	n, err := decodeDocList(list, &docs)
	if err != nil {
		return nil, err
	}
	containers := 0
	key := uint32(math.MaxUint32) // of the last container, none yet
	for _, d := range docs[:n] {
		if d>>roaringKeyShift != key {
			key = d >> roaringKeyShift
			containers++
		}
	}
	// cookie, container count, then per container its key and size less 1,
	// then its offset, and last the low halves of its documents
	data := roaringHeaderBytes + 2*4*containers
	bm := make([]byte, data+2*n)
	binary.LittleEndian.PutUint32(bm, roaringCookieNoRuns)
	binary.LittleEndian.PutUint32(bm[4:], uint32(containers)) // #nosec G115 -- at most maxListDocs.
	keys, offs, lows := bm[roaringHeaderBytes:data], bm[roaringHeaderBytes+4*containers:data], bm[data:]
	k, first := -1, 0 // the container written and its first document
	key = math.MaxUint32
	for i, d := range docs[:n] {
		if d>>roaringKeyShift != key {
			if k >= 0 {
				binary.LittleEndian.PutUint16(keys[4*k+2:], uint16(i-first-1)) // #nosec G115 -- at most maxListDocs.
			}
			key, k, first = d>>roaringKeyShift, k+1, i
			binary.LittleEndian.PutUint16(keys[4*k:], uint16(key))      // #nosec G115 -- the high half.
			binary.LittleEndian.PutUint32(offs[4*k:], uint32(data+2*i)) // #nosec G115 -- a small buffer.
		}
		binary.LittleEndian.PutUint16(lows[2*i:], uint16(d)) // #nosec G115 -- the low half.
	}
	if k >= 0 {
		binary.LittleEndian.PutUint16(keys[4*k+2:], uint16(n-first-1)) // #nosec G115 -- at most maxListDocs.
	}
	return bm, nil
}

// decodeDocList decodes a delta list (see appendDocList) into docs and
// returns how many documents it holds.
func decodeDocList(list []byte, docs *[maxListDocs]uint32) (int, error) {
	n := 0
	next := uint64(0) // the least the next document can be
	for c := 0; c < len(list); n++ {
		if n == maxListDocs {
			return 0, errDocList
		}
		// gaps of short lists take one or two bytes
		if b := list[c]; b < lastByte {
			next += uint64(b)
			c++
		} else if c+1 < len(list) && list[c+1] < lastByte {
			next += uint64(b&significantBits) | uint64(list[c+1])<<7
			c += 2
		} else {
			v, k := binary.Uvarint(list[c:])
			if k <= 0 || v > math.MaxUint32 {
				return 0, errDocList
			}
			next += v
			c += k
		}
		if next > math.MaxUint32 {
			return 0, errDocList
		}
		docs[n] = uint32(next) // #nosec G115 -- checked above.
		next++
	}
	return n, nil
}

var errDocList = errors.New("invalid postings document list")

// appendPostingsHeader appends the chunk offsets of the postings header at
// offset at: tfOffset, then locOffset and, when split (version 7+),
// offOffset, each relative to the one before when both are set. From
// version 10 a set tfOffset is stored as its distance back from at, as the
// chunks are written right before the header.
func appendPostingsHeader(h []byte, at, tfOffset, locOffset, offOffset uint64, split bool) []byte {
	if writeVersion >= versionSlimPostings && tfOffset > 0 {
		h = binary.AppendUvarint(h, at-tfOffset)
	} else {
		h = binary.AppendUvarint(h, tfOffset)
	}
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
	if locEncoder.slim {
		locEncoder.slimLoc(fieldID == locEncoder.ownField && pos >= prevPos, pos-prevPos)
	}
	offEncoder.recordVals(c, d)
	if c|d != 0 {
		offEncoder.nonZero = true
	}
}

// addLocationRecords adds the records recordLocation built for the posting
// of docNum, whose frequency is freq.
func addLocationRecords(docNum, freq uint64, locEncoder, offEncoder *chunkedIntCoder) error {
	if err := locEncoder.addLocRecord(docNum, freq); err != nil {
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
