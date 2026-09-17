//  Copyright (c) 2026 The Bluge Authors.
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
	"io"
	"math"
	"sort"

	segment "github.com/vcaesar/bluge_segment_api"
)

// NumericField is the segment API's typed numeric field; kept as an alias
// so existing callers in this package keep compiling.
type NumericField = segment.NumericField

// Numeric column layout, per field:
//
//	block data   – for every block of numericBlockSize doc numbers:
//	               [docDelta uvarint]*count (omitted when dense)
//	               [count * width bytes little-endian of uint64(v)-uint64(min)]
//	block index  – per block: [count uvarint] and, when count > 0,
//	               [dense byte][min zigzag][max zigzag][width byte][dataOffset uvarint]
//	trailer      – [blockIndexOffset u64][numBlocks u64], both big endian
//
// Doc numbers repeat for multi-valued fields; "dense" means the block holds
// exactly one value for each consecutive doc number starting at the block.
const numericBlockSize = 1024

const numericColumnTrailerLen = 16

// byte widths of block values
const (
	widthNone  uint8 = 0
	widthByte  uint8 = 1
	widthWord  uint8 = 2
	widthDword uint8 = 4
	widthQword uint8 = 8
)

const (
	prefixCodedShiftStart = 0x20
	prefixCodedChars      = 10 // ((63 - 0) / 7) + 1
	prefixCodedMask       = 0x7f
	prefixCodedSignBit    = 0x8000000000000000
)

// doc value kinds recorded in the per-field doc value index
const (
	dvKindTerms   uint64 = 0
	dvKindNumeric uint64 = 1
)

type numericBlockMeta struct {
	count  uint32
	dense  bool
	width  uint8
	min    int64
	max    int64
	offset uint64
}

func numericWidth(minVal, maxVal int64) uint8 {
	// #nosec G115 -- signed order is preserved by the two's-complement wrap.
	diff := uint64(maxVal) - uint64(minVal)
	switch {
	case diff == 0:
		return widthNone
	case diff < 1<<8:
		return widthByte
	case diff < 1<<16:
		return widthWord
	case diff < 1<<32:
		return widthDword
	default:
		return widthQword
	}
}

type numericColumnWriter struct {
	numBlocks uint64
	curBlock  uint64
	lastDoc   uint64
	docs      []uint64
	vals      []int64
	data      bytes.Buffer
	index     bytes.Buffer
	buf       [binary.MaxVarintLen64]byte
}

func newNumericColumnWriter(numDocs uint64) *numericColumnWriter {
	return &numericColumnWriter{numBlocks: (numDocs + numericBlockSize - 1) / numericBlockSize}
}

func (w *numericColumnWriter) Reset(numDocs uint64) {
	w.numBlocks = (numDocs + numericBlockSize - 1) / numericBlockSize
	w.curBlock = 0
	w.lastDoc = 0
	w.docs = w.docs[:0]
	w.vals = w.vals[:0]
	w.data.Reset()
	w.index.Reset()
}

// Add appends a value; docNum must not decrease between calls.
func (w *numericColumnWriter) Add(docNum uint64, v int64) error {
	if docNum < w.lastDoc {
		return fmt.Errorf("numeric column doc %d out of order after %d", docNum, w.lastDoc)
	}
	block := docNum / numericBlockSize
	if block >= w.numBlocks {
		return fmt.Errorf("numeric column doc %d beyond %d blocks", docNum, w.numBlocks)
	}
	for w.curBlock < block {
		w.flushBlock()
	}
	w.lastDoc = docNum
	w.docs = append(w.docs, docNum)
	w.vals = append(w.vals, v)
	return nil
}

func (w *numericColumnWriter) putUvarint(b *bytes.Buffer, v uint64) {
	n := binary.PutUvarint(w.buf[:], v)
	b.Write(w.buf[:n])
}

func (w *numericColumnWriter) putVarint(b *bytes.Buffer, v int64) {
	n := binary.PutVarint(w.buf[:], v)
	b.Write(w.buf[:n])
}

func (w *numericColumnWriter) flushBlock() {
	count := len(w.docs)
	w.putUvarint(&w.index, uint64(count))
	if count == 0 {
		w.curBlock++
		return
	}
	blockStart := w.curBlock * numericBlockSize
	dense := true
	minVal, maxVal := w.vals[0], w.vals[0]
	for i, d := range w.docs {
		if d != blockStart+uint64(i) {
			dense = false
		}
		if w.vals[i] < minVal {
			minVal = w.vals[i]
		}
		if w.vals[i] > maxVal {
			maxVal = w.vals[i]
		}
	}
	width := numericWidth(minVal, maxVal)

	// #nosec G115 -- bytes.Buffer length is nonnegative.
	w.putUvarint(&w.index, uint64(w.data.Len()))
	if dense {
		w.index.WriteByte(1)
	} else {
		w.index.WriteByte(0)
	}
	w.putVarint(&w.index, minVal)
	w.putVarint(&w.index, maxVal)
	w.index.WriteByte(width)

	if !dense {
		prev := blockStart
		for _, d := range w.docs {
			w.putUvarint(&w.data, d-prev)
			prev = d
		}
	}
	for _, v := range w.vals {
		// #nosec G115 -- signed order is preserved by the two's-complement wrap.
		delta := uint64(v) - uint64(minVal)
		binary.LittleEndian.PutUint64(w.buf[:8], delta)
		w.data.Write(w.buf[:width])
	}

	w.docs = w.docs[:0]
	w.vals = w.vals[:0]
	w.curBlock++
}

// Write flushes all blocks and writes the column, returning bytes written.
func (w *numericColumnWriter) Write(out io.Writer) (int, error) {
	for w.curBlock < w.numBlocks {
		w.flushBlock()
	}
	n, err := out.Write(w.data.Bytes())
	if err != nil {
		return n, err
	}
	m, err := out.Write(w.index.Bytes())
	n += m
	if err != nil {
		return n, err
	}
	var trailer [numericColumnTrailerLen]byte
	// #nosec G115 -- bytes.Buffer length is nonnegative.
	binary.BigEndian.PutUint64(trailer[:8], uint64(w.data.Len()))
	binary.BigEndian.PutUint64(trailer[8:], w.numBlocks)
	m, err = out.Write(trailer[:])
	return n + m, err
}

type numericColumn struct {
	field   string
	dataLoc uint64
	dataEnd uint64 // end of block data relative to dataLoc
	blocks  []numericBlockMeta
}

func (c *numericColumn) size() int {
	return len(c.field) + sizeOfString + len(c.blocks)*reflectStaticSizeNumericBlockMeta
}

func (s *Segment) loadNumericColumn(field string, start, end uint64) (*numericColumn, error) {
	dataLen := s.data.Len()
	if dataLen < 0 || start > end || end > uint64(dataLen) || end-start < numericColumnTrailerLen {
		return nil, fmt.Errorf("invalid numeric column range: %d-%d", start, end)
	}
	// #nosec G115 -- bounded by data.Len(), an int.
	tail, err := s.data.Read(int(end-numericColumnTrailerLen), int(end))
	if err != nil {
		return nil, err
	}
	indexOffset := binary.BigEndian.Uint64(tail[:8])
	numBlocks := binary.BigEndian.Uint64(tail[8:])
	indexLen := end - start - numericColumnTrailerLen
	if indexOffset > indexLen || numBlocks > indexLen-indexOffset {
		return nil, fmt.Errorf("invalid numeric column index for field %s", field)
	}
	// #nosec G115 -- bounded by data.Len(), an int.
	index, err := s.data.Read(int(start+indexOffset), int(end-numericColumnTrailerLen))
	if err != nil {
		return nil, err
	}
	col := &numericColumn{field: field, dataLoc: start, dataEnd: indexOffset, blocks: make([]numericBlockMeta, numBlocks)}
	for i := range col.blocks {
		if err = col.blocks[i].decode(&index, indexOffset); err != nil {
			return nil, fmt.Errorf("numeric column %s block %d: %w", field, i, err)
		}
	}
	return col, nil
}

func (m *numericBlockMeta) decode(index *[]byte, dataLen uint64) error {
	count, err := readUvarint(index)
	if err != nil {
		return err
	}
	if count > numericBlockSize*numericBlockSize {
		return fmt.Errorf("invalid count %d", count)
	}
	// #nosec G115 -- bounded above.
	m.count = uint32(count)
	if count == 0 {
		return nil
	}
	if m.offset, err = readUvarint(index); err != nil {
		return err
	}
	if len(*index) < 1 {
		return io.ErrUnexpectedEOF
	}
	m.dense = (*index)[0] == 1
	*index = (*index)[1:]
	if m.min, err = readVarint(index); err != nil {
		return err
	}
	if m.max, err = readVarint(index); err != nil {
		return err
	}
	if len(*index) < 1 {
		return io.ErrUnexpectedEOF
	}
	m.width = (*index)[0]
	*index = (*index)[1:]
	if m.min > m.max || m.width != numericWidth(m.min, m.max) || m.offset > dataLen ||
		count*uint64(m.width) > dataLen-m.offset {
		return fmt.Errorf("corrupt block metadata")
	}
	return nil
}

func readUvarint(b *[]byte) (uint64, error) {
	v, n := binary.Uvarint(*b)
	if n <= 0 {
		return 0, fmt.Errorf("invalid uvarint")
	}
	*b = (*b)[n:]
	return v, nil
}

func readVarint(b *[]byte) (int64, error) {
	v, n := binary.Varint(*b)
	if n <= 0 {
		return 0, fmt.Errorf("invalid varint")
	}
	*b = (*b)[n:]
	return v, nil
}

// numericColumnCursor decodes one block at a time and serves in-order
// lookups in O(1) amortized.
type numericColumnCursor struct {
	col   *numericColumn
	block uint64
	docs  []uint64 // nil for dense blocks
	vals  []int64
	pos   int
}

func newNumericColumnCursor(col *numericColumn) *numericColumnCursor {
	return &numericColumnCursor{col: col, block: math.MaxUint64}
}

func (c *numericColumnCursor) loadBlock(s *Segment, block uint64) error {
	c.block = block
	c.pos = 0
	c.docs = c.docs[:0]
	c.vals = c.vals[:0]
	if block >= uint64(len(c.col.blocks)) {
		return nil
	}
	m := &c.col.blocks[block]
	if m.count == 0 {
		return nil
	}
	start := c.col.dataLoc + m.offset
	end := c.col.dataLoc + c.col.dataEnd
	for next := block + 1; next < uint64(len(c.col.blocks)); next++ {
		if c.col.blocks[next].count > 0 {
			end = c.col.dataLoc + c.col.blocks[next].offset
			break
		}
	}
	dataLen := s.data.Len()
	if dataLen < 0 || start > end || end > uint64(dataLen) {
		return fmt.Errorf("numeric column block %d out of range", block)
	}
	// #nosec G115 -- bounded by data.Len(), an int.
	data, err := s.data.Read(int(start), int(end))
	if err != nil {
		return err
	}
	count := int(m.count)
	blockStart := block * numericBlockSize
	if !m.dense {
		prev := blockStart
		for i := 0; i < count; i++ {
			delta, err := readUvarint(&data)
			if err != nil {
				return err
			}
			prev += delta
			c.docs = append(c.docs, prev)
		}
	}
	if len(data) < count*int(m.width) {
		return io.ErrUnexpectedEOF
	}
	if cap(c.vals) < count {
		c.vals = make([]int64, 0, count)
	}
	for i := 0; i < count; i++ {
		var delta uint64
		switch m.width {
		case widthByte:
			delta = uint64(data[i])
		case widthWord:
			delta = uint64(binary.LittleEndian.Uint16(data[i*2:]))
		case widthDword:
			delta = uint64(binary.LittleEndian.Uint32(data[i*4:]))
		case widthQword:
			delta = binary.LittleEndian.Uint64(data[i*8:])
		}
		// #nosec G115 -- inverse of the writer's two's-complement wrap.
		c.vals = append(c.vals, int64(uint64(m.min)+delta))
	}
	return nil
}

// visit calls visitor for every value of docNum.
func (c *numericColumnCursor) visit(s *Segment, docNum uint64, visitor func(field string, value int64)) error {
	block := docNum / numericBlockSize
	if block != c.block {
		if err := c.loadBlock(s, block); err != nil {
			return err
		}
	}
	if len(c.vals) == 0 {
		return nil
	}
	if c.col.blocks[block].dense {
		i := docNum - block*numericBlockSize
		if i < uint64(len(c.vals)) {
			visitor(c.col.field, c.vals[i])
		}
		return nil
	}
	if c.pos >= len(c.docs) || c.docs[c.pos] > docNum {
		c.pos = sort.Search(len(c.docs), func(i int) bool { return c.docs[i] >= docNum })
	}
	for c.pos < len(c.docs) && c.docs[c.pos] < docNum {
		c.pos++
	}
	for c.pos < len(c.docs) && c.docs[c.pos] == docNum {
		visitor(c.col.field, c.vals[c.pos])
		c.pos++
	}
	return nil
}

// iterate visits every (docNum, value) pair in doc order.
func (c *numericColumnCursor) iterate(s *Segment, visitor func(docNum uint64, value int64) error) error {
	for block := range c.col.blocks {
		if err := c.loadBlock(s, uint64(block)); err != nil {
			return err
		}
		blockStart := uint64(block) * numericBlockSize
		dense := c.col.blocks[block].dense
		for i, v := range c.vals {
			docNum := blockStart + uint64(i)
			if !dense {
				docNum = c.docs[i]
			}
			if err := visitor(docNum, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// prefixCodedInt64 encodes v like bluge's numeric.NewPrefixCodedInt64 with
// shift 0, the term form consumers expect for numeric doc values.
func prefixCodedInt64(v int64) []byte {
	return appendPrefixCodedInt64(nil, v)
}

func appendPrefixCodedInt64(buf []byte, v int64) []byte {
	start := len(buf)
	buf = append(buf, make([]byte, prefixCodedChars+1)...)
	buf[start] = prefixCodedShiftStart
	// #nosec G115 -- flip the sign bit, keeping the two's-complement pattern.
	bits := uint64(v) ^ prefixCodedSignBit
	for i := prefixCodedChars; i > 0; i-- {
		buf[start+i] = byte(bits & prefixCodedMask)
		bits >>= 7
	}
	return buf
}

// decodePrefixCodedInt64 is the inverse of prefixCodedInt64 for shift 0
// terms; ok is false for any other term.
func decodePrefixCodedInt64(term []byte) (v int64, ok bool) {
	if len(term) != prefixCodedChars+1 || term[0] != prefixCodedShiftStart {
		return 0, false
	}
	var bits uint64
	for _, b := range term[1:] {
		bits = bits<<7 | uint64(b)
	}
	// #nosec G115 -- restore the sign bit flipped by the encoder.
	return int64(bits ^ prefixCodedSignBit), true
}
