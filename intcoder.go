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
	"io"
	"math"

	"github.com/vcaesar/ice/compress"
)

// We can safely use 0 to represent termNotEncoded since 0
// could never be a valid address for term location information.
// (stored field index is always non-empty and earlier in the
// file)
const termNotEncoded = 0

const initialIntCoderCapacity = 64

// freqNormVals is the number of values, (freqHasLocs, normBits), each Add
// of a bounds-tracking coder takes.
const freqNormVals = 2

type chunkedIntCoder struct {
	final     []byte
	chunkSize uint64
	chunkBuf  bytes.Buffer
	chunkLens []uint64
	currChunk uint64

	buf        []byte
	compressed []byte
	rec        []byte // pending record, see recordVals

	// bounds, when set, records per chunk the max freq and min norm bits of
	// (freqHasLocs, normBits) values and writes them after the offsets
	bounds   bool
	maxFreqs []uint64
	minNorms []uint64

	// compact (version 6+) prefixes each chunk with chunkRaw or chunkS2
	// and keeps it raw unless s2 saves at least a quarter; empty chunks
	// take no bytes
	compact bool
}

// chunk flag bytes of compact (version 6+) int coder chunks
const (
	chunkRaw byte = 0
	// chunkS2 chunks (versions 6 and 7) use compress.Algorithm
	chunkS2 byte = 1
	// chunkS2Fixed chunks (version 8+) are s2 blocks
	chunkS2Fixed byte = 2
)

// newChunkedIntCoder returns a new chunk int coder which packs data into
// chunks based on the provided chunkSize and supports up to the specified
// maxDocNum
func newChunkedIntCoder(chunkSize, maxDocNum uint64) *chunkedIntCoder {
	total := maxDocNum/chunkSize + 1
	rv := &chunkedIntCoder{
		chunkSize: chunkSize,
		chunkLens: make([]uint64, total),
		final:     make([]byte, 0, initialIntCoderCapacity),
	}

	return rv
}

// Reset lets you reuse this chunked int coder.  buffers are reset and reused
// from previous use.  you cannot change the chunk size or max doc num.
func (c *chunkedIntCoder) Reset() {
	c.final = c.final[:0]
	c.chunkBuf.Reset()
	c.rec = c.rec[:0]
	c.currChunk = 0
	for i := range c.chunkLens {
		c.chunkLens[i] = 0
	}
	clear(c.maxFreqs)
	clear(c.minNorms)
}

// trackBounds makes the coder record and write per-chunk freq/norm bounds;
// every Add must then pass (freqHasLocs, normBits).
func (c *chunkedIntCoder) trackBounds() {
	c.bounds = true
	c.maxFreqs = make([]uint64, len(c.chunkLens))
	c.minNorms = make([]uint64, len(c.chunkLens))
}

// SetChunkSize changes the chunk size.  It is only valid to do so
// with a new chunkedIntCoder, or immediately after calling Reset()
func (c *chunkedIntCoder) SetChunkSize(chunkSize, maxDocNum uint64) {
	chunks := maxDocNum / chunkSize
	if chunks >= math.MaxInt {
		panic("chunk count does not fit int")
	}
	total := int(chunks) + 1
	c.chunkSize = chunkSize
	if cap(c.chunkLens) < total {
		c.chunkLens = make([]uint64, total)
	} else {
		c.chunkLens = c.chunkLens[:total]
	}
	if c.bounds {
		if cap(c.maxFreqs) < total {
			c.maxFreqs = make([]uint64, total)
			c.minNorms = make([]uint64, total)
		} else {
			c.maxFreqs = c.maxFreqs[:total]
			c.minNorms = c.minNorms[:total]
		}
	}
}

// Add encodes the provided integers into the correct chunk for the provided
// doc num.  You MUST call Add() with increasing docNums.
func (c *chunkedIntCoder) Add(docNum uint64, vals ...uint64) error {
	chunk, err := c.enterChunk(docNum)
	if err != nil {
		return err
	}
	if c.bounds {
		if len(vals) != freqNormVals {
			return fmt.Errorf("bounds tracking needs (freqHasLocs, normBits), got %d values", len(vals))
		}
		freq, norm := vals[0]>>1, vals[1]
		if c.chunkBuf.Len() == 0 {
			c.maxFreqs[chunk], c.minNorms[chunk] = freq, norm
		} else {
			c.maxFreqs[chunk] = max(c.maxFreqs[chunk], freq)
			c.minNorms[chunk] = min(c.minNorms[chunk], norm)
		}
	}

	// encode every value into buf first so the chunk buffer sees one write
	need := binary.MaxVarintLen64 * len(vals)
	if len(c.buf) < need {
		c.buf = make([]byte, need)
	}
	n := 0
	for _, val := range vals {
		n += binary.PutUvarint(c.buf[n:], val)
	}
	_, err = c.chunkBuf.Write(c.buf[:n])
	return err
}

// enterChunk closes the current chunk when docNum is in another one and
// returns the chunk of docNum.
func (c *chunkedIntCoder) enterChunk(docNum uint64) (uint64, error) {
	chunk := docNum / c.chunkSize
	if chunk != c.currChunk {
		// starting a new chunk
		if err := c.Close(); err != nil {
			return 0, err
		}
		c.chunkBuf.Reset()
		c.currChunk = chunk
	}
	return chunk, nil
}

// recordVals appends vals to the pending record, which addRecord adds.
func (c *chunkedIntCoder) recordVals(vals ...uint64) {
	for _, v := range vals {
		c.rec = binary.AppendUvarint(c.rec, v)
	}
}

// addRecord adds the pending record, prefixed by its uvarint byte count,
// for docNum, as Add, and clears it.
func (c *chunkedIntCoder) addRecord(docNum uint64) error {
	if _, err := c.enterChunk(docNum); err != nil {
		return err
	}
	var n [binary.MaxVarintLen64]byte
	if _, err := c.chunkBuf.Write(binary.AppendUvarint(n[:0], uint64(len(c.rec)))); err != nil {
		return err
	}
	_, err := c.chunkBuf.Write(c.rec)
	c.rec = c.rec[:0]
	return err
}

// Close indicates you are done calling Add() this allows the final chunk
// to be encoded.
func (c *chunkedIntCoder) Close() error {
	if c.compact {
		return c.closeCompact()
	}
	var err error
	c.compressed, err = compress.Compress(c.compressed[:cap(c.compressed)], c.chunkBuf.Bytes())
	if err != nil {
		return err
	}
	c.chunkLens[c.currChunk] = uint64(len(c.compressed))
	c.final = append(c.final, c.compressed...)
	c.currChunk = uint64(cap(c.chunkLens)) // sentinel to detect double close
	return nil
}

// closeCompact is Close for compact (version 6+) chunks: a flag byte, then
// the s2 block when it saves at least a quarter, else the raw varints.
func (c *chunkedIntCoder) closeCompact() error {
	raw := c.chunkBuf.Bytes()
	start := len(c.final)
	if len(raw) > 0 {
		var err error
		flag := chunkS2
		if writeVersion >= versionS2Chunks {
			flag = chunkS2Fixed
			c.compressed, err = compress.S2Compress(c.compressed[:cap(c.compressed)], raw)
		} else {
			c.compressed, err = compress.Compress(c.compressed[:cap(c.compressed)], raw)
		}
		if err != nil {
			return err
		}
		if len(c.compressed)*4 <= len(raw)*3 {
			c.final = append(append(c.final, flag), c.compressed...)
		} else {
			c.final = append(append(c.final, chunkRaw), raw...)
		}
	}
	c.chunkLens[c.currChunk] = uint64(len(c.final) - start) // #nosec G115 -- final only grows past start.
	c.currChunk = uint64(cap(c.chunkLens))                  // sentinel to detect double close
	return nil
}

// Write commits all the encoded chunked integers to the provided writer.
func (c *chunkedIntCoder) Write(w io.Writer) (int, error) {
	bufNeeded := binary.MaxVarintLen64 * (1 + 3*len(c.chunkLens))
	if len(c.buf) < bufNeeded {
		c.buf = make([]byte, bufNeeded)
	}
	buf := c.buf

	// convert the chunk lengths into chunk offsets
	chunkOffsets := modifyLengthsToEndOffsets(c.chunkLens)

	// write out the number of chunks & each chunk offsets
	n := binary.PutUvarint(buf, uint64(len(chunkOffsets)))
	for _, chunkOffset := range chunkOffsets {
		n += binary.PutUvarint(buf[n:], chunkOffset)
	}
	if c.bounds {
		for k := range chunkOffsets {
			n += binary.PutUvarint(buf[n:], c.maxFreqs[k])
			n += binary.PutUvarint(buf[n:], c.minNorms[k])
		}
	}

	tw, err := w.Write(buf[:n])
	if err != nil {
		return tw, err
	}

	// write out the data
	nw, err := w.Write(c.final)
	tw += nw
	if err != nil {
		return tw, err
	}
	return tw, nil
}

// writeAt commits all the encoded chunked integers to the provided writer
// and returns the starting offset, total bytes written and an error
func (c *chunkedIntCoder) writeAt(w io.Writer) (startOffset uint64, err error) {
	startOffset = uint64(termNotEncoded)
	if len(c.final) == 0 {
		return startOffset, nil
	}

	if chw := w.(*countHashWriter); chw != nil {
		startOffset = uint64(chw.Count()) // #nosec G115 -- countHashWriter checks overflow and counts nonnegative writes.
	}

	_, err = c.Write(w)
	return startOffset, err
}

func (c *chunkedIntCoder) FinalSize() int {
	return len(c.final)
}

// modifyLengthsToEndOffsets converts the chunk length array
// to a chunk offset array. The readChunkBoundary
// will figure out the start and end of every chunk from
// these offsets. Starting offset of i'th index is stored
// in i-1'th position except for 0'th index and ending offset
// is stored at i'th index position.
// For 0'th element, starting position is always zero.
// eg:
// Lens ->  5 5 5 5 => 5 10 15 20
// Lens ->  0 5 0 5 => 0 5 5 10
// Lens ->  0 0 0 5 => 0 0 0 5
// Lens ->  5 0 0 0 => 5 5 5 5
// Lens ->  0 5 0 0 => 0 5 5 5
// Lens ->  0 0 5 0 => 0 0 5 5
func modifyLengthsToEndOffsets(lengths []uint64) []uint64 {
	var runningOffset uint64
	var index, i int
	for i = 1; i <= len(lengths); i++ {
		runningOffset += lengths[i-1]
		lengths[index] = runningOffset
		index++
	}
	return lengths
}

func readChunkBoundary(chunk int, offsets []uint64) (start, end uint64) {
	if chunk > 0 {
		start = offsets[chunk-1]
	}
	return start, offsets[chunk]
}
