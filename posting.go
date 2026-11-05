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

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

// FST or vellum value (uint64) encoding is determined by the top two
// highest-order or most significant bits...
//
//	encoding  : MSB
//	name      : 63  62  61...to...bit #0 (LSB)
//	----------+---+---+---------------------------------------------------
//	 general  : 0 | 0 | 62-bits of postingsOffset.
//	 ~        : 0 | 1 | reserved for future.
//	 1-hit    : 1 | 0 | 31-bits of positive float31 norm | 31-bits docNum.
//	 ~        : 1 | 1 | reserved for future.
//
// Encoding "general" is able to handle all cases, where the
// postingsOffset points to more information about the postings for
// the term.
//
// Encoding "1-hit" is used to optimize a commonly seen case when a
// term has only a single hit.  For example, a term in the _id field
// will have only 1 hit.  The "1-hit" encoding is used for a term
// in a field when...
//
// - term vector info is disabled for that field;
// - and, the term appears in only a single doc for that field;
// - and, the term's freq is exactly 1 in that single doc for that field;
// - and, the docNum must fit into 31-bits;
//
// Otherwise, the "general" encoding is used instead.
//
// In the "1-hit" encoding, the field in that single doc may have
// other terms, which is supported in the "1-hit" encoding by the
// positive float31 norm.
const fSTValEncodingMaskRaw = 0xc000000000000000
const fSTValEncodingMask = uint64(fSTValEncodingMaskRaw)
const fSTValEncoding1HitRaw = 0x8000000000000000
const fSTValEncoding1Hit = uint64(fSTValEncoding1HitRaw)

func fSTValEncode1Hit(docNum, normBits uint64) uint64 {
	return fSTValEncoding1Hit | ((mask31Bits & normBits) << 31) | (mask31Bits & docNum)
}

func fSTValDecode1Hit(v uint64) (docNum, normBits uint64) {
	return mask31Bits & v, mask31Bits & (v >> 31)
}

const mask31BitsRaw = 0x000000007fffffff
const mask31Bits = uint64(mask31BitsRaw)

func under32Bits(x uint64) bool {
	return x <= mask31Bits
}

const docNum1HitFinished = math.MaxUint64

// PostingsList is an in-memory representation of a postings list
type PostingsList struct {
	sb             *Segment
	postingsOffset uint64
	freqOffset     uint64
	locOffset      uint64
	offOffset      uint64 // location offsets stream, version 7+
	postings       *roaring.Bitmap
	except         *roaring.Bitmap

	// when normBits1Hit != 0, then this postings list came from a
	// 1-hit encoding, and only the docNum1Hit & normBits1Hit apply
	docNum1Hit   uint64
	normBits1Hit uint64

	chunkSize uint64
}

// represents an immutable, empty postings list
var emptyPostingsList = &PostingsList{}

func (p *PostingsList) Size() int {
	sizeInBytes := reflectStaticSizePostingsList + sizeOfPtr

	if p.except != nil {
		bitmapSize := p.except.GetSizeInBytes()
		// #nosec G115 -- sizeInBytes is only the small static PostingsList and pointer sizes here.
		if bitmapSize > uint64(math.MaxInt-sizeInBytes) {
			return math.MaxInt
		}
		// #nosec G115 -- bitmapSize fits the remaining int-sized result above.
		sizeInBytes += int(bitmapSize)
	}

	return sizeInBytes
}

func (p *PostingsList) OrInto(receiver *roaring.Bitmap) {
	if p.normBits1Hit != 0 {
		// #nosec G115 -- fSTValDecode1Hit masks the list document number to 31 bits.
		receiver.Add(uint32(p.docNum1Hit))
		return
	}

	if p.postings != nil {
		receiver.Or(p.postings)
	}
}

// Iterator returns an iterator for this postings list
func (p *PostingsList) Iterator(includeFreq, includeNorm, includeLocs bool,
	prealloc segment.PostingsIterator) (segment.PostingsIterator, error) {
	if p.normBits1Hit == 0 && p.postings == nil {
		return emptyPostingsIterator, nil
	}

	var preallocPI *PostingsIterator
	pi, ok := prealloc.(*PostingsIterator)
	if ok && pi != nil {
		preallocPI = pi
	}
	if preallocPI == emptyPostingsIterator {
		preallocPI = nil
	}

	return p.iterator(includeFreq, includeNorm, includeLocs, preallocPI)
}

func (p *PostingsList) iterator(includeFreq, includeNorm, includeLocs bool,
	rv *PostingsIterator) (*PostingsIterator, error) {
	if rv == nil {
		rv = &PostingsIterator{}
	} else {
		freqNormReader := rv.freqNormReader
		if freqNormReader != nil {
			freqNormReader.reset()
		}

		locReader := rv.locReader
		if locReader != nil {
			locReader.reset()
		}

		offReader := rv.offReader
		if offReader != nil {
			offReader.reset()
		}

		nextLocs := rv.nextLocs[:0]
		nextSegmentLocs := rv.nextSegmentLocs[:0]

		buf := rv.buf
		offs, splitFields, splitLocs := rv.offs, rv.splitFields[:0], rv.splitLocs[:0]

		*rv = PostingsIterator{} // clear the struct

		rv.freqNormReader = freqNormReader
		rv.locReader = locReader
		rv.offReader = offReader

		rv.nextLocs = nextLocs
		rv.nextSegmentLocs = nextSegmentLocs

		rv.buf = buf
		rv.offs, rv.splitFields, rv.splitLocs = offs, splitFields, splitLocs
	}

	rv.postings = p
	rv.includeFreqNorm = includeFreq || includeNorm || includeLocs
	rv.includeLocs = includeLocs

	if p.normBits1Hit != 0 {
		// "1-hit" encoding
		rv.docNum1Hit = p.docNum1Hit
		rv.normBits1Hit = p.normBits1Hit

		// #nosec G115 -- copied from the 31-bit list document number, before setting the sentinel.
		if p.except != nil && p.except.Contains(uint32(rv.docNum1Hit)) {
			rv.docNum1Hit = docNum1HitFinished
		}

		return rv, nil
	}

	// "general" encoding, check if empty
	if p.postings == nil {
		return rv, nil
	}

	// initialize freq chunk reader
	if rv.includeFreqNorm {
		var err error
		rv.freqNormReader, err = newChunkedIntDecoder(p.sb.data, p.freqOffset, rv.freqNormReader,
			p.sb.footer != nil && p.sb.footer.version >= versionBlockBounds, p.compact())
		if err != nil {
			return nil, err
		}
	}

	// initialize the loc chunk reader
	if rv.includeLocs {
		var err error
		rv.locReader, err = newChunkedIntDecoder(p.sb.data, p.locOffset, rv.locReader, false, p.compact())
		if err != nil {
			return nil, err
		}
	}

	// the roaring iterators are created on first use: multi-term
	// searchers open hundreds of iterators per query only to OR their
	// ActualBM's together
	if p.except != nil {
		rv.ActualBM = roaring.AndNot(p.postings, p.except)
	} else {
		rv.ActualBM = p.postings
	}

	return rv, nil
}

// Count returns the number of items on this postings list
func (p *PostingsList) Count() uint64 {
	var n, e uint64
	if p.normBits1Hit != 0 {
		n = 1
		// #nosec G115 -- fSTValDecode1Hit masks the list document number to 31 bits.
		if p.except != nil && p.except.Contains(uint32(p.docNum1Hit)) {
			e = 1
		}
	} else if p.postings != nil {
		n = p.postings.GetCardinality()
		if p.except != nil {
			e = p.postings.AndCardinality(p.except)
		}
	}
	return n - e
}

// compact reports whether the list's chunks use the version 6+ layout.
func (p *PostingsList) compact() bool {
	return p.sb.footer != nil && p.sb.footer.version >= versionCompactPostings
}

// split reports whether the list keeps location offsets apart (version 7+).
func (p *PostingsList) split() bool {
	return p.sb.footer != nil && p.sb.footer.version >= versionSplitOffsets
}

func (p *PostingsList) read(postingsOffset uint64, d *Dictionary) error {
	p.postingsOffset = postingsOffset

	// handle "1-hit" encoding special case
	if p.postingsOffset&fSTValEncodingMask == fSTValEncoding1Hit {
		return p.init1Hit(postingsOffset)
	}

	// read the location of the freq/norm details
	pos := postingsOffset
	var err error
	p.freqOffset, err = readDataUvarint(d.sb.data, &pos)
	if err != nil {
		return err
	}
	p.locOffset, err = readDataUvarint(d.sb.data, &pos)
	if err != nil {
		return err
	}
	if p.locOffset > 0 && p.freqOffset > 0 {
		if p.locOffset > math.MaxUint64-p.freqOffset {
			return fmt.Errorf("postings location offset overflow")
		}
		p.locOffset += p.freqOffset
	}
	p.offOffset = 0
	if d.sb.footer != nil && d.sb.footer.version >= versionSplitOffsets {
		if p.offOffset, err = readDataUvarint(d.sb.data, &pos); err != nil {
			return err
		}
		if p.offOffset > 0 && p.locOffset > 0 {
			if p.offOffset > math.MaxUint64-p.locOffset {
				return fmt.Errorf("postings offsets offset overflow")
			}
			p.offOffset += p.locOffset
		}
	}
	postingsLen, err := readDataUvarint(d.sb.data, &pos)
	if err != nil {
		return err
	}
	roaringData, err := readDataAt(d.sb.data, pos, postingsLen)
	if err != nil {
		return err
	}
	roaringBytes := roaringData

	if p.postings == nil {
		p.postings = roaring.NewBitmap()
	}
	_, err = p.postings.FromBuffer(roaringBytes)
	if err != nil {
		return fmt.Errorf("error loading roaring bitmap: %v", err)
	}

	p.chunkSize, err = getChunkSize(d.sb.footer.chunkMode,
		p.postings.GetCardinality(), d.sb.footer.numDocs)
	if err != nil {
		return err
	}

	return nil
}

func (p *PostingsList) init1Hit(fstVal uint64) error {
	docNum, normBits := fSTValDecode1Hit(fstVal)

	p.docNum1Hit = docNum
	p.normBits1Hit = normBits

	return nil
}

// PostingsIterator provides a way to iterate through the postings list
type PostingsIterator struct {
	postings *PostingsList
	all      roaring.IntPeekable
	Actual   roaring.IntPeekable
	ActualBM *roaring.Bitmap

	currChunk      uint32
	freqNormReader *chunkedIntDecoder
	locReader      *chunkedIntDecoder

	// version 7+ location offsets: locRec counts the location records
	// read or skipped in the loaded chunk, which numbers the record of the
	// next posting; offReader, set up on first use, sits after record
	// offRec of chunk offChunk when offLoaded
	locRec    uint32
	offReader *chunkedIntDecoder
	offInit   bool
	offLoaded bool
	offChunk  uint32
	offRec    uint32
	offs      []uint64 // NextLocBatch's offsets handles

	splitFields []string // readSplitLocs' record
	splitLocs   []int

	next            Posting            // reused across Next() calls
	nextLocs        []Location         // reused across Next() calls
	nextSegmentLocs []segment.Location // reused across Next() calls

	docNum1Hit   uint64
	normBits1Hit uint64

	buf []byte

	includeFreqNorm bool
	includeLocs     bool
	started         bool // all/Actual iterators created

	// chunkEnd is the first docNum past the loaded freq/norm chunk, so a
	// clean iterator finds a hit's chunk with a compare, not a divide
	chunkEnd uint64
	// lastDoc is the last docNum a clean iterator returned; its freq/norm
	// entry is the last one read when hasLast
	lastDoc uint32
	hasLast bool
	// denseEnd, when denseChecked for the loaded chunk and ActualBM, is
	// the end of the chunk if ActualBM holds each of its documents, else 0
	denseEnd     uint64
	denseChecked bool
}

var emptyPostingsIterator = &PostingsIterator{}

// startIterators creates the roaring iterators deferred by iterator().
func (i *PostingsIterator) startIterators() {
	i.started = true
	i.all = i.postings.postings.Iterator()
	if i.ActualBM == i.postings.postings {
		i.Actual = i.all // Optimize to use same iterator for all & Actual.
	} else {
		i.Actual = i.ActualBM.Iterator()
	}
}

func (i *PostingsIterator) Size() int {
	sizeInBytes := reflectStaticSizePostingsIterator + sizeOfPtr +
		i.next.Size()
	// account for freqNormReader, locReader if we start using this.
	for _, entry := range i.nextLocs {
		sizeInBytes += entry.Size()
	}

	return sizeInBytes
}
func (i *PostingsIterator) Empty() bool {
	return false
}

func (i *PostingsIterator) loadChunk(chunk int) error {
	if chunk < 0 || uint64(chunk) > math.MaxUint32 {
		return fmt.Errorf("posting chunk number out of range: %d", chunk)
	}
	if i.includeFreqNorm {
		err := i.freqNormReader.loadChunk(chunk)
		if err != nil {
			return err
		}
	}

	if i.includeLocs {
		err := i.locReader.loadChunk(chunk)
		if err != nil {
			return err
		}
	}

	i.currChunk = uint32(chunk)
	i.locRec, i.denseChecked = 0, false
	if i.postings != nil {
		i.chunkEnd = (uint64(chunk) + 1) * i.postings.chunkSize
	}
	return nil
}

func (i *PostingsIterator) readFreqNormHasLocs() (freq int, norm uint64, hasLocs bool, err error) {
	if i.normBits1Hit != 0 {
		return 1, i.normBits1Hit, false, nil
	}

	freqHasLocs, err := i.freqNormReader.r.readUvarintFast()
	if err != nil {
		return 0, 0, false, fmt.Errorf("error reading frequency: %v", err)
	}

	freq, hasLocs = decodeFreqHasLocs(freqHasLocs)

	norm, err = i.freqNormReader.readUvarint()
	if err != nil {
		return 0, 0, false, fmt.Errorf("error reading norm: %v", err)
	}

	return freq, norm, hasLocs, nil
}

func (i *PostingsIterator) skipFreqNormReadHasLocs() (bool, error) {
	if i.normBits1Hit != 0 {
		return false, nil
	}

	freqHasLocs, err := i.freqNormReader.readUvarint()
	if err != nil {
		return false, fmt.Errorf("error reading freqHasLocs: %v", err)
	}

	i.freqNormReader.SkipUvarint() // Skip normBits.

	return freqHasLocs&0x01 != 0, nil // See decodeFreqHasLocs() / hasLocs.
}

func encodeFreqHasLocs(freq uint64, hasLocs bool) uint64 {
	rv := freq << 1
	if hasLocs {
		rv |= 0x01 // 0'th LSB encodes whether there are locations
	}
	return rv
}

func decodeFreqHasLocs(freqHasLocs uint64) (int, bool) {
	freq := freqHasLocs >> 1
	hasLocs := freqHasLocs&0x01 != 0
	return int(freq), hasLocs
}

// readLocation processes all the integers on the stream representing a single
// location; prev is the posting's previous location (nil for the first),
// the base of compact (version 6+) deltas.
func (i *PostingsIterator) readLocation(l, prev *Location) error {
	var fieldID, pos, start, end uint64
	if r := i.locReader.r; r.C+4 <= len(r.S) && (r.S[r.C]|r.S[r.C+1]|r.S[r.C+2]|r.S[r.C+3]) < lastByte {
		// common case: all four varints are one byte
		b := r.S[r.C : r.C+4]
		fieldID, pos, start, end = uint64(b[0]), uint64(b[1]), uint64(b[2]), uint64(b[3])
		r.C += 4
	} else if err := i.readLocationSlow(&fieldID, &pos, &start, &end); err != nil {
		return err
	}

	if fieldID >= uint64(len(i.postings.sb.fieldsInv)) {
		return fmt.Errorf("location field %d out of range", fieldID)
	}
	l.field = i.postings.sb.fieldsInv[fieldID]
	if i.postings.compact() {
		var prevPos, prevStart uint64
		if prev != nil {
			prevPos, prevStart = uint64(prev.pos), uint64(prev.start) // #nosec G115 -- validated nonnegative below.
		}
		// #nosec G115 -- inverse of locationVals' two's-complement deltas.
		pos, start = prevPos+uint64(unzigzag(pos)), prevStart+uint64(unzigzag(start))
		end += start
	}
	if pos > math.MaxInt || start > math.MaxInt || end > math.MaxInt || end < start {
		return fmt.Errorf("location exceeds int range")
	}
	l.pos = int(pos)
	l.start = int(start)
	l.end = int(end)

	return nil
}

// readLocationSlow reads the four location varints of any length.
func (i *PostingsIterator) readLocationSlow(fieldID, pos, start, end *uint64) error {
	var err error
	if *fieldID, err = i.locReader.readUvarint(); err != nil {
		return fmt.Errorf("error reading location field: %v", err)
	}
	if *pos, err = i.locReader.readUvarint(); err != nil {
		return fmt.Errorf("error reading location pos: %v", err)
	}
	if *start, err = i.locReader.readUvarint(); err != nil {
		return fmt.Errorf("error reading location start: %v", err)
	}
	if *end, err = i.locReader.readUvarint(); err != nil {
		return fmt.Errorf("error reading location end: %v", err)
	}
	return nil
}

// Next returns the next posting on the postings list, or nil at the end
func (i *PostingsIterator) Next() (segment.Posting, error) {
	// Once a chunk is loaded, sequential reads need neither a seek nor a
	// bitmap rank. Subsets can use this path too when no posting was skipped.
	if i.hasLast && i.Actual != nil {
		if !i.Actual.HasNext() {
			return nil, nil
		}
		n := i.Actual.Next()
		if n <= i.lastDoc || uint64(n) >= i.chunkEnd ||
			(i.postings.postings != i.ActualBM && n != i.lastDoc+1) {
			if err := i.seekFreqNorm(n, i.chunkOf(n)); err != nil {
				return nil, err
			}
		} else {
			i.lastDoc = n
		}
		return i.readPosting(uint64(n))
	}
	return i.nextAtOrAfter(0)
}

// Advance returns the posting at the specified docNum or it is not present
// the next posting, or if the end is reached, nil
func (i *PostingsIterator) Advance(docNum uint64) (segment.Posting, error) {
	return i.nextAtOrAfter(docNum)
}

const locSliceGrowth = 2

// Next returns the next posting on the postings list, or nil at the end
func (i *PostingsIterator) nextAtOrAfter(atOrAfter uint64) (segment.Posting, error) {
	docNum, exists, err := i.nextDocNumAtOrAfter(atOrAfter)
	if err != nil || !exists {
		return nil, err
	}

	return i.readPosting(docNum)
}

//nolint:gocyclo,funlen // hot decode path, kept inline for speed.
func (i *PostingsIterator) readPosting(docNum uint64) (segment.Posting, error) {
	i.next = Posting{} // clear the struct
	rv := &i.next
	rv.docNum = docNum

	if !i.includeFreqNorm {
		return rv, nil
	}

	var normBits uint64
	var hasLocs bool
	var err error

	rv.freq, normBits, hasLocs, err = i.readFreqNormHasLocs()
	if err != nil {
		return nil, err
	}

	if normBits > math.MaxUint32 {
		return nil, fmt.Errorf("norm bits exceed uint32")
	}
	rv.norm = math.Float32frombits(uint32(normBits))

	if i.includeLocs && hasLocs {
		if i.postings.split() {
			if err := i.readSplitLocs(rv); err != nil {
				return nil, err
			}
			return rv, nil
		}
		if rv.freq == 1 && len(i.nextLocs) > 0 && len(i.nextSegmentLocs) > 0 {
			r := i.locReader.r
			if r.C+5 <= len(r.S) && r.S[r.C] == 4 &&
				(r.S[r.C+1]|r.S[r.C+2]|r.S[r.C+3]|r.S[r.C+4]) < lastByte {
				b := r.S[r.C+1 : r.C+5]
				if int(b[0]) >= len(i.postings.sb.fieldsInv) {
					return nil, fmt.Errorf("location field %d out of range", b[0])
				}
				pos, start, end := int(b[1]), int(b[2]), int(b[3])
				if i.postings.compact() {
					// #nosec G115 -- pos and start are one-byte values.
					pos, start = int(unzigzag(uint64(pos))), int(unzigzag(uint64(start)))
					end += start
				}
				if pos < 0 || start < 0 || end < start {
					return nil, fmt.Errorf("location exceeds int range")
				}
				i.nextLocs[0] = Location{field: i.postings.sb.fieldsInv[b[0]], pos: pos, start: start, end: end}
				r.C += 5
				rv.locs = i.nextSegmentLocs[:1]
				return rv, nil
			}
		}
		// prepare locations into reused slices, where we assume
		// rv.freq >= "number of locs", since in a composite field,
		// some component fields might have their IncludeTermVector
		// flags disabled while other component fields are enabled
		if cap(i.nextLocs) >= rv.freq {
			i.nextLocs = i.nextLocs[0:rv.freq]
		} else {
			i.nextLocs = make([]Location, rv.freq, rv.freq*locSliceGrowth)
		}
		if cap(i.nextSegmentLocs) < rv.freq {
			i.nextSegmentLocs = make([]segment.Location, rv.freq, rv.freq*locSliceGrowth)
		}
		rv.locs = i.nextSegmentLocs[:0]

		numLocsBytes, err := i.locReader.readUvarint()
		if err != nil {
			return nil, fmt.Errorf("error reading location numLocsBytes: %v", err)
		}

		j := 0
		startBytesRemaining := i.locReader.Len() // # bytes remaining in the locReader
		// #nosec G115 -- memUvarintReader.Len clamps its result to nonnegative values.
		if numLocsBytes > uint64(startBytesRemaining) {
			return nil, fmt.Errorf("location byte count exceeds chunk")
		}
		// #nosec G115 -- numLocsBytes is bounded by the int-sized remaining chunk length.
		for startBytesRemaining-i.locReader.Len() < int(numLocsBytes) {
			if j >= len(i.nextLocs) {
				return nil, fmt.Errorf("location count exceeds frequency")
			}
			var prev *Location
			if j > 0 {
				prev = &i.nextLocs[j-1]
			}
			err := i.readLocation(&i.nextLocs[j], prev)
			if err != nil {
				return nil, err
			}
			rv.locs = append(rv.locs, &i.nextLocs[j])
			j++
		}
		i.nextSegmentLocs = rv.locs
	}

	return rv, nil
}

// nextDocNum returns the next docNum on the postings list, and also
// sets up the currChunk / loc related fields of the iterator.
func (i *PostingsIterator) nextDocNumAtOrAfter(atOrAfter uint64) (docNum uint64, exists bool, err error) {
	if i.normBits1Hit != 0 {
		docNum, exists = i.nextDocNum1Hit(atOrAfter)
		return docNum, exists, nil
	}

	if atOrAfter > math.MaxUint32 {
		i.Actual = nil
		i.started = true
		return 0, false, nil
	}
	if !i.started {
		if i.ActualBM == nil {
			return 0, false, nil
		}
		i.startIterators()
	}
	if i.Actual == nil || !i.Actual.HasNext() {
		return 0, false, nil
	}

	if i.postings == nil || i.postings.postings == i.ActualBM {
		return i.nextDocNumAtOrAfterClean(uint32(atOrAfter))
	}

	if atOrAfter > 0 {
		i.Actual.AdvanceIfNeeded(uint32(atOrAfter))
		if !i.Actual.HasNext() {
			// couldn't find anything
			return 0, false, nil
		}
	}

	n := i.Actual.Next()
	if i.postings.chunkSize == 0 {
		return 0, false, fmt.Errorf("invalid zero postings chunk size")
	}
	if !i.includeFreqNorm {
		return uint64(n), true, nil
	}
	// Actual is a subset of the full postings (minus deletions, or AND'ed
	// by a conjunction), so the freq/norm entries to pass over are counted
	// on the full postings rather than iterated
	if err := i.seekFreqNorm(n, i.chunkOf(n)); err != nil {
		return 0, false, err
	}
	return uint64(n), true, nil
}

// nextDocNum1Hit consumes the single docNum of a "1-hit" encoded
// postings list, if it is at or after atOrAfter.
func (i *PostingsIterator) nextDocNum1Hit(atOrAfter uint64) (docNum uint64, exists bool) {
	if i.docNum1Hit == docNum1HitFinished {
		return 0, false
	}
	docNum = i.docNum1Hit
	i.docNum1Hit = docNum1HitFinished // consume our 1-hit docNum
	if docNum < atOrAfter {
		// advanced past our 1-hit
		return 0, false
	}
	return docNum, true
}

// rankSkipMinGap is the docNum gap past which a clean Advance skips the
// passed postings by bitmap rank rather than iterating them. Ranking is
// cheaper from gaps of about 16 docs (BenchmarkPostingsAdvanceGap).
const rankSkipMinGap = 16

// optimization when the postings list is "clean" (e.g., no updates &
// no deletions) where the all bitmap is the same as the actual bitmap
func (i *PostingsIterator) nextDocNumAtOrAfterClean(
	atOrAfter uint32) (docNum uint64, exists bool, err error) {
	if !i.includeFreqNorm {
		i.Actual.AdvanceIfNeeded(atOrAfter)

		if !i.Actual.HasNext() {
			return 0, false, nil // couldn't find anything
		}

		return uint64(i.Actual.Next()), true, nil
	}
	if i.postings.chunkSize == 0 {
		return 0, false, fmt.Errorf("invalid zero postings chunk size")
	}
	if atOrAfter > rankSkipMinGap {
		if peek := i.Actual.PeekNext(); atOrAfter > peek && atOrAfter-peek > rankSkipMinGap {
			return i.advanceByRank(atOrAfter)
		}
	}

	// freq-norm's needed, so maintain freq-norm chunk reader
	sameChunkNexts := 0 // # of times we called Next() in the same chunk
	n := i.Actual.Next()
	nChunk := i.chunkOf(n)

	for n < atOrAfter && i.Actual.HasNext() {
		n = i.Actual.Next()

		nChunkPrev := nChunk
		nChunk = i.chunkOf(n)

		if nChunk != nChunkPrev {
			sameChunkNexts = 0
		} else {
			sameChunkNexts++
		}
	}

	if n < atOrAfter {
		// couldn't find anything
		return 0, false, nil
	}

	for j := 0; j < sameChunkNexts; j++ {
		err := i.currChunkNext(nChunk)
		if err != nil {
			return 0, false, fmt.Errorf("error optimized currChunkNext: %v", err)
		}
	}

	if i.currChunk != nChunk || i.freqNormReader.isNil() {
		err := i.loadChunk(int(nChunk))
		if err != nil {
			return 0, false, fmt.Errorf("error loading chunk: %v", err)
		}
	}

	i.lastDoc, i.hasLast = n, true
	return uint64(n), true, nil
}

// chunkOf returns the freq/norm chunk holding docNum n.
func (i *PostingsIterator) chunkOf(n uint32) uint32 {
	cs := i.postings.chunkSize
	if uint64(n) < i.chunkEnd && uint64(n)+cs >= i.chunkEnd {
		return i.currChunk
	}
	// #nosec G115 -- a uint32 document number divided by a positive size still fits uint32.
	return uint32(uint64(n) / cs)
}

// advanceByRank moves a clean iterator to the first posting at or after
// atOrAfter, skipping the freq/norm entries of the postings passed over by
// counting them with bitmap ranks instead of iterating them.
func (i *PostingsIterator) advanceByRank(atOrAfter uint32) (docNum uint64, exists bool, err error) {
	i.Actual.AdvanceIfNeeded(atOrAfter)
	if !i.Actual.HasNext() {
		return 0, false, nil
	}
	n := i.Actual.Next()
	if err := i.seekFreqNorm(n, i.chunkOf(n)); err != nil {
		return 0, false, err
	}
	return uint64(n), true, nil
}

// freqNormVarints is the number of varints in a freq/norm entry.
const freqNormVarints = 2

// seekFreqNorm positions the freq/norm (and location) readers on the entry
// of posting n, in chunk nChunk, skipping the entries of the full postings
// between the last entry read and n, counted by bitmap range cardinality.
func (i *PostingsIterator) seekFreqNorm(n, nChunk uint32) error {
	bm := i.postings.postings
	var skip uint64
	if i.hasLast && i.currChunk == nChunk && !i.freqNormReader.isNil() && i.lastDoc < n {
		// the reader sits just past lastDoc's entry in this chunk
		if n-i.lastDoc > 1 {
			skip = bm.CardinalityInRange(uint64(i.lastDoc)+1, uint64(n))
		}
	} else {
		if err := i.loadChunk(int(nChunk)); err != nil {
			return fmt.Errorf("error loading chunk: %v", err)
		}
		skip = bm.CardinalityInRange(uint64(nChunk)*i.postings.chunkSize, uint64(n))
	}
	if !i.includeLocs {
		// without locations an entry is just two varints, freq and norm
		if err := i.freqNormReader.r.skipUvarints(freqNormVarints * skip); err != nil {
			return err
		}
		skip = 0
	}
	for ; skip > 0; skip-- {
		if err := i.currChunkNext(nChunk); err != nil {
			return fmt.Errorf("error rank currChunkNext: %v", err)
		}
	}
	i.lastDoc, i.hasLast = n, true
	return nil
}

// nextBatchSubset is NextBatch for an Actual bitmap that is a subset of
// the postings (deletions, or a conjunction's ReplaceActual): the freq/norm
// entries of the postings left out are skipped by range cardinality.
func (i *PostingsIterator) nextBatchSubset(docs []uint64, freqs []int, norms []float64) (int, error) {
	it := i.Actual
	r := i.freqNormReader
	n := 0
	for n < len(docs) && it.HasNext() {
		d := it.Next()
		if err := i.seekFreqNorm(d, i.chunkOf(d)); err != nil {
			return n, err
		}
		freqHasLocs, err := r.r.readUvarintFast()
		if err != nil {
			return n, fmt.Errorf("error reading frequency: %v", err)
		}
		normBits, err := r.r.ReadUvarint()
		if err != nil {
			return n, fmt.Errorf("error reading norm: %v", err)
		}
		if normBits > math.MaxUint32 {
			return n, fmt.Errorf("norm bits exceed uint32")
		}
		docs[n] = uint64(d)
		freqs[n] = int(freqHasLocs >> 1)
		norms[n] = float64(math.Float32frombits(uint32(normBits)))
		n++
	}
	return n, nil
}

// nextBatchSlow is NextBatch one Next at a time, for iterators the batch
// paths do not handle.
func (i *PostingsIterator) nextBatchSlow(docs []uint64, freqs []int, norms []float64) (int, error) {
	n := 0
	for n < len(docs) {
		p, err := i.nextAtOrAfter(0)
		if err != nil || p == nil {
			return n, err
		}
		docs[n], freqs[n], norms[n] = i.next.docNum, i.next.freq, float64(i.next.norm)
		n++
	}
	return n, nil
}

// NextBatch reads up to len(docs) postings into docs, freqs and norms,
// which must be the same length, and returns how many it read; 0 means
// the end. It is Next in bulk for iterators without locations.
func (i *PostingsIterator) NextBatch(docs []uint64, freqs []int, norms []float64) (int, error) {
	batchable := i.normBits1Hit == 0 && i.includeFreqNorm && !i.includeLocs &&
		i.postings != nil && i.ActualBM != nil && i.postings.chunkSize > 0
	if !batchable {
		return i.nextBatchSlow(docs, freqs, norms)
	}
	if !i.started {
		i.startIterators()
	}
	if i.Actual == nil {
		return 0, nil
	}
	if i.postings.postings != i.ActualBM {
		return i.nextBatchSubset(docs, freqs, norms)
	}
	it := i.Actual
	r := i.freqNormReader
	n := 0
	for n < len(docs) && it.HasNext() {
		d := it.Next()
		if uint64(d) >= i.chunkEnd || r.isNil() {
			// sequential reads enter a new chunk at its first entry
			// #nosec G115 -- a uint32 document number divided by a positive size fits int.
			if err := i.loadChunk(int(uint64(d) / i.postings.chunkSize)); err != nil {
				return n, fmt.Errorf("error loading chunk: %v", err)
			}
		}
		freqHasLocs, err := r.r.readUvarintFast()
		if err != nil {
			return n, fmt.Errorf("error reading frequency: %v", err)
		}
		normBits, err := r.r.ReadUvarint()
		if err != nil {
			return n, fmt.Errorf("error reading norm: %v", err)
		}
		if normBits > math.MaxUint32 {
			return n, fmt.Errorf("norm bits exceed uint32")
		}
		docs[n] = uint64(d)
		freqs[n] = int(freqHasLocs >> 1)
		norms[n] = float64(math.Float32frombits(uint32(normBits)))
		n++
		i.lastDoc, i.hasLast = d, true
	}
	return n, nil
}

// NoOffsets is the offsets handle NextPosBatch gives a posting whose
// locations already carry their start and end, or that has none.
const NoOffsets = math.MaxUint64

// NextLocBatch is NextBatch with locations: it reads up to len(docs)
// postings into docs, freqs and norms, appends the field of each of their
// locations to fields and its pos, start and end to locs, and sets ends[k]
// to len(fields) after posting k. ends must be as long as docs. It returns
// how many postings it read, 0 meaning the end, and the grown fields/locs.
func (i *PostingsIterator) NextLocBatch(docs []uint64, freqs []int, norms []float64, ends []int,
	fields []string, locs []int) (n int, fieldsOut []string, locsOut []int, err error) {
	if cap(i.offs) < len(docs) {
		i.offs = make([]uint64, len(docs))
	}
	offs := i.offs[:len(docs)]
	lo := len(fields)
	if n, fields, locs, err = i.NextPosBatch(docs, freqs, norms, ends, offs, fields, locs); err != nil {
		return n, fields, locs, err
	}
	for k := range n {
		err = i.LoadOffsets(offs[k], locs[3*lo:3*ends[k]])
		if err != nil {
			return n, fields, locs, err
		}
		lo = ends[k]
	}
	return n, fields, locs, nil
}

// NextPosBatch is NextLocBatch that may leave the start and end of the
// locations of a posting zero: then offs[k], else NoOffsets, is the handle
// that LoadOffsets fills them in with, until the iterator is reused. offs
// must be as long as docs. Version 7+ segments, which keep the offsets
// apart, read them only when asked.
//
//nolint:gocyclo // hot decode path, kept inline for speed.
func (i *PostingsIterator) NextPosBatch(docs []uint64, freqs []int, norms []float64, ends []int, offs []uint64,
	fields []string, locs []int) (n int, fieldsOut []string, locsOut []int, err error) {
	fast := i.includeFreqNorm && i.includeLocs && i.normBits1Hit == 0 && i.postings != nil
	var fieldsInv []string
	var compact, split bool
	if fast {
		fieldsInv, compact, split = i.postings.sb.fieldsInv, i.postings.compact(), i.postings.split()
	}
	for n < len(docs) {
		if !fast || !i.hasLast || i.Actual == nil {
			// first posting, or a list the sequential path does not handle
			var p segment.Posting
			p, err = i.Next()
			if err != nil || p == nil {
				return n, fields, locs, err
			}
			docs[n], freqs[n], norms[n], offs[n] = i.next.docNum, i.next.freq, float64(i.next.norm), NoOffsets
			for _, l := range i.next.locs {
				fields = append(fields, l.Field())
				locs = append(locs, l.Pos(), l.Start(), l.End())
			}
			ends[n] = len(fields)
			n++
			continue
		}
		if !i.Actual.HasNext() {
			break
		}
		d := i.Actual.Next()
		// same sequential-read test as Next
		if d <= i.lastDoc || uint64(d) >= i.chunkEnd ||
			(i.postings.postings != i.ActualBM && d != i.lastDoc+1) {
			err = i.seekFreqNorm(d, i.chunkOf(d))
			if err != nil {
				return n, fields, locs, err
			}
		} else {
			i.lastDoc = d
		}
		fr := i.freqNormReader.r
		freqHasLocs, c, ok := uvarintAt(fr.S, fr.C)
		if !ok {
			return n, fields, locs, fmt.Errorf("error reading frequency")
		}
		normBits, c, ok := uvarintAt(fr.S, c)
		if !ok || normBits > math.MaxUint32 {
			return n, fields, locs, fmt.Errorf("error reading norm")
		}
		fr.C = c
		freq, hasLocs := decodeFreqHasLocs(freqHasLocs)
		docs[n], freqs[n], norms[n], offs[n] = uint64(d), freq, float64(math.Float32frombits(uint32(normBits))), NoOffsets
		if hasLocs && split {
			offs[n] = i.recHandle()
			if fields, locs, err = i.appendSplitLocs(freq, fields, locs); err != nil {
				return n, fields, locs, err
			}
		} else if hasLocs {
			lr := i.locReader.r
			if S, c := lr.S, lr.C; freq == 1 && c+5 <= len(S) && S[c] == 4 &&
				(S[c+1]|S[c+2]|S[c+3]|S[c+4]) < lastByte {
				// one location of four one-byte varints, as readPosting
				f, p, st, e := int(S[c+1]), int(S[c+2]), int(S[c+3]), int(S[c+4])
				if compact {
					// #nosec G115 -- p and st are one-byte values.
					p, st = int(unzigzag(uint64(p))), int(unzigzag(uint64(st)))
					e += st
				}
				if f >= len(fieldsInv) || p < 0 || st < 0 || e < st {
					return n, fields, locs, fmt.Errorf("invalid location")
				}
				fields = append(fields, fieldsInv[f])
				locs = append(locs, p, st, e)
				lr.C = c + 5
			} else if fields, locs, err = i.appendLocs(freq, fields, locs); err != nil {
				return n, fields, locs, err
			}
		}
		ends[n] = len(fields)
		n++
		if split {
			n, fields, locs = i.nextPosRun(n, docs, freqs, norms, ends, offs, fields, locs, fieldsInv)
		} else {
			n, fields, locs = i.nextLocRun(n, docs, freqs, norms, ends, offs, fields, locs, fieldsInv, compact)
		}
	}
	return n, fields, locs, nil
}

// nextLocRun continues NextLocBatch through the postings that follow in
// the loaded chunk, keeping the read offsets in locals. It stops, leaving
// the rest to NextLocBatch, at the chunk end, at a value that is not one
// byte, at a posting with other than one location, and, on a subset list
// (deletions or a conjunction's ReplaceActual), at a skipped posting.
func (i *PostingsIterator) nextLocRun(n int, docs []uint64, freqs []int, norms []float64, ends []int, offs []uint64,
	fields []string, locs []int, fieldsInv []string, compact bool) (nOut int, fieldsOut []string, locsOut []int) {
	it, ok := i.Actual.(*roaring.IntIterator) // concrete calls in the loop below
	if !ok {
		return n, fields, locs
	}
	fr, lr := i.freqNormReader.r, i.locReader.r
	fS, fc, lS, lc := fr.S, fr.C, lr.S, lr.C
	last, chunkEnd := i.lastDoc, i.chunkEnd
	clean := i.postings.postings == i.ActualBM
	for n < len(docs) && it.HasNext() {
		d := it.PeekNext()
		if uint64(d) >= chunkEnd || (!clean && d != last+1) || fc+2 > len(fS) || lc+5 > len(lS) {
			break
		}
		fh, nb := fS[fc], fS[fc+1]
		// freq 1 with locations, one-byte norm, one 4-byte location record
		if fh != 3 || nb >= lastByte || lS[lc] != 4 ||
			(lS[lc+1]|lS[lc+2]|lS[lc+3]|lS[lc+4]) >= lastByte {
			break
		}
		f, p, st, e := int(lS[lc+1]), int(lS[lc+2]), int(lS[lc+3]), int(lS[lc+4])
		if compact {
			// #nosec G115 -- p and st are one-byte values.
			p, st = int(unzigzag(uint64(p))), int(unzigzag(uint64(st)))
			e += st
		}
		if f >= len(fieldsInv) || p < 0 || st < 0 || e < st {
			break // readLocation reports it
		}
		it.Next()
		last = d
		fc, lc = fc+2, lc+5
		docs[n], freqs[n], norms[n], offs[n] = uint64(d), 1, float64(math.Float32frombits(uint32(nb))), NoOffsets
		fields = append(fields, fieldsInv[f])
		locs = append(locs, p, st, e)
		ends[n] = len(fields)
		n++
	}
	fr.C, lr.C, i.lastDoc = fc, lc, last
	return n, fields, locs
}

// nextPosRun is nextLocRun for version 7+ records, stopping at a posting
// with other than one location of a one-byte field and pos: record 2, f, p.
func (i *PostingsIterator) nextPosRun(n int, docs []uint64, freqs []int, norms []float64, ends []int, offs []uint64,
	fields []string, locs []int, fieldsInv []string) (nOut int, fieldsOut []string, locsOut []int) {
	it, ok := i.Actual.(*roaring.IntIterator) // concrete calls in the loop below
	if !ok {
		return n, fields, locs
	}
	fr, lr := i.freqNormReader.r, i.locReader.r
	fS, fc, lS, lc := fr.S, fr.C, lr.S, lr.C
	last, chunkEnd := i.lastDoc, i.chunkEnd
	clean := i.postings.postings == i.ActualBM
	h := i.recHandle()
	if m := i.denseRun(it, len(docs)-n); m >= minDenseRun {
		// docs d0, d0+1, ... d0+m-1 are all postings: read them without
		// stepping the iterator per document
		d0, j := it.PeekNext(), uint32(0)
		for ; j < uint32(m); j++ { // #nosec G115 -- m is at most chunkEnd-d0.
			nb, f, p, ok := posRecord(fS, lS, fc, lc, len(fieldsInv))
			if !ok {
				break
			}
			fc, lc = fc+2, lc+3
			docs[n], freqs[n], norms[n], offs[n] = uint64(d0+j), 1, float64(math.Float32frombits(uint32(nb))), h
			h++
			fields = append(fields, fieldsInv[f])
			locs = append(locs, p, 0, 0)
			ends[n] = len(fields)
			n++
		}
		if j > 0 {
			last = d0 + j - 1
			it.AdvanceIfNeeded(last + 1)
		}
	}
	for n < len(docs) && it.HasNext() {
		d := it.PeekNext()
		if uint64(d) >= chunkEnd || (!clean && d != last+1) {
			break
		}
		nb, f, p, ok := posRecord(fS, lS, fc, lc, len(fieldsInv))
		if !ok {
			break
		}
		it.Next()
		last = d
		fc, lc = fc+2, lc+3
		docs[n], freqs[n], norms[n], offs[n] = uint64(d), 1, float64(math.Float32frombits(uint32(nb))), h
		h++
		fields = append(fields, fieldsInv[f])
		locs = append(locs, p, 0, 0)
		ends[n] = len(fields)
		n++
	}
	fr.C, lr.C, i.lastDoc = fc, lc, last
	i.locRec = uint32(h) // #nosec G115 -- the record number is the low half of the handle.
	return n, fields, locs
}

// posRecord decodes, at fc and lc, the freq/norm entry and version 7+
// location record of a posting with one location, all one-byte values:
// freq 1 with locations, norm nb, then record 2, field f, pos p. ok is
// false for any other posting, which appendSplitLocs reads or reports.
func posRecord(fS, lS []byte, fc, lc, numFields int) (nb byte, f, p int, ok bool) {
	if fc+2 > len(fS) || lc+3 > len(lS) || fS[fc] != 3 || fS[fc+1] >= lastByte || lS[lc] != 2 {
		return 0, 0, 0, false
	}
	// a one-byte zigzag pos is not negative when even
	f, z := int(lS[lc+1]), lS[lc+2]
	return fS[fc+1], f, int(z >> 1), f < min(numFields, lastByte) && z&(lastByte|1) == 0
}

// minDenseRun is the shortest run nextPosRun reads without stepping the
// iterator per document.
const minDenseRun = 16

// denseRun returns m when the next m (at most limit) documents of it, the
// iterator over ActualBM, are consecutive and in the loaded chunk, else 0.
// Whether ActualBM holds every document of the chunk's range is checked
// once per chunk; then so do the postings, and the next document follows
// the last one read.
func (i *PostingsIterator) denseRun(it *roaring.IntIterator, limit int) int {
	if limit < minDenseRun || !it.HasNext() {
		return 0
	}
	if !i.denseChecked {
		i.denseChecked, i.denseEnd = true, 0
		size, bm := i.postings.chunkSize, i.ActualBM
		start, end := i.chunkEnd-min(size, i.chunkEnd), min(i.chunkEnd, uint64(bm.Maximum())+1)
		var below uint64 // postings before start
		if start > 0 {
			below = bm.Rank(uint32(start - 1)) // #nosec G115 -- start is a document number.
		}
		if end > start && bm.Rank(uint32(end-1))-below == end-start { // #nosec G115 -- end-1 is a document number.
			i.denseEnd = end
		}
	}
	d := uint64(it.PeekNext())
	if d >= i.denseEnd {
		return 0
	}
	return int(min(uint64(limit), i.denseEnd-d)) // #nosec G115 -- at most limit.
}

// handleChunkShift places the chunk number in the high half of an offsets
// handle, above the record number.
const handleChunkShift = 32

// recHandle is the offsets handle of the next location record.
func (i *PostingsIterator) recHandle() uint64 {
	return uint64(i.currChunk)<<handleChunkShift | uint64(i.locRec)
}

// appendSplitLocs is appendLocs for a version 7+ record, which holds the
// field and pos of each location: start and end are left zero.
func (i *PostingsIterator) appendSplitLocs(freq int, fields []string, locs []int) (
	fieldsOut []string, locsOut []int, err error) {
	r := i.locReader.r
	numLocsBytes, c, ok := uvarintAt(r.S, r.C)
	if !ok {
		return fields, locs, fmt.Errorf("error reading location numLocsBytes")
	}
	// #nosec G115 -- c is at most len(r.S).
	if numLocsBytes > uint64(len(r.S)-c) {
		return fields, locs, fmt.Errorf("location byte count exceeds chunk")
	}
	end := c + int(numLocsBytes) // #nosec G115 -- bounded by the remaining chunk length above.
	S, fieldsInv := r.S[:end], i.postings.sb.fieldsInv
	var pos uint64
	for j := 0; c < end; j++ {
		if j >= freq {
			return fields, locs, fmt.Errorf("location count exceeds frequency")
		}
		var f, p uint64
		if c+2 <= end && (S[c]|S[c+1]) < lastByte {
			f, p = uint64(S[c]), uint64(S[c+1])
			c += 2
		} else {
			if f, c, ok = uvarintAt(S, c); ok {
				p, c, ok = uvarintAt(S, c)
			}
			if !ok {
				return fields, locs, fmt.Errorf("location record truncated")
			}
		}
		if f >= uint64(len(fieldsInv)) {
			return fields, locs, fmt.Errorf("location field %d out of range", f)
		}
		pos += uint64(unzigzag(p)) // #nosec G115 -- inverse of locationVals' two's-complement deltas.
		if pos > math.MaxInt {
			return fields, locs, fmt.Errorf("location exceeds int range")
		}
		fields = append(fields, fieldsInv[f])
		locs = append(locs, int(pos), 0, 0)
	}
	r.C = end
	i.locRec++
	return fields, locs, nil
}

// readSplitLocs reads the locations of rv from a version 7+ record and,
// right after, its offsets.
func (i *PostingsIterator) readSplitLocs(rv *Posting) error {
	h := i.recHandle()
	var err error
	if i.splitFields, i.splitLocs, err = i.appendSplitLocs(rv.freq, i.splitFields[:0], i.splitLocs[:0]); err != nil {
		return err
	}
	err = i.LoadOffsets(h, i.splitLocs)
	if err != nil {
		return err
	}
	n := len(i.splitFields)
	if cap(i.nextLocs) >= n {
		i.nextLocs = i.nextLocs[:n]
	} else {
		i.nextLocs = make([]Location, n, n*locSliceGrowth)
	}
	if cap(i.nextSegmentLocs) < n {
		i.nextSegmentLocs = make([]segment.Location, 0, n*locSliceGrowth)
	}
	rv.locs = i.nextSegmentLocs[:0]
	for j, f := range i.splitFields {
		l := i.splitLocs[3*j:]
		i.nextLocs[j] = Location{field: f, pos: l[0], start: l[1], end: l[2]}
		rv.locs = append(rv.locs, &i.nextLocs[j])
	}
	i.nextSegmentLocs = rv.locs
	return nil
}

// LoadOffsets sets the start and end of the locations of a posting that
// NextPosBatch returned with offsets handle h, given as that posting's
// (pos, start, end) triples in locs. It does nothing for NoOffsets.
func (i *PostingsIterator) LoadOffsets(h uint64, locs []int) error {
	if h == NoOffsets {
		return nil
	}
	if len(locs)%3 != 0 {
		return fmt.Errorf("locations are not (pos, start, end) triples")
	}
	end, err := i.seekOffsets(h)
	if err != nil {
		return err
	}
	r := i.offReader.r
	var start uint64 // the previous location's, base of the start deltas
	for j := 0; j < len(locs); j += 3 {
		var s, l uint64
		c, ok := r.C, true
		if c+2 <= end && (r.S[c]|r.S[c+1]) < lastByte {
			s, l = uint64(r.S[c]), uint64(r.S[c+1])
			c += 2
		} else if s, c, ok = uvarintAt(r.S[:end], c); ok {
			l, c, ok = uvarintAt(r.S[:end], c)
		}
		start += uint64(unzigzag(s)) // #nosec G115 -- inverse of locationVals' two's-complement deltas.
		if !ok || start > math.MaxInt || l > math.MaxInt-start {
			i.offLoaded = false
			return fmt.Errorf("invalid location offsets")
		}
		r.C = c
		locs[j+1], locs[j+2] = int(start), int(start+l) // #nosec G115 -- checked above.
	}
	if r.C != end {
		i.offLoaded = false
		return fmt.Errorf("location offsets do not match the locations")
	}
	return nil
}

// seekOffsets positions the offsets reader at the values of the offsets
// record of handle h and returns the end of that record.
func (i *PostingsIterator) seekOffsets(h uint64) (end int, err error) {
	if i.postings == nil || !i.postings.split() {
		return 0, fmt.Errorf("postings keep no separate location offsets")
	}
	if !i.offInit {
		i.offReader, err = newChunkedIntDecoder(i.postings.sb.data, i.postings.offOffset, i.offReader, false, true)
		if err != nil {
			return 0, err
		}
		i.offInit = true
	}
	chunk, rec := uint32(h>>handleChunkShift), uint32(h) // #nosec G115 -- a handle packs two uint32 halves.
	if !i.offLoaded || i.offChunk != chunk || i.offRec > rec {
		if err = i.offReader.loadChunk(int(chunk)); err != nil {
			return 0, fmt.Errorf("error loading offsets chunk: %v", err)
		}
		i.offLoaded, i.offChunk, i.offRec = true, chunk, 0
	}
	r := i.offReader.r
	for {
		n, c, ok := uvarintAt(r.S, r.C)
		// #nosec G115 -- c is at most len(r.S) when ok.
		if !ok || n > uint64(len(r.S)-c) {
			i.offLoaded = false
			return 0, fmt.Errorf("location offsets record truncated")
		}
		end = c + int(n) // #nosec G115 -- bounded by the chunk length above.
		if i.offRec == rec {
			r.C = c
			i.offRec++
			return end, nil
		}
		r.C = end
		i.offRec++
	}
}

// uvarintAt decodes the varint at S[c:] and returns it with the offset
// past it; ok is false when it is truncated or overflows.
func uvarintAt(b []byte, c int) (v uint64, next int, ok bool) {
	if c < len(b) && b[c] < lastByte {
		return uint64(b[c]), c + 1, true
	}
	if c >= len(b) {
		return 0, c, false
	}
	v, k := binary.Uvarint(b[c:])
	return v, c + k, k > 0
}

// appendLocs reads the location record of a posting with frequency freq
// and appends its locations to fields and locs, as NextLocBatch.
func (i *PostingsIterator) appendLocs(freq int, fields []string, locs []int) (
	fieldsOut []string, locsOut []int, err error) {
	r := i.locReader.r
	S, c := r.S, r.C
	var numLocsBytes uint64
	if c < len(S) && S[c] < lastByte {
		numLocsBytes = uint64(S[c])
		c++
	} else {
		var err error
		if numLocsBytes, err = i.locReader.readUvarint(); err != nil {
			return fields, locs, fmt.Errorf("error reading location numLocsBytes: %v", err)
		}
		c = r.C
	}
	// #nosec G115 -- c is at most len(S).
	if numLocsBytes > uint64(len(S)-c) {
		return fields, locs, fmt.Errorf("location byte count exceeds chunk")
	}
	end := c + int(numLocsBytes) // #nosec G115 -- bounded by the remaining chunk length above.
	fieldsInv, compact := i.postings.sb.fieldsInv, i.postings.compact()
	var pos, start uint64 // the previous location, base of compact deltas
	for j := 0; c < end; j++ {
		if j >= freq {
			return fields, locs, fmt.Errorf("location count exceeds frequency")
		}
		var f, p, s, e uint64
		if c+4 <= len(S) && (S[c]|S[c+1]|S[c+2]|S[c+3]) < lastByte {
			// common case: all four varints are one byte
			f, p, s, e = uint64(S[c]), uint64(S[c+1]), uint64(S[c+2]), uint64(S[c+3])
			c += 4
		} else {
			r.C = c
			if err := i.readLocationSlow(&f, &p, &s, &e); err != nil {
				return fields, locs, err
			}
			c = r.C
		}
		if f >= uint64(len(fieldsInv)) {
			return fields, locs, fmt.Errorf("location field %d out of range", f)
		}
		if compact {
			// #nosec G115 -- inverse of locationVals' two's-complement deltas.
			p, s = pos+uint64(unzigzag(p)), start+uint64(unzigzag(s))
			e += s
		}
		if p > math.MaxInt || s > math.MaxInt || e > math.MaxInt || e < s {
			return fields, locs, fmt.Errorf("location exceeds int range")
		}
		pos, start = p, s
		fields = append(fields, fieldsInv[f])
		locs = append(locs, int(p), int(s), int(e))
	}
	r.C = c
	return fields, locs, nil
}

func (i *PostingsIterator) currChunkNext(nChunk uint32) error {
	if i.currChunk != nChunk || i.freqNormReader.isNil() {
		err := i.loadChunk(int(nChunk))
		if err != nil {
			return fmt.Errorf("error loading chunk: %v", err)
		}
	}

	// read off freq/offsets even though we don't care about them
	hasLocs, err := i.skipFreqNormReadHasLocs()
	if err != nil {
		return err
	}

	if i.includeLocs && hasLocs {
		numLocsBytes, err := i.locReader.readUvarint()
		if err != nil {
			return fmt.Errorf("error reading location numLocsBytes: %v", err)
		}

		// #nosec G115 -- memUvarintReader.Len clamps its result to nonnegative values.
		if numLocsBytes > uint64(i.locReader.Len()) {
			return fmt.Errorf("location byte count exceeds chunk")
		}
		// skip over all the location bytes
		// #nosec G115 -- numLocsBytes is bounded by the int-sized reader length above.
		i.locReader.SkipBytes(int(numLocsBytes))
		i.locRec++
	}

	return nil
}

// DocNum1Hit returns the docNum and true if this is "1-hit" optimized
// and the docNum is available.
func (i *PostingsIterator) DocNum1Hit() (uint64, bool) {
	if i.normBits1Hit != 0 && i.docNum1Hit != docNum1HitFinished {
		return i.docNum1Hit, true
	}
	return 0, false
}

// ActualBitmap returns the underlying actual bitmap
// which can be used up the stack for optimizations
func (i *PostingsIterator) ActualBitmap() *roaring.Bitmap {
	return i.ActualBM
}

// ReplaceActual replaces the ActualBM with the provided
// bitmap
func (i *PostingsIterator) ReplaceActual(abm *roaring.Bitmap) {
	i.ActualBM = abm
	i.Actual = abm.Iterator()
	i.started, i.denseChecked = true, false
	if i.all == nil && i.postings != nil && i.postings.postings != nil {
		i.all = i.postings.postings.Iterator()
	}
}

func (i *PostingsIterator) Count() uint64 {
	return i.postings.Count()
}

func (i *PostingsIterator) Close() error {
	return nil
}

// Posting is a single entry in a postings list
type Posting struct {
	docNum uint64
	freq   int
	norm   float32
	locs   []segment.Location
}

func (p *Posting) Size() int {
	sizeInBytes := reflectStaticSizePosting

	for _, entry := range p.locs {
		sizeInBytes += entry.Size()
	}

	return sizeInBytes
}

// Number returns the document number of this posting in this segment
func (p *Posting) Number() uint64 {
	return p.docNum
}

// SetNumber sets the document number of this posting
func (p *Posting) SetNumber(n uint64) {
	p.docNum = n
}

// Frequency returns the frequencies of occurrence of this term in this doc/field
func (p *Posting) Frequency() int {
	return p.freq
}

// Norm returns the normalization factor for this posting
func (p *Posting) Norm() float64 {
	return float64(p.norm)
}

// Locations returns the location information for each occurrence
func (p *Posting) Locations() []segment.Location {
	return p.locs
}

// Location represents the location of a single occurrence
type Location struct {
	field string
	pos   int
	start int
	end   int
}

func (l *Location) Size() int {
	return reflectStaticSizeLocation +
		len(l.field)
}

// Field returns the name of the field (useful in composite fields to know
// which original field the value came from)
func (l *Location) Field() string {
	return l.field
}

// Start returns the start byte offset of this occurrence
func (l *Location) Start() int {
	return l.start
}

// End returns the end byte offset of this occurrence
func (l *Location) End() int {
	return l.end
}

// Pos returns the 1-based phrase position of this occurrence
func (l *Location) Pos() int {
	return l.pos
}

// BlockBound returns the largest freq and smallest norm of the freq/norm
// chunk holding docNum, and the first docNum past that chunk. ok is false
// when the segment stores no chunk bounds (before version 5) or the list
// has no freq/norm chunks.
func (i *PostingsIterator) BlockBound(docNum uint64) (maxFreq int, minNorm float64, end uint64, ok bool) {
	if i.normBits1Hit != 0 || !i.includeFreqNorm || i.postings == nil || i.postings.chunkSize == 0 ||
		i.freqNormReader == nil {
		return 0, 0, 0, false
	}
	r, cs := i.freqNormReader, i.postings.chunkSize
	c := docNum / cs
	if c >= uint64(len(r.maxFreqs)) || r.minNorms[c] > math.MaxUint32 || r.maxFreqs[c] > math.MaxInt32 {
		return 0, 0, 0, false
	}
	return int(r.maxFreqs[c]), float64(math.Float32frombits(uint32(r.minNorms[c]))), (c + 1) * cs, true
}

// SkipBlocks skips, from the next posting on, the rest of every freq/norm
// chunk whose bounds competitive rejects, stopping at the first chunk it
// accepts, and returns how many postings it skipped. competitive gets the
// chunk's largest freq and smallest norm. Chunk bounds exist in version 5+
// segments; elsewhere, and on lists with deletions, it skips nothing.
func (i *PostingsIterator) SkipBlocks(competitive func(maxFreq int, minNorm float64) bool) (uint64, error) {
	if i.normBits1Hit != 0 || !i.includeFreqNorm || i.includeLocs || i.postings == nil ||
		i.postings.postings != i.ActualBM || i.postings.chunkSize == 0 ||
		i.freqNormReader == nil || len(i.freqNormReader.maxFreqs) == 0 {
		return 0, nil
	}
	if !i.started {
		i.startIterators()
	}
	if i.Actual == nil {
		return 0, nil
	}
	r, bm, cs := i.freqNormReader, i.postings.postings, i.postings.chunkSize
	var skipped uint64
	for i.Actual.HasNext() {
		d := i.Actual.PeekNext()
		c := uint64(d) / cs
		end := (c + 1) * cs
		if c >= uint64(len(r.maxFreqs)) || end > math.MaxUint32 || r.minNorms[c] > math.MaxUint32 ||
			r.maxFreqs[c] > math.MaxInt32 {
			break
		}
		if competitive(int(r.maxFreqs[c]), float64(math.Float32frombits(uint32(r.minNorms[c])))) {
			break
		}
		n := bm.Rank(uint32(end - 1))
		if d > 0 {
			n -= bm.Rank(d - 1)
		}
		skipped += n
		i.Actual.AdvanceIfNeeded(uint32(end))
		i.hasLast = false
	}
	return skipped, nil
}
