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
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/vellum"
	segment "github.com/vcaesar/bluge_segment_api"
)

var newSegmentBufferNumResultsBump = 0
var newSegmentBufferNumResultsFactor = 1.0
var newSegmentBufferAvgBytesPerDocFactor = 1.0

// New creates an in-memory implementation
// of a segment for the source documents. Large batches are converted by
// several goroutines at once, so the documents must be safe to visit, and
// normCalc to call, concurrently.
func New(results []segment.Document, normCalc func(string, int) float32) (
	segment.Segment, uint64, error) {
	return DefaultOptions().New(results, normCalc)
}

// New is like the package-level New but applies o to the segment.
func (o Options) New(results []segment.Document, normCalc func(string, int) float32) (
	segment.Segment, uint64, error) {
	return newWithChunkMode(results, normCalc, defaultChunkMode, o)
}

func newWithChunkMode(results []segment.Document, normCalc func(string, int) float32,
	chunkMode uint32, opts Options) (segment.Segment, uint64, error) {
	s := interimPool.Get().(*interim)

	s.normCalc = normCalc
	if s.visitTerm == nil {
		s.initVisitors()
	}

	var br bytes.Buffer
	if s.lastNumDocs > 0 {
		// use previous results to initialize the buf with an estimate
		// size, but note that the interim instance comes from a
		// global interimPool, so multiple index instances indexing
		// different docs can lead to low quality estimates
		estimateAvgBytesPerDoc := int(float64(s.lastOutSize/s.lastNumDocs) *
			newSegmentBufferNumResultsFactor)
		estimateNumResults := int(float64(len(results)+newSegmentBufferNumResultsBump) *
			newSegmentBufferAvgBytesPerDocFactor)
		br.Grow(estimateAvgBytesPerDoc * estimateNumResults)
	}

	s.results = results
	s.chunkMode = chunkMode
	s.w = newCountHashWriter(&br)

	var footer *footer
	footer, dictOffsets, storedFieldChunkOffsets, err := s.convert()
	if err != nil {
		return nil, uint64(0), err
	}
	footer.crc = s.w.Sum32()
	footer.chunkMode = chunkMode
	footer.numDocs = uint64(len(results))

	sb, err := initSegmentBase(br.Bytes(), footer,
		s.FieldsMap, s.FieldsInv,
		s.FieldDocs, s.FieldFreqs,
		dictOffsets, storedFieldChunkOffsets, opts)

	if err == nil {
		s.reset()
		s.lastNumDocs = len(results)
		s.lastOutSize = len(br.Bytes())
		// a big batch's buffers would stay pinned by the pool for every
		// later small batch, so only modest interims are reused
		if s.retainedBytes() <= maxPooledInterimBytes {
			interimPool.Put(s)
		}
	}

	return sb, uint64(len(br.Bytes())), err
}

func initSegmentBase(mem []byte, footer *footer,
	fieldsMap map[string]uint16, fieldsInv []string,
	fieldsDocs, fieldsFreqs map[uint16]uint64,
	dictLocs []uint64, storedFieldChunkOffsets []uint64, opts Options) (*Segment, error) {
	sb := &Segment{
		data:                    segment.NewDataBytes(mem),
		footer:                  footer,
		fieldsMap:               fieldsMap,
		fieldsInv:               fieldsInv,
		fieldDocs:               fieldsDocs,
		fieldFreqs:              fieldsFreqs,
		dictLocs:                dictLocs,
		fieldDvReaders:          make(map[uint16]*docValueReader),
		fieldNumCols:            make(map[uint16]*numericColumn),
		fieldFSTs:               make(map[uint16]*vellum.FST),
		storedFieldChunkOffsets: storedFieldChunkOffsets,
	}
	sb.initFieldStats()
	sb.initStoredChunkCache(opts.StoredChunkCacheSize)
	sb.updateSize()

	err := sb.loadDvReaders()
	if err != nil {
		return nil, err
	}

	return sb, nil
}

var interimPool = sync.Pool{New: func() interface{} { return &interim{} }}

// maxPooledInterimBytes bounds the buffer capacity an interim may keep and
// still be returned to interimPool.
var maxPooledInterimBytes = 16 << 20

// interim holds temporary working data used while converting from
// the source operations to an encoded segment
type interim struct {
	results []segment.Document

	// numDocs is the segment's document count and numPostingsLists its
	// number of distinct terms; the encoders read these, not results
	numDocs          int
	numPostingsLists int

	chunkMode uint32

	w *countHashWriter

	// FieldsMap adds 1 to field id to avoid zero value issues
	//  name -> field id + 1
	FieldsMap map[string]uint16

	// FieldsInv is the inverse of FieldsMap
	//  field id -> name
	FieldsInv []string

	// FieldDocs tracks how many documents have at least one value
	// for each field
	FieldDocs map[uint16]uint64

	// FieldFreqs tracks how many total tokens there are in a field
	// across all documents
	FieldFreqs map[uint16]uint64

	// Term dictionaries for each field
	//  field id -> term -> postings list id + 1
	Dicts []map[string]uint64

	// Terms for each field, where terms are sorted ascending
	//  field id -> []term
	DictKeys [][]string

	// Fields whose IncludeDocValues is true
	//  field id -> bool
	IncludeDocValues []bool

	// NumericOnly is true while every doc value seen for the field came
	// from a NumericField; such fields are written as a numeric column.
	//  field id -> bool
	NumericOnly []bool

	// NumericValues holds (docNum, value) pairs in doc order
	//  field id -> values
	NumericValues [][]interimNumeric

	// postings id -> bitmap of docNums; may be nil for singletons without locations
	Postings []*roaring.Bitmap

	// postings id -> freq/norm's, one for each docNum in postings
	FreqNorms        [][]interimFreqNorm
	freqNormsBacking []interimFreqNorm

	// postings id -> locs, one for each freq
	Locs        [][]interimLoc
	locsBacking []interimLoc

	numTermsPerPostingsList []int // key is postings list id
	numLocsPerPostingsList  []int // key is postings list id

	// gather, when set, returns the postings of a postings list id, using
	// w's buffers; Postings, FreqNorms and Locs are then unused (Builder),
	// and Dicts too: field f's lists are its sorted DictKeys from pidBase[f]
	gather  func(pid uint64, w *dictWorker) termIn
	pidBase []uint64
	// beforePlace, when set, runs before writeDicts first writes to w
	// (Builder: the stored fields are written meanwhile)
	beforePlace func() error

	fieldDicts  []fieldDicts // writeDicts state, per field ID
	dictWorkers []*dictWorker

	metaBuf bytes.Buffer

	tmp0 []byte
	tmp1 []byte

	lastNumDocs int
	lastOutSize int

	normCalc func(string, int) float32

	docTimeMin int64
	docTimeMax int64

	// repeatedFields is set by prepareDicts when a field name repeats
	// within a document, which needs the serial rollup path
	repeatedFields bool

	// reusable callback state, see initVisitors
	termVisitor termVisitor
	locCounter  locCounter
	dictPrep    dictPrep
	visitTerm   func(segment.FieldTerm)
	visitLoc    func(segment.Location)
	countLoc    func(segment.Location)
}

// retainedBytes approximates the capacity of the interim's per-posting and
// per-location buffers, the ones that scale with the batch.
func (s *interim) retainedBytes() int {
	n := cap(s.freqNormsBacking)*int(unsafe.Sizeof(interimFreqNorm{})) +
		cap(s.locsBacking)*int(unsafe.Sizeof(interimLoc{})) +
		(cap(s.FreqNorms)+cap(s.Locs))*int(unsafe.Sizeof([]byte(nil))) +
		cap(s.Postings)*int(unsafe.Sizeof((*roaring.Bitmap)(nil))) +
		(cap(s.numTermsPerPostingsList)+cap(s.numLocsPerPostingsList))*int(unsafe.Sizeof(0)) +
		cap(s.tmp0) + cap(s.tmp1) + s.metaBuf.Cap()
	for _, k := range s.DictKeys[:cap(s.DictKeys)] {
		n += cap(k) * int(unsafe.Sizeof(""))
	}
	for _, v := range s.NumericValues[:cap(s.NumericValues)] {
		n += cap(v) * int(unsafe.Sizeof(interimNumeric{}))
	}
	fieldDicts := s.fieldDicts[:cap(s.fieldDicts)]
	for i := range fieldDicts {
		fd := &fieldDicts[i]
		n += fd.post.Cap() + fd.fst.Cap() + fd.dv.Cap() + cap(fd.terms)*int(unsafe.Sizeof(termPostings{}))
	}
	return n
}

func (s *interim) reset() {
	s.results = nil
	s.numDocs, s.numPostingsLists = 0, 0
	s.chunkMode = 0
	s.w = nil
	s.FieldsMap = nil
	s.FieldsInv = nil
	s.termVisitor.dict = nil
	s.termVisitor.locs = nil
	s.termVisitor.err = nil
	s.locCounter.err = nil
	s.dictPrep.err = nil
	s.dictPrep.dict = nil
	s.dictPrep.dictKeys = nil
	s.repeatedFields = false
	for i := range s.Dicts {
		s.Dicts[i] = nil
	}
	s.Dicts = s.Dicts[:0]
	for i := range s.DictKeys {
		s.DictKeys[i] = s.DictKeys[i][:0]
	}
	s.DictKeys = s.DictKeys[:0]
	for i := range s.IncludeDocValues {
		s.IncludeDocValues[i] = false
	}
	s.IncludeDocValues = s.IncludeDocValues[:0]
	s.NumericOnly = s.NumericOnly[:0]
	for i := range s.NumericValues {
		s.NumericValues[i] = s.NumericValues[i][:0]
	}
	s.NumericValues = s.NumericValues[:0]
	for _, idn := range s.Postings {
		if idn != nil {
			idn.Clear()
		}
	}
	s.Postings = s.Postings[:0]
	s.FreqNorms = s.FreqNorms[:0]
	for i := range s.freqNormsBacking {
		s.freqNormsBacking[i] = interimFreqNorm{}
	}
	s.freqNormsBacking = s.freqNormsBacking[:0]
	s.Locs = s.Locs[:0]
	for i := range s.locsBacking {
		s.locsBacking[i] = interimLoc{}
	}
	s.locsBacking = s.locsBacking[:0]
	s.numTermsPerPostingsList = s.numTermsPerPostingsList[:0]
	s.numLocsPerPostingsList = s.numLocsPerPostingsList[:0]
	s.metaBuf.Reset()
	s.tmp0 = s.tmp0[:0]
	s.tmp1 = s.tmp1[:0]
	s.lastNumDocs = 0
	s.lastOutSize = 0

	s.docTimeMin = 0
	s.docTimeMax = 0
}

func (s *interim) grabBuf(size int) []byte {
	buf := s.tmp0
	if cap(buf) < size {
		buf = make([]byte, size)
		s.tmp0 = buf
	}
	return buf[0:size]
}

type interimStoredField struct {
	vals [][]byte
}

// interimFreqNorm and interimLoc hold one entry per posting and per
// location of the whole batch, so they are packed to 16 bytes each.
type interimFreqNorm struct {
	freq    uint32
	norm    float32
	docNum  uint32 // used when the bitmap is nil
	numLocs uint32
}

type interimNumeric struct {
	docNum uint64
	val    int64
}

type interimLoc struct {
	fieldID uint16
	pos     uint32
	start   uint32
	end     uint32
}

var errInterimRange = errors.New("term frequency or location exceeds uint32")

// over32 reports whether a nonnegative v does not fit the interim uint32s.
func over32(v int) bool { return uint64(v) > math.MaxUint32 } // #nosec G115 -- negative v wraps above MaxUint32.

func (s *interim) convert() (f *footer, dictOffsets, storedFieldChunkOffsets []uint64, err error) {
	if uint64(len(s.results)) > uint64(math.MaxUint32)+1 {
		return nil, nil, nil, fmt.Errorf("too many documents for uint32 document numbers")
	}
	s.numDocs = len(s.results)
	s.FieldsMap = map[string]uint16{}
	s.FieldDocs = map[uint16]uint64{}
	s.FieldFreqs = map[uint16]uint64{}

	// FIXME review if this is still necessary
	// YES, integration tests fail when removed
	if _, err = s.getOrDefineField(_idFieldName); err != nil { // _id field is fieldID 0
		return nil, nil, nil, err
	}

	for _, result := range s.results {
		result.EachField(func(field segment.Field) {
			if err == nil {
				_, err = s.getOrDefineField(field.Name())
			}
		})
		if err != nil {
			return nil, nil, nil, err
		}
	}

	sort.Strings(s.FieldsInv[1:]) // keep _id as first field

	for fieldID, fieldName := range s.FieldsInv {
		// #nosec G115 -- getOrDefineField caps FieldsInv at MaxUint16; IDs reserve zero.
		s.FieldsMap[fieldName] = uint16(fieldID + 1)
	}

	if prepareErr := s.prepareDicts(); prepareErr != nil {
		return nil, nil, nil, prepareErr
	}
	s.numPostingsLists = s.dictPrep.pidNext
	// sized after prepareDicts, which defines the fields only named by
	// composite locations
	s.sizeFieldValues()

	for _, dict := range s.DictKeys {
		sort.Strings(dict)
	}

	var storedIndexOffset uint64
	if s.parallelPostings() {
		// the field-parallel postings pass only reads FieldsMap/Dicts and
		// writes postings state, so stored fields (output writer, doc
		// value flags) can be written at the same time
		var storedErr error
		done := make(chan struct{})
		go func() {
			defer close(done)
			storedIndexOffset, storedFieldChunkOffsets, storedErr = s.writeStoredFields()
		}()
		processErr := s.processFieldsParallel(min(runtime.GOMAXPROCS(0), len(s.FieldsInv)))
		<-done
		if joinErr := errors.Join(processErr, storedErr); joinErr != nil {
			return nil, nil, nil, joinErr
		}
	} else {
		if processErr := s.processDocumentsSerial(); processErr != nil {
			return nil, nil, nil, processErr
		}
		storedIndexOffset, storedFieldChunkOffsets, err = s.writeStoredFields()
		if err != nil {
			return nil, nil, nil, err
		}
	}

	var fdvIndexOffset uint64

	if len(s.results) > 0 {
		fdvIndexOffset, dictOffsets, err = s.writeDicts()
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		dictOffsets = make([]uint64, len(s.FieldsInv))
	}

	fieldsIndexOffset, err := persistFields(s.FieldsInv, s.FieldDocs, s.FieldFreqs, s.w, dictOffsets)
	if err != nil {
		return nil, nil, nil, err
	}

	docTimeMin, docTimeMax := s.calcTimestamp()

	return &footer{
		storedIndexOffset: storedIndexOffset,
		fieldsIndexOffset: fieldsIndexOffset,
		docValueOffset:    fdvIndexOffset,
		version:           writeVersion,
		docTimeMin:        uint64(docTimeMin), // #nosec G115 -- preserve signed timestamp bits in the uint64 wire format.
		docTimeMax:        uint64(docTimeMax), // #nosec G115 -- preserve signed timestamp bits in the uint64 wire format.
	}, dictOffsets, storedFieldChunkOffsets, nil
}

// sizeFieldValues sizes the per-field doc value state to FieldsInv.
func (s *interim) sizeFieldValues() {
	if cap(s.IncludeDocValues) >= len(s.FieldsInv) {
		s.IncludeDocValues = s.IncludeDocValues[:len(s.FieldsInv)]
	} else {
		s.IncludeDocValues = make([]bool, len(s.FieldsInv))
	}
	if cap(s.NumericOnly) >= len(s.FieldsInv) {
		s.NumericOnly = s.NumericOnly[:len(s.FieldsInv)]
	} else {
		s.NumericOnly = make([]bool, len(s.FieldsInv))
	}
	for i := range s.NumericOnly {
		s.NumericOnly[i] = true
	}
	if cap(s.NumericValues) >= len(s.FieldsInv) {
		s.NumericValues = s.NumericValues[:len(s.FieldsInv)]
	} else {
		s.NumericValues = make([][]interimNumeric, len(s.FieldsInv))
	}
}

func (s *interim) getOrDefineField(fieldName string) (uint16, error) {
	fieldIDPlus1, exists := s.FieldsMap[fieldName]
	if !exists {
		if len(s.FieldsInv) >= math.MaxUint16 {
			return 0, fmt.Errorf("too many fields for uint16 field IDs")
		}
		// #nosec G115 -- len(FieldsInv) < MaxUint16 above, so len+1 fits the reserved-zero field encoding.
		fieldIDPlus1 = uint16(len(s.FieldsInv) + 1)
		s.FieldsMap[fieldName] = fieldIDPlus1
		s.FieldsInv = append(s.FieldsInv, fieldName)

		s.Dicts = append(s.Dicts, make(map[string]uint64))

		n := len(s.DictKeys)
		if n < cap(s.DictKeys) {
			s.DictKeys = s.DictKeys[:n+1]
			s.DictKeys[n] = s.DictKeys[n][:0]
		} else {
			s.DictKeys = append(s.DictKeys, []string(nil))
		}
	}

	return fieldIDPlus1 - 1, nil
}

// fill Dicts and DictKeys from analysis results
func (s *interim) prepareDicts() error {
	p := &s.dictPrep
	p.pidNext, p.totLocs, p.totTFs, p.err = 0, 0, 0, nil
	clear(p.fieldMark)
	done, err := s.countTermsParallel()
	if err != nil {
		return err
	}
	for docNum, result := range s.results {
		if done {
			break
		}
		p.docMark = docNum + 1
		p.docFields = p.docFields[:0]
		result.EachField(p.visitField)
		// record fields seen by this doc
		for _, k := range p.docFields {
			s.FieldDocs[k]++
		}
		if p.err != nil {
			return p.err
		}
	}
	totTFs, totLocs := p.totTFs, p.totLocs

	numPostingsLists := p.pidNext

	if cap(s.Postings) >= numPostingsLists {
		s.Postings = s.Postings[:numPostingsLists]
	} else {
		postings := make([]*roaring.Bitmap, numPostingsLists)
		copy(postings, s.Postings[:cap(s.Postings)])
		s.Postings = postings
	}
	// Singletons without locations can go directly into the FST.
	for pid, numTerms := range s.numTermsPerPostingsList {
		if s.Postings[pid] == nil && (numTerms > 1 || s.numLocsPerPostingsList[pid] > 0) {
			s.Postings[pid] = roaring.New()
		}
	}

	if cap(s.FreqNorms) >= numPostingsLists {
		s.FreqNorms = s.FreqNorms[:numPostingsLists]
	} else {
		s.FreqNorms = make([][]interimFreqNorm, numPostingsLists)
	}

	if cap(s.freqNormsBacking) >= totTFs {
		s.freqNormsBacking = s.freqNormsBacking[:totTFs]
	} else {
		s.freqNormsBacking = make([]interimFreqNorm, totTFs)
	}

	freqNormsBacking := s.freqNormsBacking
	for pid, numTerms := range s.numTermsPerPostingsList {
		s.FreqNorms[pid] = freqNormsBacking[0:0]
		freqNormsBacking = freqNormsBacking[numTerms:]
	}

	if cap(s.Locs) >= numPostingsLists {
		s.Locs = s.Locs[:numPostingsLists]
	} else {
		s.Locs = make([][]interimLoc, numPostingsLists)
	}

	if cap(s.locsBacking) >= totLocs {
		s.locsBacking = s.locsBacking[:totLocs]
	} else {
		s.locsBacking = make([]interimLoc, totLocs)
	}

	locsBacking := s.locsBacking
	for pid, numLocs := range s.numLocsPerPostingsList {
		s.Locs[pid] = locsBacking[0:0]
		locsBacking = locsBacking[numLocs:]
	}
	return nil
}

// dictPrep is the state of prepareDicts across its EachField/EachTerm
// callbacks, which are bound once in initVisitors so that visiting
// fields and terms allocates nothing.
type dictPrep struct {
	s          *interim
	visitField func(segment.Field)
	visitTerm  func(segment.FieldTerm)

	pidNext, totLocs, totTFs int
	err                      error

	docMark   int      // 1 + the current document's index
	fieldMark []int    // per field ID, the docMark of the last document with it
	docFields []uint16 // the fields of the current document

	dict     map[string]uint64 // the current field's
	dictKeys []string
	numTerms int
}

func (p *dictPrep) field(field segment.Field) {
	s := p.s
	fieldID := s.FieldsMap[field.Name()] - 1

	length := field.Length()
	if length < 0 {
		p.err = fmt.Errorf("negative field length")
		return
	}
	if int(fieldID) >= len(p.fieldMark) {
		p.fieldMark = append(p.fieldMark, make([]int, int(fieldID)+1-len(p.fieldMark))...)
	}
	if p.fieldMark[fieldID] == p.docMark {
		s.repeatedFields = true
	} else {
		p.fieldMark[fieldID] = p.docMark
		p.docFields = append(p.docFields, fieldID)
	}
	s.FieldFreqs[fieldID] += uint64(length)

	p.dict, p.dictKeys, p.numTerms = s.Dicts[fieldID], s.DictKeys[fieldID], 0
	field.EachTerm(p.visitTerm)
	p.totTFs += p.numTerms
	s.DictKeys[fieldID] = p.dictKeys
}

func (p *dictPrep) term(term segment.FieldTerm) {
	s := p.s
	p.numTerms++
	// look up without allocating; only materialize the string for new terms
	pidPlus1, exists := p.dict[string(term.Term())]
	if !exists {
		p.pidNext++
		// #nosec G115 -- pidNext counts entries in the int-sized postings slices, starting at zero.
		pidPlus1 = uint64(p.pidNext)

		termStr := string(term.Term())
		p.dict[termStr] = pidPlus1
		p.dictKeys = append(growDouble(p.dictKeys), termStr)

		s.numTermsPerPostingsList = append(growDouble(s.numTermsPerPostingsList), 0)
		s.numLocsPerPostingsList = append(growDouble(s.numLocsPerPostingsList), 0)
	}

	pid := pidPlus1 - 1

	s.numTermsPerPostingsList[pid]++

	s.locCounter.n = 0
	term.EachLocation(s.countLoc)
	if s.locCounter.err != nil {
		p.err = s.locCounter.err
	}
	s.numLocsPerPostingsList[pid] += s.locCounter.n

	p.totLocs += s.locCounter.n
}

// growDouble makes room for one more element by doubling a full slice:
// append grows large slices by about 1.25x, copying them several times.
func growDouble[S ~[]E, E any](v S) S {
	const minGrowDouble = 64
	if len(v) < cap(v) {
		return v
	}
	return slices.Grow(v, max(cap(v), minGrowDouble))
}

// fieldCount is one field's share of countTermsParallel; the dictionary
// values it assigns are local postings ids until rebased.
type fieldCount struct {
	s        *interim
	visit    func(segment.FieldTerm) // c.term, bound once
	visitLoc func(segment.Location)  // c.loc, bound once
	name     string
	dict     map[string]uint64
	keys     []string
	numTerms []int // per local postings id
	numLocs  []int
	docs     uint64 // documents with the field
	freq     uint64 // total field length
	tfs      int
	lastDoc  int // 1 + index of the last document seen with the field
	docMark  int
	locs     int
	repeated bool
	ghost    bool // a location names an undefined field
	err      error
}

func (c *fieldCount) field(field segment.Field) {
	if c.err != nil || field.Name() != c.name {
		return
	}
	length := field.Length()
	if length < 0 {
		c.err = fmt.Errorf("negative field length")
		return
	}
	if c.lastDoc == c.docMark {
		c.repeated = true
	} else {
		c.lastDoc = c.docMark
		c.docs++
	}
	c.freq += uint64(length)
	field.EachTerm(c.visit)
}

func (c *fieldCount) term(term segment.FieldTerm) {
	c.tfs++
	pidPlus1, exists := c.dict[string(term.Term())]
	if !exists {
		key := string(term.Term())
		c.numTerms = append(c.numTerms, 0)
		c.numLocs = append(c.numLocs, 0)
		pidPlus1 = uint64(len(c.numTerms))
		c.dict[key] = pidPlus1
		c.keys = append(c.keys, key)
	}
	pid := pidPlus1 - 1
	c.numTerms[pid]++
	n := c.locs
	term.EachLocation(c.visitLoc)
	c.numLocs[pid] += c.locs - n
}

func (c *fieldCount) loc(loc segment.Location) {
	c.locs++
	if f := loc.Field(); f != "" {
		if _, ok := c.s.FieldsMap[f]; !ok {
			c.ghost = true
		}
	}
}

// countTermsParallel is the counting pass of prepareDicts with one
// goroutine per field at a time; postings ids come out numbered field by
// field. done is false, with all state untouched, when the batch is too
// small or a composite location names a field it would have to define.
func (s *interim) countTermsParallel() (done bool, err error) {
	workers := min(runtime.GOMAXPROCS(0), len(s.FieldsInv))
	if workers < 2 || len(s.results) < parallelPostingsMinDocs {
		return false, nil
	}
	counts := make([]fieldCount, len(s.FieldsInv))
	for f := range counts {
		counts[f] = fieldCount{s: s, name: s.FieldsInv[f], dict: s.Dicts[f], keys: s.DictKeys[f][:0]}
	}
	run := func(work func(c *fieldCount)) {
		var next atomic.Int64
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				for f := int(next.Add(1) - 1); f < len(counts); f = int(next.Add(1) - 1) {
					work(&counts[f])
				}
			})
		}
		wg.Wait()
	}
	run(func(c *fieldCount) {
		visit := c.field
		c.visit, c.visitLoc = c.term, c.loc
		for docNum, result := range s.results {
			c.docMark = docNum + 1
			result.EachField(visit)
			if c.err != nil || c.ghost {
				return
			}
		}
	})
	for f := range counts {
		if c := &counts[f]; c.err != nil || c.ghost {
			for g := range counts {
				clear(counts[g].dict)
			}
			return false, c.err
		}
	}
	// rebase each field's local postings ids after the fields before it
	p, bases := &s.dictPrep, make([]uint64, len(counts))
	for f := range counts {
		c := &counts[f]
		bases[f] = uint64(p.pidNext) // #nosec G115 -- a nonnegative count of postings lists.
		p.pidNext += len(c.numTerms)
		p.totTFs += c.tfs
		p.totLocs += c.locs
		s.numTermsPerPostingsList = append(s.numTermsPerPostingsList, c.numTerms...)
		s.numLocsPerPostingsList = append(s.numLocsPerPostingsList, c.numLocs...)
		s.DictKeys[f] = c.keys
		s.repeatedFields = s.repeatedFields || c.repeated
		if c.docs > 0 {
			s.FieldDocs[uint16(f)] = c.docs // #nosec G115 -- field IDs are capped at MaxUint16.
		}
		if c.freq > 0 || c.docs > 0 {
			s.FieldFreqs[uint16(f)] = c.freq // #nosec G115 -- see above.
		}
	}
	run(func(c *fieldCount) {
		base := bases[c.s.FieldsMap[c.name]-1]
		if base == 0 {
			return
		}
		for k, v := range c.dict {
			c.dict[k] = v + base
		}
	})
	return true, nil
}

// parallelPostingsMinDocs is the batch size from which convert
// splits the postings pass by field across goroutines.
var parallelPostingsMinDocs = 2048

// parallelPostings reports whether the postings pass runs one field set
// per goroutine: no field repeats within a document, several fields and
// cores, and a batch large enough to pay for the goroutines.
func (s *interim) parallelPostings() bool {
	return !s.repeatedFields && min(runtime.GOMAXPROCS(0), len(s.FieldsInv)) > 1 &&
		len(s.results) >= parallelPostingsMinDocs
}

// processFieldsParallel is processDocumentDirect split by field: each
// field's postings lists are disjoint and, with location fields defined by
// prepareDicts, the shared maps are only read, so workers each own a set of
// fields, balanced by token count. normCalc must be safe for concurrent use.
func (s *interim) processFieldsParallel(workers int) error {
	byWeight := make([]int, len(s.FieldsInv))
	for i := range byWeight {
		byWeight[i] = i
	}
	sort.Slice(byWeight, func(a, b int) bool {
		// #nosec G115 -- field IDs are capped at MaxUint16.
		return s.FieldFreqs[uint16(byWeight[a])] > s.FieldFreqs[uint16(byWeight[b])]
	})
	owner := make([]int, len(s.FieldsInv))
	load := make([]uint64, workers)
	for _, f := range byWeight {
		w := 0
		for k := range load {
			if load[k] < load[w] {
				w = k
			}
		}
		owner[f] = w
		load[w] += s.FieldFreqs[uint16(f)] + 1 // #nosec G115 -- see above.
	}
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tv := &termVisitor{s: s}
			tv.visitLocFn = tv.visitLoc
			visitTerm := tv.visitTerm
			visitField := func(field segment.Field) {
				fieldID := s.FieldsMap[field.Name()] - 1
				if tv.err != nil || owner[fieldID] != w {
					return
				}
				tv.fieldID = fieldID
				tv.dict = s.Dicts[fieldID]
				tv.norm = s.normCalc(s.FieldsInv[fieldID], field.Length())
				field.EachTerm(visitTerm)
			}
			for docNum, result := range s.results {
				tv.docNum = uint64(docNum)
				result.EachField(visitField)
				if tv.err != nil {
					errs[w] = tv.err
					return
				}
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (s *interim) processDocumentsSerial() error {
	numFields := len(s.FieldsInv)
	reuseFieldLens := make([]int, numFields)
	reuseFieldTFs := make([]tokenFrequencies, numFields)
	reuseFieldSeen := make([]bool, numFields)

	for docNum, result := range s.results {
		if !s.repeatedFields {
			// prepareDicts saw no field repeat within a document, so each
			// field's length is its own and no rollup is needed
			if err := s.processDocumentDirect(uint64(docNum), result, nil); err != nil {
				return err
			}
			continue
		}
		for i := 0; i < numFields; i++ { // clear these for reuse
			reuseFieldLens[i] = 0
			reuseFieldTFs[i] = nil
			reuseFieldSeen[i] = false
		}

		if err := s.processDocument(uint64(docNum), result,
			reuseFieldLens, reuseFieldTFs, reuseFieldSeen); err != nil {
			return err
		}
	}
	return nil
}

func (s *interim) processDocument(docNum uint64,
	result segment.Document,
	fieldLens []int, fieldTFs []tokenFrequencies, fieldSeen []bool) error {
	// a field name repeated within one document has to be rolled up into a
	// single posting per term; otherwise the fields can be written directly
	repeated := false
	result.EachField(func(field segment.Field) {
		fieldID := s.FieldsMap[field.Name()] - 1
		fieldLens[fieldID] += field.Length()
		if fieldSeen[fieldID] {
			repeated = true
		}
		fieldSeen[fieldID] = true
	})
	if !repeated {
		return s.processDocumentDirect(docNum, result, fieldLens)
	}
	return s.processDocumentRollup(docNum, result, fieldLens, fieldTFs)
}

// processDocumentRollup merges repeated field values into one tokenFreq per
// term before writing postings.
func (s *interim) processDocumentRollup(docNum uint64,
	result segment.Document,
	fieldLens []int, fieldTFs []tokenFrequencies) error {
	visitField := func(field segment.Field) {
		fieldID := s.FieldsMap[field.Name()] - 1

		if existingFreqs := fieldTFs[fieldID]; existingFreqs == nil {
			fieldTFs[fieldID] = make(map[string]*tokenFreq)
		}

		existingFreqs := fieldTFs[fieldID]
		field.EachTerm(func(term segment.FieldTerm) {
			tfk := string(term.Term())
			existingTf, exists := existingFreqs[tfk]
			if exists {
				term.EachLocation(func(location segment.Location) {
					existingTf.Locations = append(existingTf.Locations,
						&tokenLocation{
							FieldVal:    field.Name(),
							StartVal:    location.Start(),
							EndVal:      location.End(),
							PositionVal: location.Pos(),
						})
				})
				existingTf.frequency += term.Frequency()
			} else {
				newTf := &tokenFreq{
					TermVal:   term.Term(),
					frequency: term.Frequency(),
				}
				term.EachLocation(func(location segment.Location) {
					newTf.Locations = append(newTf.Locations,
						&tokenLocation{
							FieldVal:    location.Field(),
							StartVal:    location.Start(),
							EndVal:      location.End(),
							PositionVal: location.Pos(),
						})
				})
				existingFreqs[tfk] = newTf
			}
		})
	}

	result.EachField(visitField)

	// now that it's been rolled up into fieldTFs, walk that
	for fieldID, tfs := range fieldTFs {
		dict := s.Dicts[fieldID]
		norm := s.normCalc(s.FieldsInv[fieldID], fieldLens[fieldID])

		for term, tf := range tfs {
			pid := dict[term] - 1
			if bs := s.Postings[pid]; bs != nil {
				// #nosec G115 -- convert bounds results to MaxUint32+1; docNum is a results index.
				bs.Add(uint32(docNum))
			}

			frequency := tf.Frequency()
			if frequency < 0 {
				return fmt.Errorf("negative term frequency")
			}
			if over32(frequency) || over32(len(tf.Locations)) {
				return errInterimRange
			}
			s.FreqNorms[pid] = append(s.FreqNorms[pid],
				interimFreqNorm{
					freq: uint32(frequency), // #nosec G115 -- bounded above.
					norm: norm,
					// #nosec G115 -- convert bounds results to MaxUint32+1; docNum is a results index.
					docNum:  uint32(docNum),
					numLocs: uint32(len(tf.Locations)), // #nosec G115 -- bounded above.
				})

			if err := s.appendRollupLocs(pid, fieldID, tf.Locations); err != nil {
				return err
			}
		}
	}
	return nil
}

// appendRollupLocs appends a rolled up term's locations in field fieldID
// to postings list pid.
func (s *interim) appendRollupLocs(pid uint64, fieldID int, tlocs []*tokenLocation) error {
	if len(tlocs) == 0 {
		return nil
	}
	locs := s.Locs[pid]
	for _, loc := range tlocs {
		if loc.PositionVal < 0 || loc.StartVal < 0 || loc.EndVal < 0 {
			return fmt.Errorf("negative term location")
		}
		if over32(loc.PositionVal) || over32(loc.StartVal) || over32(loc.EndVal) {
			return errInterimRange
		}
		// #nosec G115 -- fieldTFs has one entry per field; getOrDefineField caps fields at MaxUint16.
		locf := uint16(fieldID)
		if loc.FieldVal != "" {
			var err error
			locf, err = s.getOrDefineField(loc.FieldVal)
			if err != nil {
				return err
			}
		}
		locs = append(locs, interimLoc{
			fieldID: locf,
			pos:     uint32(loc.PositionVal), // #nosec G115 -- bounded above.
			start:   uint32(loc.StartVal),    // #nosec G115 -- bounded above.
			end:     uint32(loc.EndVal),      // #nosec G115 -- bounded above.
		})
	}
	s.Locs[pid] = locs
	return nil
}

// processDocumentDirect writes postings straight from the document's fields
// without the per-document tokenFreq/tokenLocation rollup, valid only when
// no field name repeats within the document (fieldLens already summed, or
// nil to use each field's own length).
func (s *interim) processDocumentDirect(docNum uint64, result segment.Document, fieldLens []int) error {
	tv := &s.termVisitor
	tv.docNum = docNum
	tv.err = nil
	result.EachField(func(field segment.Field) {
		if tv.err != nil {
			return
		}
		tv.fieldID = s.FieldsMap[field.Name()] - 1
		tv.dict = s.Dicts[tv.fieldID]
		length := field.Length()
		if fieldLens != nil {
			length = fieldLens[tv.fieldID]
		}
		tv.norm = s.normCalc(s.FieldsInv[tv.fieldID], length)
		field.EachTerm(s.visitTerm)
	})
	return tv.err
}

// locCounter and termVisitor hold the state of the EachLocation/EachTerm
// callbacks so the bound callbacks (built once in newInterim) capture no
// per-call variables and cost no allocations per term.
type locCounter struct {
	s   *interim
	n   int
	err error
}

type termVisitor struct {
	s          *interim
	visitLocFn func(segment.Location) // v.visitLoc, bound once
	docNum     uint64
	fieldID    uint16
	dict       map[string]uint64
	norm       float32
	locs       []interimLoc
	numLocs    int
	err        error
}

func (s *interim) initVisitors() {
	s.termVisitor.s = s
	s.termVisitor.visitLocFn = s.termVisitor.visitLoc
	s.locCounter.s = s
	s.dictPrep.s = s
	s.dictPrep.visitField = s.dictPrep.field
	s.dictPrep.visitTerm = s.dictPrep.term
	s.visitTerm = s.termVisitor.visitTerm
	s.visitLoc = s.termVisitor.visitLoc
	s.countLoc = s.locCounter.count
}

// count also defines composite location fields up front, so the postings
// pass only reads FieldsMap and can run one field per goroutine.
func (c *locCounter) count(loc segment.Location) {
	c.n++
	if f := loc.Field(); f != "" && c.err == nil {
		_, c.err = c.s.getOrDefineField(f)
	}
}

func (v *termVisitor) visitTerm(term segment.FieldTerm) {
	if v.err != nil {
		return
	}
	v.err = v.addPosting(v.dict[string(term.Term())]-1, term)
}

func (v *termVisitor) visitLoc(loc segment.Location) {
	if v.err != nil {
		return
	}
	pos, start, end := loc.Pos(), loc.Start(), loc.End()
	if pos < 0 || start < 0 || end < 0 {
		v.err = fmt.Errorf("negative term location")
		return
	}
	if over32(pos) || over32(start) || over32(end) {
		v.err = errInterimRange
		return
	}
	locf := v.fieldID
	if locField := loc.Field(); locField != "" {
		locf, v.err = v.s.getOrDefineField(locField)
		if v.err != nil {
			return
		}
	}
	v.locs = append(v.locs, interimLoc{
		fieldID: locf,
		pos:     uint32(pos),   // #nosec G115 -- bounded above.
		start:   uint32(start), // #nosec G115 -- bounded above.
		end:     uint32(end),   // #nosec G115 -- bounded above.
	})
	v.numLocs++
}

func (v *termVisitor) addPosting(pid uint64, term segment.FieldTerm) error {
	frequency := term.Frequency()
	if frequency < 0 {
		return fmt.Errorf("negative term frequency")
	}
	if over32(frequency) {
		return errInterimRange
	}
	s := v.s
	if bs := s.Postings[pid]; bs != nil {
		// #nosec G115 -- convert bounds results to MaxUint32+1; docNum is a results index.
		bs.Add(uint32(v.docNum))
	}

	v.locs = s.Locs[pid]
	v.numLocs = 0
	term.EachLocation(v.visitLocFn)
	if v.err != nil {
		return v.err
	}
	s.Locs[pid] = v.locs
	v.locs = nil
	if over32(v.numLocs) {
		return errInterimRange
	}
	s.FreqNorms[pid] = append(s.FreqNorms[pid], interimFreqNorm{
		freq: uint32(frequency), // #nosec G115 -- bounded above.
		norm: v.norm,
		// #nosec G115 -- convert bounds results to MaxUint32+1; docNum is a results index.
		docNum:  uint32(v.docNum),
		numLocs: uint32(v.numLocs), // #nosec G115 -- bounded above.
	})
	return nil
}

func (s *interim) writeStoredFields() (
	storedIndexOffset uint64, storedFieldChunkOffsets []uint64, err error) {
	varBuf := make([]byte, binary.MaxVarintLen64)
	metaEncode := func(val uint64) (int, error) {
		wb := binary.PutUvarint(varBuf, val)
		return s.metaBuf.Write(varBuf[:wb])
	}

	data, compressed := s.tmp0[:0], s.tmp1[:0]
	defer func() { s.tmp0, s.tmp1 = data, compressed }()

	// keyed by docNum
	docStoredOffsets := make([]uint64, len(s.results))

	// keyed by fieldID, the stored values of the current doc in the loop
	docStoredFields := make([]interimStoredField, len(s.FieldsInv))

	// document chunk coder
	docChunkCoder := newChunkedDocumentCoder(uint64(defaultDocumentChunkSize), s.w)

	var docNum int
	visitField := func(field segment.Field) {
		fieldID := s.FieldsMap[field.Name()] - 1

		if field.Store() {
			isf := &docStoredFields[fieldID]
			isf.vals = append(isf.vals, field.Value())
		}

		if field.IndexDocValues() {
			s.IncludeDocValues[fieldID] = true
			if nf, ok := field.(NumericField); ok {
				if v, ok := nf.NumericValue(); ok {
					s.NumericValues[fieldID] = append(s.NumericValues[fieldID],
						interimNumeric{docNum: uint64(docNum), val: v}) // #nosec G115 -- docNum is a nonnegative slice index.
				} else {
					s.NumericOnly[fieldID] = false
				}
			} else {
				s.NumericOnly[fieldID] = false
			}
		}
	}
	for docNum = range s.results {
		for i := range docStoredFields { // reset for next doc
			docStoredFields[i].vals = docStoredFields[i].vals[:0]
		}
		s.results[docNum].EachField(visitField)

		var curr int

		s.metaBuf.Reset()
		data = data[:0]

		// handle fields
		for fieldID := 0; fieldID < len(s.FieldsInv); fieldID++ {
			if isf := docStoredFields[fieldID]; len(isf.vals) > 0 {
				curr, data, err = encodeStoredFieldValues(
					fieldID, isf.vals,
					curr, metaEncode, data)
				if err != nil {
					return 0, nil, err
				}
			}
		}

		metaBytes := s.metaBuf.Bytes()
		docStoredOffsets[docNum] = docChunkCoder.Size()
		_, err = docChunkCoder.Add(uint64(docNum), metaBytes, data)
		if err != nil {
			return 0, nil, err
		}
	}

	// document chunk coder
	err = docChunkCoder.Write()
	if err != nil {
		return 0, nil, err
	}
	storedFieldChunkOffsets = docChunkCoder.Offsets()

	storedIndexOffset = uint64(s.w.Count()) // #nosec G115 -- countHashWriter checks overflow and counts nonnegative writes.

	for _, docStoredOffset := range docStoredOffsets {
		err = binary.Write(s.w, binary.BigEndian, docStoredOffset)
		if err != nil {
			return 0, nil, err
		}
	}

	return storedIndexOffset, storedFieldChunkOffsets, nil
}

// fieldDicts is one field's share of writeDicts: its postings, encoded
// without absolute offsets, then its FST and doc values.
type fieldDicts struct {
	post           bytes.Buffer   // per term with postings: tf, loc and offsets chunks, roaring with length
	terms          []termPostings // one per DictKeys entry
	fst            bytes.Buffer
	dv             bytes.Buffer
	dvStart, dvEnd uint64 // relative to dv, or fieldNotUninverted
}

// termPostings is a term's dictionary value (its 1-hit value, or the
// postings offset once placed) and the lengths of its parts in post.
type termPostings struct {
	val                          uint64
	tfLen, locLen, offLen, bmLen int
}

// dictWorker is the per-goroutine state of writeDicts.
type dictWorker struct {
	tf, loc     *chunkedIntCoder
	off         *chunkedIntCoder // location offsets, nil before version 7
	numeric     *numericColumnWriter
	docTerms    [][]byte
	small, bulk *vellum.Builder // reused FST builders, see buildFST

	// one term's postings, see interim.gather
	fns  []interimFreqNorm
	locs []interimLoc
	bm   *roaring.Bitmap
}

// termIn is the postings of one term: its bitmap, nil for a singleton
// without locations, and its freq/norms and locations in document order.
type termIn struct {
	bm   *roaring.Bitmap
	fns  []interimFreqNorm
	locs []interimLoc
}

// reuseIntCoder returns c reset, keeping its buffers, or a new coder when
// c is nil, for doc numbers up to maxDocNum.
func reuseIntCoder(c *chunkedIntCoder, maxDocNum uint64) *chunkedIntCoder {
	if c == nil {
		return newChunkedIntCoder(uint64(legacyChunkMode), maxDocNum)
	}
	c.Reset()
	c.SetChunkSize(uint64(legacyChunkMode), maxDocNum)
	return c
}

// writeDicts writes all fields' postings, then each field's FST and doc
// values. Encoding the postings and building the FSTs are independent per
// field and run one field per goroutine for large batches; only placing
// the bytes, which fixes the absolute offsets, is serial.
//
//nolint:gocyclo,funlen // linear setup and write steps; splitting adds no clarity.
func (s *interim) writeDicts() (fdvIndexOffset uint64, dictOffsets []uint64, err error) {
	numFields := len(s.FieldsInv)
	dictOffsets = make([]uint64, numFields)
	fdvOffsetsStart := make([]uint64, numFields)
	fdvOffsetsEnd := make([]uint64, numFields)

	if cap(s.fieldDicts) < numFields {
		s.fieldDicts = append(s.fieldDicts[:cap(s.fieldDicts)], make([]fieldDicts, numFields-cap(s.fieldDicts))...)
	}
	s.fieldDicts = s.fieldDicts[:numFields]

	workers := 1
	if s.numDocs >= parallelPostingsMinDocs {
		workers = max(min(runtime.GOMAXPROCS(0), numFields), 1)
	}
	for len(s.dictWorkers) < workers {
		s.dictWorkers = append(s.dictWorkers, &dictWorker{})
	}
	for _, w := range s.dictWorkers[:workers] {
		// #nosec G115 -- convert calls writeDicts only for nonempty results.
		maxDocNum := uint64(s.numDocs - 1)
		// the coders of a pooled interim are reused with their buffers
		if bounds := writeVersion >= versionBlockBounds; w.tf == nil || w.tf.bounds != bounds {
			w.tf = newChunkedIntCoder(uint64(legacyChunkMode), maxDocNum)
			if bounds {
				w.tf.trackBounds()
			}
		} else {
			w.tf = reuseIntCoder(w.tf, maxDocNum)
		}
		w.loc = reuseIntCoder(w.loc, maxDocNum)
		w.tf.compact = writeVersion >= versionCompactPostings
		w.loc.compact = w.tf.compact
		if writeVersion >= versionSplitOffsets {
			w.off = reuseIntCoder(w.off, maxDocNum)
			w.off.compact = true
		} else {
			w.off = nil
		}
	}

	err = s.eachField(workers, s.encodeField)
	if err != nil {
		return 0, nil, err
	}
	if s.beforePlace != nil {
		err = s.beforePlace()
		if err != nil {
			return 0, nil, err
		}
	}
	for f := range s.fieldDicts {
		err = s.placePostings(&s.fieldDicts[f])
		if err != nil {
			return 0, nil, err
		}
	}
	err = s.eachField(workers, s.buildFST)
	if err != nil {
		return 0, nil, err
	}

	buf := s.grabBuf(binary.MaxVarintLen64)
	for f := range s.fieldDicts {
		fd := &s.fieldDicts[f]
		dictOffsets[f] = uint64(s.w.Count())              // #nosec G115 -- countHashWriter checks overflow and counts nonnegative writes.
		n := binary.PutUvarint(buf, uint64(fd.fst.Len())) // #nosec G115 -- buffer length is nonnegative.
		if _, err = s.w.Write(buf[:n]); err != nil {
			return 0, nil, err
		}
		if _, err = s.w.Write(fd.fst.Bytes()); err != nil {
			return 0, nil, err
		}
		if fd.dvStart == fieldNotUninverted {
			fdvOffsetsStart[f], fdvOffsetsEnd[f] = fieldNotUninverted, fieldNotUninverted
			continue
		}
		base := uint64(s.w.Count()) // #nosec G115 -- see above.
		if _, err = s.w.Write(fd.dv.Bytes()); err != nil {
			return 0, nil, err
		}
		fdvOffsetsStart[f], fdvOffsetsEnd[f] = base+fd.dvStart, base+fd.dvEnd
	}

	fdvIndexOffset = uint64(s.w.Count()) // #nosec G115 -- countHashWriter checks overflow and counts nonnegative writes.

	for i := 0; i < len(fdvOffsetsStart); i++ {
		kind := dvKindTerms
		if s.IncludeDocValues[i] && s.NumericOnly[i] {
			kind = dvKindNumeric
		}
		n := binary.PutUvarint(buf, kind)
		_, err := s.w.Write(buf[:n])
		if err != nil {
			return 0, nil, err
		}
		n = binary.PutUvarint(buf, fdvOffsetsStart[i])
		_, err = s.w.Write(buf[:n])
		if err != nil {
			return 0, nil, err
		}
		n = binary.PutUvarint(buf, fdvOffsetsEnd[i])
		_, err = s.w.Write(buf[:n])
		if err != nil {
			return 0, nil, err
		}
	}

	return fdvIndexOffset, dictOffsets, nil
}

// eachField runs work for every field, with workers goroutines taking the
// fields heaviest first; one worker runs them in order on this goroutine.
func (s *interim) eachField(workers int, work func(w *dictWorker, fieldID int) error) error {
	if workers == 1 {
		for f := range s.FieldsInv {
			if err := work(s.dictWorkers[0], f); err != nil {
				return err
			}
		}
		return nil
	}
	order := make([]int, len(s.FieldsInv))
	weight := func(f int) int {
		return len(s.DictKeys[f]) + int(s.FieldFreqs[uint16(f)]) // #nosec G115 G115 -- field IDs are capped at MaxUint16; counts fit int.
	}
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return weight(order[a]) > weight(order[b]) })
	errs := make([]error, len(order))
	var next atomic.Int64
	var wg sync.WaitGroup
	for _, w := range s.dictWorkers[:workers] {
		wg.Go(func() {
			for i := int(next.Add(1) - 1); i < len(order); i = int(next.Add(1) - 1) {
				errs[i] = work(w, order[i])
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// encodeField encodes the postings and doc values of fieldID into its
// fieldDicts.
func (s *interim) encodeField(w *dictWorker, fieldID int) error {
	fd := &s.fieldDicts[fieldID]
	fd.post.Reset()
	fd.dv.Reset()
	fd.terms = fd.terms[:0]

	var docTerms [][]byte
	if s.IncludeDocValues[fieldID] && !s.NumericOnly[fieldID] {
		if cap(w.docTerms) < s.numDocs {
			w.docTerms = make([][]byte, s.numDocs)
		}
		w.docTerms = w.docTerms[:s.numDocs]
		for docNum := range w.docTerms {
			w.docTerms[docNum] = w.docTerms[docNum][:0]
		}
		docTerms = w.docTerms
	}

	dict := s.Dicts[fieldID]
	fd.terms = slices.Grow(fd.terms, len(s.DictKeys[fieldID]))
	for i, term := range s.DictKeys[fieldID] { // terms are already sorted
		var pid uint64
		var in termIn
		if s.gather != nil {
			pid = s.pidBase[fieldID] + uint64(i) // #nosec G115 -- a rank.
			in = s.gather(pid, w)
		} else {
			pid = dict[term] - 1
			in = termIn{s.Postings[pid], s.FreqNorms[pid], s.Locs[pid]}
		}
		tp, err := s.appendTermPostings(&fd.post, docTerms, term, pid, in, w.tf, w.loc, w.off)
		if err != nil {
			return err
		}
		fd.terms = append(fd.terms, tp)
	}

	var err error
	fd.dvStart, fd.dvEnd, err = s.writeFieldDocValues(newCountHashWriter(&fd.dv), fieldID, docTerms, &w.numeric)
	return err
}

// appendTermPostings encodes the postings in of term, postings list pid,
// to post and returns its termPostings; a 1-hit term appends nothing.
func (s *interim) appendTermPostings(post *bytes.Buffer, docTermMap [][]byte, term string, pid uint64, in termIn,
	tfEncoder, locEncoder, offEncoder *chunkedIntCoder) (tp termPostings, err error) {
	val, oneHit, postingsBS, err := s.encodeTermPostings(docTermMap, term, pid, in, tfEncoder, locEncoder, offEncoder)
	if err != nil || oneHit {
		return termPostings{val: val}, err
	}
	if len(tfEncoder.final) > 0 {
		if tp.tfLen, err = tfEncoder.Write(post); err != nil {
			return tp, err
		}
	}
	if len(locEncoder.final) > 0 {
		if tp.locLen, err = locEncoder.Write(post); err != nil {
			return tp, err
		}
	}
	if offEncoder != nil && len(offEncoder.final) > 0 {
		if tp.offLen, err = offEncoder.Write(post); err != nil {
			return tp, err
		}
	}
	start := post.Len()
	postingsBS.RunOptimize()
	post.Write(binary.AppendUvarint(post.AvailableBuffer(), postingsBS.GetSerializedSizeInBytes()))
	if _, err = postingsBS.WriteTo(post); err != nil {
		return tp, err
	}
	tp.bmLen = post.Len() - start
	tfEncoder.Reset()
	locEncoder.Reset()
	if offEncoder != nil {
		offEncoder.Reset()
	}
	return tp, nil
}

// placePostings writes fd's postings to s.w as writePostings lays them
// out and sets each term's dictionary value to its postings offset.
func (s *interim) placePostings(fd *fieldDicts) error {
	p := fd.post.Bytes()
	split := writeVersion >= versionSplitOffsets
	var header [3 * binary.MaxVarintLen64]byte
	for i := range fd.terms {
		tp := &fd.terms[i]
		if tp.bmLen == 0 { // 1-hit
			continue
		}
		base := uint64(s.w.Count()) // #nosec G115 -- countHashWriter checks overflow and counts nonnegative writes.
		var tfOffset, locOffset, offOffset uint64
		if tp.tfLen > 0 {
			tfOffset = base
		}
		if tp.locLen > 0 {
			locOffset = base + uint64(tp.tfLen) // #nosec G115 -- a nonnegative length.
		}
		if tp.offLen > 0 {
			offOffset = base + uint64(tp.tfLen+tp.locLen) // #nosec G115 -- nonnegative lengths.
		}
		chunks := tp.tfLen + tp.locLen + tp.offLen
		if _, err := s.w.Write(p[:chunks]); err != nil {
			return err
		}
		p = p[chunks:]
		tp.val = uint64(s.w.Count()) // #nosec G115 -- see above.
		h := appendPostingsHeader(header[:0], tfOffset, locOffset, offOffset, split)
		if _, err := s.w.Write(h); err != nil {
			return err
		}
		if _, err := s.w.Write(p[:tp.bmLen]); err != nil {
			return err
		}
		p = p[tp.bmLen:]
	}
	return nil
}

// smallSegmentTerms is the number of distinct terms up to which a segment's
// FSTs are built with smallFSTOpts.
const smallSegmentTerms = 1024

var smallFSTOpts = &vellum.BuilderOpts{Encoder: 1, RegistryTableSize: 512, RegistryMRUSize: 2}

// buildFST builds the FST of fieldID into its fieldDicts with w's builder
// for this segment's size; vellum resets the whole node registry per FST,
// which dominates small segments, so those use a small registry.
func (s *interim) buildFST(w *dictWorker, fieldID int) error {
	fd := &s.fieldDicts[fieldID]
	fd.fst.Reset()
	b, opts := &w.bulk, (*vellum.BuilderOpts)(nil)
	if s.numPostingsLists <= smallSegmentTerms {
		b, opts = &w.small, smallFSTOpts
	}
	var err error
	if *b == nil {
		*b, err = vellum.New(&fd.fst, opts)
	} else {
		err = (*b).Reset(&fd.fst)
	}
	if err != nil {
		return err
	}
	for i, term := range s.DictKeys[fieldID] {
		err = (*b).Insert([]byte(term), fd.terms[i].val)
		if err != nil {
			return err
		}
	}
	return (*b).Close()
}

// writeFieldDocValues writes the doc values of fieldID to w and returns
// their offsets in w, fieldNotUninverted for both when it has none.
// docTermMap holds each document's terms of the field, numericWriter is
// reused (and created when nil) for numeric columns.
func (s *interim) writeFieldDocValues(w *countHashWriter, fieldID int, docTermMap [][]byte,
	numericWriter **numericColumnWriter) (start, end uint64, err error) {
	if s.IncludeDocValues[fieldID] && s.NumericOnly[fieldID] {
		numDocs := uint64(s.numDocs) // #nosec G115 -- a nonnegative count.
		if *numericWriter == nil {
			*numericWriter = newNumericColumnWriter(numDocs)
		} else {
			(*numericWriter).Reset(numDocs)
		}
		nw := *numericWriter
		for _, nv := range s.NumericValues[fieldID] {
			err = nw.Add(nv.docNum, nv.val)
			if err != nil {
				return 0, 0, err
			}
		}
		start = uint64(w.Count()) // #nosec G115 -- countHashWriter checks overflow and counts nonnegative writes.
		if _, err = nw.Write(w); err != nil {
			return 0, 0, err
		}
		return start, uint64(w.Count()), nil // #nosec G115 -- see above.
	}
	if !s.IncludeDocValues[fieldID] {
		return fieldNotUninverted, fieldNotUninverted, nil
	}
	// NOTE: term doc values continue to use legacy chunk mode
	chunkSize, err := getChunkSize(legacyChunkMode, 0, 0)
	if err != nil {
		return 0, 0, err
	}
	// #nosec G115 -- called by writeDicts only for nonempty results.
	fdvEncoder := newChunkedContentCoder(chunkSize, uint64(s.numDocs-1), w, false)
	for docNum, docTerms := range docTermMap {
		if len(docTerms) > 0 {
			err = fdvEncoder.Add(uint64(docNum), docTerms)
			if err != nil {
				return 0, 0, err
			}
		}
	}
	err = fdvEncoder.Close()
	if err != nil {
		return 0, 0, err
	}
	start = uint64(w.Count()) // #nosec G115 -- countHashWriter checks overflow and counts nonnegative writes.
	if _, err = fdvEncoder.Write(); err != nil {
		return 0, 0, err
	}
	return start, uint64(w.Count()), nil // #nosec G115 -- see above.
}

// encodeTermPostings encodes the postings of term into the closed
// tfEncoder, locEncoder and offEncoder (nil before version 7) and returns
// its bitmap, or, when oneHit, its "1-hit" dictionary value with the
// coders untouched.
//
//nolint:gocyclo,funlen // hot encode path, kept inline for speed.
func (s *interim) encodeTermPostings(docTermMap [][]byte, term string, pid uint64, in termIn,
	tfEncoder, locEncoder, offEncoder *chunkedIntCoder) (val uint64, oneHit bool, postingsBS *roaring.Bitmap, err error) {
	postingsBS = in.bm

	freqNorms := in.fns
	freqNormOffset := 0

	locs := in.locs
	locOffset := 0

	card, docNum := uint64(1), uint64(freqNorms[0].docNum)
	if postingsBS != nil {
		card = postingsBS.GetCardinality()
		if card == 1 {
			docNum = uint64(postingsBS.Minimum())
		}
	}
	// same "1-hit" encoding as merge: one doc, freq 1, no locations
	if card == 1 && freqNorms[0].freq == 1 && freqNorms[0].numLocs == 0 {
		normBits := uint64(math.Float32bits(freqNorms[0].norm))
		if under32Bits(docNum) && normBits != 0 && under32Bits(normBits) {
			if docTermMap != nil {
				docTermMap[docNum] = append(append(docTermMap[docNum], term...), termSeparator)
			}
			return fSTValEncode1Hit(docNum, normBits), true, nil, nil
		}
	}

	if postingsBS == nil {
		postingsBS = roaring.New()
		postingsBS.Add(freqNorms[0].docNum)
		if s.gather == nil {
			s.Postings[pid] = postingsBS // kept for reuse, see prepareDicts
		}
	}

	chunkSize, err := getChunkSize(s.chunkMode, card, uint64(s.numDocs)) // #nosec G115 -- a nonnegative count.
	if err != nil {
		return 0, false, nil, err
	}
	// #nosec G115 -- called by writeDicts only for nonempty results.
	tfEncoder.SetChunkSize(chunkSize, uint64(s.numDocs-1))
	// #nosec G115 -- called by writeDicts only for nonempty results.
	locEncoder.SetChunkSize(chunkSize, uint64(s.numDocs-1))
	if offEncoder != nil {
		// #nosec G115 -- called by writeDicts only for nonempty results.
		offEncoder.SetChunkSize(chunkSize, uint64(s.numDocs-1))
	}

	postingsItr := postingsBS.Iterator()
	for postingsItr.HasNext() {
		docNum := uint64(postingsItr.Next())

		freqNorm := freqNorms[freqNormOffset]

		err = tfEncoder.Add(docNum,
			encodeFreqHasLocs(uint64(freqNorm.freq), freqNorm.numLocs > 0),
			uint64(math.Float32bits(freqNorm.norm)))
		if err != nil {
			return 0, false, nil, err
		}

		if freqNorm.numLocs > 0 {
			var prevPos, prevStart uint64
			for _, loc := range locs[locOffset : locOffset+int(freqNorm.numLocs)] {
				pos, start := uint64(loc.pos), uint64(loc.start)
				recordLocation(locEncoder, offEncoder,
					uint64(loc.fieldID), pos, start, uint64(loc.end), prevPos, prevStart)
				prevPos, prevStart = pos, start
			}
			err = addLocationRecords(docNum, locEncoder, offEncoder)
			if err != nil {
				return 0, false, nil, err
			}

			locOffset += int(freqNorm.numLocs)
		}

		freqNormOffset++

		if docTermMap != nil {
			docTermMap[docNum] = append(
				append(docTermMap[docNum], term...),
				termSeparator)
		}
	}

	if closeErr := tfEncoder.Close(); closeErr != nil {
		return 0, false, nil, closeErr
	}
	if closeErr := locEncoder.Close(); closeErr != nil {
		return 0, false, nil, closeErr
	}
	if offEncoder != nil {
		if closeErr := offEncoder.Close(); closeErr != nil {
			return 0, false, nil, closeErr
		}
	}
	return 0, false, postingsBS, nil
}

func (s *interim) calcTimestamp() (minimum, maximum int64) {
	for i, v := range s.results {
		timed, ok := v.(interface{ Timestamp() int64 })
		if !ok {
			return 0, 0
		}
		t := timed.Timestamp()
		// Unknown timestamps must not allow the segment to be pruned.
		if t == 0 {
			return 0, 0
		}
		if i == 0 || t < minimum {
			minimum = t
		}
		if i == 0 || t > maximum {
			maximum = t
		}
	}
	return minimum, maximum
}
