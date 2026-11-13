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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

// Builder builds a segment from documents added one at a time. Add records
// each term's postings varint encoded in its own chain of blocks (see
// termStream), the way riot-go's and Tantivy's indexing recorders do, and
// copies the stored values, so a caller can drop each document (and its
// analysis) once added. Build sorts the terms and decodes one term's
// postings at a time as it encodes them, so no inverted copy of the
// documents is held alongside the records. Build writes the same segment
// as New over the same documents in the same order.
//
// A Builder is not safe for concurrent use. After an error, Add and Build
// return that error.
type Builder struct {
	normCalc func(string, int) float32
	opts     Options
	err      error

	fieldIDs map[string]uint16 // field name -> local id, in order of first use
	fields   []*builderField
	numDocs  int

	arena     termArena         // the postings of every term, see termStream
	keys      byteBlocks        // the bytes of every term
	stored    byteBlocks        // stored values of all documents, in order
	storedRec blocks[storedRec] // one per stored value
	docStored blocks[int]       // per document, the end of its storedRec

	timeMin, timeMax int64
	untimed          bool // a document has no timestamp

	idDv storedDocValue // see interim.idDv

	// state of the current document
	docNum    uint32
	docMark   int
	docFields []uint16
	mark      []int // per local field id, the docMark of its last document
	length    []int // per local field id, its length in the current document
	count     []int // per local field id, its values in the current document
	curID     uint16
	curField  *builderField
	tlocs     []interimLoc // locations of the current term
	scratch   []byte       // the current posting, encoded
	rollIdx   map[rollKey]int
	rolls     []rollTerm
	rollCur   int

	// set by Build for gather: local field id -> global id, the fields in
	// global order, and the first postings list id of each
	gid    []uint16
	global []*builderField
	bases  []uint64

	// visitors, bound once so visiting allocates nothing
	visitCount, visitAdd     func(segment.Field)
	visitTerm, visitRollTerm func(segment.FieldTerm)
	visitLoc, visitRollLoc   func(segment.Location)
}

type builderField struct {
	id          uint16 // local
	name        string
	terms       termTable          // term -> local term id
	streams     blocks[termStream] // local term id -> its postings
	docs, freq  uint64
	dv          bool
	numericOnly bool
	seen        bool // named by a field, not only by locations
	numeric     []interimNumeric

	// norms by field length, computed as lengths are first seen
	norms    []float32 // lengths below smallNorms; normSet marks them
	normSet  []bool
	bigNorms map[int]float32

	byRank []uint32 // set by Build: local term ids in term order
}

// smallNorms bounds the field lengths whose norms a slice holds.
const smallNorms = 1024

// minNorms is the least number of norms a field's slice grows to.
const minNorms = 16

type storedRec struct {
	field uint16
	n     uint32
}

// rollKey and rollTerm merge a field's values within a document when its
// name repeats, as New's rollup does.
type rollKey struct {
	field uint16
	term  string
}

type rollTerm struct {
	key  rollKey
	freq int
	locs []interimLoc
}

// NewBuilder returns an empty Builder with the default Options.
func NewBuilder(normCalc func(string, int) float32) *Builder {
	return DefaultOptions().NewBuilder(normCalc)
}

// NewBuilder returns an empty Builder whose segment uses o.
func (o Options) NewBuilder(normCalc func(string, int) float32) *Builder {
	b := &Builder{normCalc: normCalc, opts: o, fieldIDs: map[string]uint16{}, rollIdx: map[rollKey]int{}}
	b.visitCount, b.visitAdd = b.countField, b.addField
	b.visitTerm, b.visitLoc = b.term, b.directLoc
	b.visitRollTerm, b.visitRollLoc = b.rollTerm, b.rollLoc
	_, b.err = b.fieldID(_idFieldName) // _id is always field 0
	return b
}

// Len returns the number of documents added.
func (b *Builder) Len() int { return b.numDocs }

// Size approximates the heap the added documents take.
func (b *Builder) Size() int {
	n := b.stored.cap + int(unsafe.Sizeof(storedRec{}))*b.storedRec.cap() + 8*b.docStored.cap()
	for _, fb := range b.fields {
		n += int(unsafe.Sizeof(termStream{}))*fb.streams.cap() +
			int(unsafe.Sizeof(interimNumeric{}))*cap(fb.numeric) + fb.terms.size()
	}
	return n + b.arena.size + b.keys.cap
}

// Add adds doc as the next document; doc is not used once Add returns.
func (b *Builder) Add(doc segment.Document) error {
	if b.err == nil {
		b.add(doc)
	}
	return b.err
}

func (b *Builder) fieldID(name string) (uint16, error) {
	if id, ok := b.fieldIDs[name]; ok {
		return id, nil
	}
	if len(b.fields) >= math.MaxUint16 {
		return 0, fmt.Errorf("too many fields for uint16 field IDs")
	}
	id := uint16(len(b.fields)) // #nosec G115 -- bounded above.
	b.fieldIDs[name] = id
	b.fields = append(b.fields, &builderField{id: id, name: name, numericOnly: true})
	b.mark, b.length, b.count = append(b.mark, 0), append(b.length, 0), append(b.count, 0)
	return id, nil
}

func (b *Builder) add(doc segment.Document) {
	if over32(b.numDocs) {
		b.err = fmt.Errorf("too many documents for uint32 document numbers")
		return
	}
	b.docNum = uint32(b.numDocs) // #nosec G115 -- bounded above.
	b.docMark++
	b.docFields = b.docFields[:0]
	doc.EachField(b.visitCount)
	if b.err != nil {
		return
	}
	for _, id := range b.docFields {
		fb := b.fields[id]
		fb.docs++
		fb.freq += uint64(b.length[id]) // #nosec G115 -- lengths are checked nonnegative.
	}
	doc.EachField(b.visitAdd)
	if b.err == nil && len(b.rolls) > 0 {
		b.flushRolls()
	}
	if b.err != nil {
		return
	}
	b.docStored.append(b.storedRec.len())
	// as New: an unknown timestamp keeps the segment from being pruned
	if t := doc.Timestamp(); t == 0 {
		b.untimed = true
	} else if b.numDocs == 0 {
		b.timeMin, b.timeMax = t, t
	} else {
		b.timeMin, b.timeMax = min(b.timeMin, t), max(b.timeMax, t)
	}
	b.numDocs++
}

// countField defines the field and sums its length in the document.
func (b *Builder) countField(f segment.Field) {
	if b.err != nil {
		return
	}
	id, err := b.fieldID(f.Name())
	if err != nil {
		b.err = err
		return
	}
	b.fields[id].seen = true
	n := f.Length()
	if n < 0 {
		b.err = fmt.Errorf("negative field length")
		return
	}
	if b.mark[id] != b.docMark {
		b.mark[id], b.length[id], b.count[id] = b.docMark, 0, 0
		b.docFields = append(b.docFields, id)
	}
	if n > math.MaxInt-b.length[id] {
		b.err = fmt.Errorf("field %s length overflows", f.Name())
		return
	}
	b.length[id] += n
	b.count[id]++
}

func (b *Builder) addField(f segment.Field) {
	if b.err != nil {
		return
	}
	id := b.fieldIDs[f.Name()]
	fb := b.fields[id]
	if id == 0 {
		b.idDv.check(f)
		b.idDv.bad = b.idDv.bad || b.count[id] > 1
	}
	if f.Store() {
		v := f.Value()
		if over32(len(v)) {
			b.err = errInterimRange
			return
		}
		b.stored.append(v)
		b.storedRec.append(storedRec{field: id, n: uint32(len(v))}) // #nosec G115 -- bounded above.
	}
	if f.IndexDocValues() {
		fb.dv = true
		nv, ok := int64(0), false
		if nf, isNum := f.(NumericField); isNum {
			nv, ok = nf.NumericValue()
		}
		if ok {
			fb.numeric = append(fb.numeric, interimNumeric{docNum: uint64(b.docNum), val: nv})
		} else {
			fb.numericOnly = false
		}
	}
	b.curID, b.curField = id, fb
	if b.count[id] > 1 {
		f.EachTerm(b.visitRollTerm)
		return
	}
	b.cacheNorm(fb, b.length[id])
	f.EachTerm(b.visitTerm)
}

func termFreq(t segment.FieldTerm) (uint32, error) {
	freq := t.Frequency()
	if freq < 0 {
		return 0, fmt.Errorf("negative term frequency")
	}
	if over32(freq) {
		return 0, errInterimRange
	}
	return uint32(freq), nil // #nosec G115 -- bounded above.
}

func (b *Builder) term(t segment.FieldTerm) {
	if b.err != nil {
		return
	}
	freq, err := termFreq(t)
	if err != nil {
		b.err = err
		return
	}
	fb := b.curField
	tid, ok := b.termID(fb, t.Term())
	if !ok {
		return
	}
	b.tlocs = b.tlocs[:0]
	t.EachLocation(b.visitLoc)
	if b.err != nil {
		return
	}
	b.addPosting(fb, tid, freq, b.length[b.curID], b.tlocs)
}

// addPosting appends the current document's posting of term tid to its
// termStream: the document as a delta to the previous one, freq, the
// field length, which gives the norm, and the locations: each the zigzag
// delta of its position to the previous one's, shifted left over a bit
// set when it names another field, that field, and zigzag deltas of its
// start to the previous one's and of its end to its start.
func (b *Builder) addPosting(fb *builderField, tid, freq uint32, length int, locs []interimLoc) {
	t := fb.streams.ptr(int(tid))
	if over32(len(locs)) {
		b.err = errInterimRange
		return
	}
	p := binary.AppendUvarint(b.scratch[:0], uint64(b.docNum-t.lastDoc))
	p = binary.AppendUvarint(p, uint64(freq))
	p = binary.AppendUvarint(p, uint64(length)) // #nosec G115 -- lengths are checked nonnegative.
	p = binary.AppendUvarint(p, uint64(len(locs)))
	var pos, start int64
	for _, l := range locs {
		d := zigzag(int64(l.pos) - pos)
		if l.fieldID == fb.id {
			p = binary.AppendUvarint(p, d<<1)
		} else {
			p = binary.AppendUvarint(binary.AppendUvarint(p, d<<1|1), uint64(l.fieldID))
		}
		p = binary.AppendVarint(p, int64(l.start)-start)
		p = binary.AppendVarint(p, int64(l.end)-int64(l.start))
		pos, start = int64(l.pos), int64(l.start)
	}
	b.scratch = p
	if !b.arena.write(t, p) {
		b.err = errInterimRange
		return
	}
	t.lastDoc = b.docNum
	t.docs++
}

// cacheNorm computes the norm of fb at length unless known.
func (b *Builder) cacheNorm(fb *builderField, length int) {
	if length < smallNorms {
		if length < len(fb.normSet) && fb.normSet[length] {
			return
		}
		if length >= len(fb.norms) {
			n := 2 * len(fb.norms)
			n = min(max(n, length+1, minNorms), smallNorms)
			fb.norms, fb.normSet = slices.Grow(fb.norms, n-len(fb.norms))[:n], slices.Grow(fb.normSet, n-len(fb.normSet))[:n]
		}
		fb.norms[length], fb.normSet[length] = b.normCalc(fb.name, length), true
		return
	}
	if _, ok := fb.bigNorms[length]; !ok {
		if fb.bigNorms == nil {
			fb.bigNorms = map[int]float32{}
		}
		fb.bigNorms[length] = b.normCalc(fb.name, length)
	}
}

// norm returns the norm cacheNorm computed for length.
func (fb *builderField) norm(length int) float32 {
	if length < smallNorms {
		return fb.norms[length]
	}
	return fb.bigNorms[length]
}

// termID returns the local id of term, defining it if new; false when the
// postings arena is full. A new term's string is over its copy in b.keys,
// which is never modified, so terms take no allocation each.
func (b *Builder) termID(fb *builderField, term []byte) (uint32, bool) {
	h := termHash(term)
	if id, ok := fb.terms.find(h, term); ok {
		return id, true
	}
	t, ok := b.arena.newTermStream()
	if !ok {
		b.err = errInterimRange
		return 0, false
	}
	key := b.keys.append(term)
	id := fb.terms.add(h, unsafe.String(unsafe.SliceData(key), len(key)))
	fb.streams.append(t)
	return id, true
}

// location converts l; a location naming a field defines it, as New does.
func (b *Builder) location(l segment.Location) (interimLoc, bool) {
	pos, start, end := l.Pos(), l.Start(), l.End()
	if pos < 0 || start < 0 || end < 0 {
		b.err = fmt.Errorf("negative term location")
		return interimLoc{}, false
	}
	if over32(pos) || over32(start) || over32(end) {
		b.err = errInterimRange
		return interimLoc{}, false
	}
	f := b.curID
	if name := l.Field(); name != "" {
		var err error
		if f, err = b.fieldID(name); err != nil {
			b.err = err
			return interimLoc{}, false
		}
	}
	// #nosec G115 -- bounded above.
	return interimLoc{fieldID: f, pos: uint32(pos), start: uint32(start), end: uint32(end)}, true
}

func (b *Builder) directLoc(l segment.Location) {
	if b.err != nil {
		return
	}
	if il, ok := b.location(l); ok {
		b.tlocs = append(b.tlocs, il)
	}
}

func (b *Builder) rollTerm(t segment.FieldTerm) {
	if b.err != nil {
		return
	}
	freq, err := termFreq(t)
	if err != nil {
		b.err = err
		return
	}
	key := rollKey{field: b.curID, term: string(t.Term())}
	i, ok := b.rollIdx[key]
	if !ok {
		i = len(b.rolls)
		b.rolls = append(b.rolls, rollTerm{key: key})
		b.rollIdx[key] = i
	}
	b.rollCur = i
	b.rolls[i].freq += int(freq)
	t.EachLocation(b.visitRollLoc)
}

func (b *Builder) rollLoc(l segment.Location) {
	if b.err != nil {
		return
	}
	if il, ok := b.location(l); ok {
		r := &b.rolls[b.rollCur]
		r.locs = append(r.locs, il)
	}
}

// flushRolls adds the merged postings of the document's repeated fields.
func (b *Builder) flushRolls() {
	for i := range b.rolls {
		r := &b.rolls[i]
		if over32(r.freq) {
			b.err = errInterimRange
			return
		}
		fb := b.fields[r.key.field]
		tid, ok := b.termID(fb, []byte(r.key.term))
		if !ok {
			return
		}
		b.cacheNorm(fb, b.length[r.key.field])
		b.addPosting(fb, tid, uint32(r.freq), b.length[r.key.field], r.locs) // #nosec G115 -- bounded above.
		if b.err != nil {
			return
		}
	}
	clear(b.rollIdx)
	clear(b.rolls)
	b.rolls = b.rolls[:0]
}

// Build returns the segment of the added documents and its size in bytes;
// the Builder must not be used afterwards. The stored fields are written
// while the terms are sorted and encoded, and each field's terms are
// sorted on their own goroutine.
func (b *Builder) Build() (_ segment.Segment, _ uint64, err error) {
	if b.err != nil {
		return nil, 0, b.err
	}
	b.err = fmt.Errorf("ice: Builder already built")

	n := len(b.fields)
	order, gid := b.fieldOrder()
	var br bytes.Buffer
	s := b.newInterim(order, &br)

	var storedIndexOffset uint64
	var storedChunkOffsets []uint64
	stored := make(chan error, 1)
	go func() {
		var werr error
		storedIndexOffset, storedChunkOffsets, werr = b.writeStored(s, gid)
		stored <- werr
	}()
	waitStored := sync.OnceValue(func() error { return <-stored })
	defer func() {
		if serr := waitStored(); serr != nil && !errors.Is(err, serr) {
			err = errors.Join(err, serr)
		}
	}()

	// writeDicts encodes each term's postings straight from the records,
	// so the records are never copied in term order
	s.encode, s.beforePlace = b.encode, waitStored
	b.gid, b.global, b.bases = gid, make([]*builderField, n), make([]uint64, n)
	for g, l := range order {
		b.global[g], b.bases[g] = b.fields[l], uint64(s.numPostingsLists) // #nosec G115 -- a count.
		s.numPostingsLists += b.fields[l].terms.len()
	}
	s.pidBase = b.bases
	var inverted sync.WaitGroup
	for g, fb := range b.global {
		inverted.Go(func() { b.invert(s, g, fb) })
	}
	inverted.Wait()

	var fdvIndexOffset uint64
	var dictOffsets []uint64
	if b.numDocs > 0 {
		if fdvIndexOffset, dictOffsets, err = s.writeDicts(); err != nil {
			return nil, 0, err
		}
	} else {
		dictOffsets = make([]uint64, n)
	}
	b.arena.release() // encode is done with the postings
	err = waitStored()
	if err != nil {
		return nil, 0, err
	}
	b.fields, b.global = nil, nil
	fieldsIndexOffset, err := persistFields(s.FieldsInv, s.FieldDocs, s.FieldFreqs, s.w, dictOffsets)
	if err != nil {
		return nil, 0, err
	}
	f := b.footer(s, storedIndexOffset, fieldsIndexOffset, fdvIndexOffset)
	// the segment keeps its bytes: the stored fields compress, so the buffer
	// grown for them uncompressed may hold far more than it holds
	data := br.Bytes()
	if cap(data)-len(data) > len(data)/8 {
		data = bytes.Clone(data)
	}
	sb, err := initSegmentBase(data, f, s.FieldsMap, s.FieldsInv, s.FieldDocs, s.FieldFreqs,
		dictOffsets, storedChunkOffsets, b.opts)
	if err != nil {
		return nil, 0, err
	}
	return sb, uint64(br.Len()), nil // #nosec G115 -- a nonnegative length.
}

// fieldOrder returns the fields in global field id order, as New: _id, the
// fields by name, then the fields only locations name, in order of
// definition; and the global id of each field.
func (b *Builder) fieldOrder() (order []int, gid []uint16) {
	n := len(b.fields)
	order = make([]int, 1, n)
	var located []int
	for i := 1; i < n; i++ {
		if b.fields[i].seen {
			order = append(order, i)
		} else {
			located = append(located, i)
		}
	}
	slices.SortFunc(order[1:], func(x, y int) int { return strings.Compare(b.fields[x].name, b.fields[y].name) })
	order = append(order, located...)
	gid = make([]uint16, n)
	for g, l := range order {
		gid[l] = uint16(g) // #nosec G115 -- at most MaxUint16 fields.
	}
	return order, gid
}

// newInterim returns the interim writing to br with the fields in order.
func (b *Builder) newInterim(order []int, br *bytes.Buffer) *interim {
	n := len(order)
	s := &interim{}
	s.initVisitors()
	s.chunkMode, s.normCalc, s.numDocs = defaultChunkMode, b.normCalc, b.numDocs
	s.idDv = b.idDv.result()
	br.Grow(b.stored.cap + 4*b.numDocs)
	s.w = newCountHashWriter(br)
	s.FieldsMap, s.FieldsInv = make(map[string]uint16, n), make([]string, n)
	s.FieldDocs, s.FieldFreqs = map[uint16]uint64{}, map[uint16]uint64{}
	s.Dicts, s.DictKeys = make([]map[string]uint64, n), make([][]string, n)
	s.IncludeDocValues, s.NumericOnly = make([]bool, n), make([]bool, n)
	s.NumericValues = make([][]interimNumeric, n)
	for g, l := range order {
		fb := b.fields[l]
		s.FieldsInv[g], s.FieldsMap[fb.name] = fb.name, uint16(g+1) // #nosec G115 -- see fieldOrder.
		if fb.docs > 0 {
			s.FieldDocs[uint16(g)], s.FieldFreqs[uint16(g)] = fb.docs, fb.freq // #nosec G115 -- see fieldOrder.
		}
		s.IncludeDocValues[g], s.NumericOnly[g], s.NumericValues[g] = fb.dv, fb.numericOnly, fb.numeric
	}
	return s
}

// footer returns the footer of the segment s holds.
func (b *Builder) footer(s *interim, storedIndexOffset, fieldsIndexOffset, fdvIndexOffset uint64) *footer {
	tMin, tMax := b.timeMin, b.timeMax
	if b.untimed || b.numDocs == 0 {
		tMin, tMax = 0, 0
	}
	return &footer{
		storedIndexOffset: storedIndexOffset,
		fieldsIndexOffset: fieldsIndexOffset,
		docValueOffset:    fdvIndexOffset,
		version:           writeVersion,
		docTimeMin:        uint64(tMin), // #nosec G115 -- signed timestamp bits, as New.
		docTimeMax:        uint64(tMax), // #nosec G115 -- see above.
		crc:               s.w.Sum32(),
		chunkMode:         defaultChunkMode,
		numDocs:           uint64(b.numDocs), // #nosec G115 -- a nonnegative count.
	}
}

// writeStored writes the stored fields as New's writeStoredFields does.
func (b *Builder) writeStored(s *interim, gid []uint16) (indexOffset uint64, chunkOffsets []uint64, err error) {
	var varBuf [binary.MaxVarintLen64]byte
	metaEncode := func(v uint64) (int, error) {
		return s.metaBuf.Write(varBuf[:binary.PutUvarint(varBuf[:], v)])
	}
	vals := make([][][]byte, len(gid))
	var offsets []byte
	if writeVersion < versionNoStoredIndex {
		offsets = make([]byte, 0, fileAddrWidth*b.numDocs)
	}
	coder := newChunkedDocumentCoder(uint64(defaultDocumentChunkSize), s.w)
	var data []byte
	rec, stored := 0, byteReader{l: &b.stored}
	for d := range b.numDocs {
		for i := range vals {
			vals[i] = vals[i][:0]
		}
		for end := b.docStored.at(d); rec < end; rec++ {
			r := b.storedRec.at(rec)
			g := gid[r.field]
			vals[g] = append(vals[g], stored.next(int(r.n)))
		}
		s.metaBuf.Reset()
		data = data[:0]
		curr := 0
		for g, v := range vals {
			if len(v) > 0 {
				if curr, data, err = encodeStoredFieldValues(g, v, curr, metaEncode, data); err != nil {
					return 0, nil, err
				}
			}
		}
		if writeVersion < versionNoStoredIndex {
			offsets = binary.BigEndian.AppendUint64(offsets, coder.Size())
		}
		if _, err = coder.Add(uint64(d), s.metaBuf.Bytes(), data); err != nil { // #nosec G115 -- a document number.
			return 0, nil, err
		}
	}
	b.stored, b.storedRec, b.docStored = byteBlocks{}, blocks[storedRec]{}, blocks[int]{}
	err = coder.Write()
	if err != nil {
		return 0, nil, err
	}
	indexOffset = uint64(s.w.Count()) // #nosec G115 -- countHashWriter counts nonnegative writes.
	if writeVersion >= versionNoStoredIndex {
		return indexOffset, coder.Offsets(), nil
	}
	if _, err = s.w.Write(offsets); err != nil {
		return 0, nil, err
	}
	return indexOffset, coder.Offsets(), nil
}

// invert sorts fb's terms into s, the way prepareDicts lays them out:
// field g's postings lists are its terms in order from s.pidBase[g] on.
func (b *Builder) invert(s *interim, g int, fb *builderField) {
	terms := fb.terms.terms
	fb.byRank = make([]uint32, len(terms))
	for i := range fb.byRank {
		fb.byRank[i] = uint32(i) // #nosec G115 -- local term ids.
	}
	// terms first seen in order, as increasing numeric values are, need no
	// sort; the others sort with their keys at hand, not behind their ids
	if !slices.IsSorted(terms) {
		type ranked struct {
			key string
			id  uint32
		}
		rs := make([]ranked, len(terms))
		for i, k := range terms {
			rs[i] = ranked{k, uint32(i)} // #nosec G115 -- local term ids.
		}
		slices.SortFunc(rs, func(x, y ranked) int { return strings.Compare(x.key, y.key) })
		for o, r := range rs {
			fb.byRank[o] = r.id
		}
	}
	keys := make([]string, len(terms))
	for o, id := range fb.byRank {
		keys[o] = terms[id]
	}
	s.DictKeys[g] = keys
	fb.terms, fb.numeric = termTable{}, nil
}

// encode is interim.encode: it encodes the postings of pid as
// encodeTermPostings does, reading them straight from their records in one
// pass, so they are never decoded into an intermediate copy.
func (b *Builder) encode(docTermMap [][]byte, term string, pid uint64, w *dictWorker) (
	val uint64, oneHit bool, bm *roaring.Bitmap, err error) {
	g := sort.Search(len(b.bases), func(i int) bool { return b.bases[i] > pid }) - 1
	fb := b.global[g]
	t := fb.streams.ptr(int(fb.byRank[pid-b.bases[g]]))
	gid := uint16(g) // #nosec G115 -- at most MaxUint16 fields.
	r := b.arena.reader(t)
	if t.docs == 1 {
		// same "1-hit" encoding as encodeTermPostings; p is a copy, so the
		// posting is read again below when it is not one
		p := r
		// #nosec G115 -- addPosting wrote these from uint32s.
		doc, freq, norm, nlocs := uint32(p.uvarint()), p.uvarint(), fb.norm(int(p.uvarint())), p.uvarint()
		normBits := uint64(math.Float32bits(norm))
		if freq == 1 && nlocs == 0 && under32Bits(uint64(doc)) && normBits != 0 && under32Bits(normBits) {
			if docTermMap != nil {
				docTermMap[doc] = append(append(docTermMap[doc], term...), termSeparator)
			}
			return fSTValEncode1Hit(uint64(doc), normBits), true, nil, nil
		}
	}
	if err := setChunkSizes(defaultChunkMode, uint64(t.docs), b.numDocs, w.tf, w.loc, w.off); err != nil {
		return 0, false, nil, err
	}
	w.docs = w.docs[:0]
	var doc uint32
	for !r.at(t.tail) {
		// #nosec G115 -- addPosting wrote these from uint32s.
		doc += uint32(r.uvarint())
		freq, norm, nlocs := r.uvarint(), fb.norm(int(r.uvarint())), r.uvarint() // #nosec G115 -- see doc.
		w.docs = append(w.docs, doc)
		if err := w.tf.Add(uint64(doc), encodeFreqHasLocs(freq, nlocs > 0), uint64(math.Float32bits(norm))); err != nil {
			return 0, false, nil, err
		}
		if nlocs > 0 {
			var pos, start int64
			var prevPos, prevStart uint64
			for range nlocs {
				d, field := r.uvarint(), gid
				if d&1 != 0 {
					field = b.gid[r.uvarint()]
				}
				pos += unzigzag(d >> 1)
				start += r.varint()
				end := start + r.varint()
				// #nosec G115 -- locations decoded from checked uint32s.
				p, st := uint64(pos), uint64(start)
				recordLocation(w.loc, w.off, uint64(field), p, st, uint64(end), prevPos, prevStart) // #nosec G115 -- see above.
				prevPos, prevStart = p, st
			}
			if err := addLocationRecords(uint64(doc), freq, w.loc, w.off); err != nil {
				return 0, false, nil, err
			}
		}
		if docTermMap != nil {
			docTermMap[doc] = append(append(docTermMap[doc], term...), termSeparator)
		}
	}
	if err := closeEncoders(w.tf, w.loc, w.off); err != nil {
		return 0, false, nil, err
	}
	if w.bm == nil {
		w.bm = roaring.New()
	}
	w.bm.Clear()
	w.bm.AddMany(w.docs)
	return 0, false, w.bm, nil
}
