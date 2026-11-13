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
	"fmt"
	"math"
	"slices"

	segment "github.com/vcaesar/bluge_segment_api"
)

// Term doc values of version 9 come in two more kinds than the chunked
// content (dvKindTerms):
//
//   - dvKindStored: no bytes; each document's single value is its stored
//     value of the field. New and Builder use it for _id when every document
//     stores exactly its one _id term, so the identifier is kept once.
//
//   - dvKindOrdinal: the field's distinct terms, sorted, and a numeric column
//     of term ordinals, written when smaller than the chunked content (few
//     distinct terms, as a category or tag field):
//
//     [numeric column of ordinals][per term: uvarint length, bytes]
//     [column length u64][term count u64], both big endian
const ordinalTrailerLen = 16

// maxOrdinalTerms bounds the distinct terms a termDvWriter tracks for the
// ordinal kind; a field with more is written as chunked content.
const maxOrdinalTerms = 4096

// minOrdinalValues is the least capacity the ordinal value slices grow to.
const minOrdinalValues = 64

// termDvWriter writes a field's term doc values, added in doc order, as
// chunked content or, when that is smaller, as ordinals.
type termDvWriter struct {
	numDocs uint64
	content *chunkedContentCoder
	buf     bytes.Buffer // the chunked content

	ids   map[string]uint16 // term -> first-seen id; nil once ordinals are given up
	terms []string          // by first-seen id
	docs  []uint32          // per value
	vals  []uint16          // per value, its first-seen id

	block   uint64 // numeric block of the last document
	inBlock int    // values in it
}

func newTermDvWriter(numDocs uint64) (*termDvWriter, error) {
	// NOTE: term doc values continue to use legacy chunk mode
	chunkSize, err := getChunkSize(legacyChunkMode, 0, 0)
	if err != nil {
		return nil, err
	}
	w := &termDvWriter{numDocs: numDocs, ids: map[string]uint16{}}
	// progressive: each chunk goes to buf as it fills, not through a second
	// copy of the whole content in the coder
	w.content = newChunkedContentCoder(chunkSize, max(numDocs, 1)-1, &w.buf, true)
	return w, nil
}

// Add adds the terms of docNum, each followed by termSeparator; docNum
// must increase between calls.
func (w *termDvWriter) Add(docNum uint64, terms []byte) error {
	if b := docNum / numericBlockSize; b != w.block {
		w.block, w.inBlock = b, 0
	}
	// as visitDocValues reads them: a term ends at its termSeparator
	for rest := terms; w.ids != nil; {
		i := bytes.IndexByte(rest, termSeparator)
		if i < 0 {
			break
		}
		t := rest[:i]
		rest = rest[i+1:]
		id, ok := w.ids[string(t)]
		// ordinals are given up for more terms than tracked, more values
		// than a numeric block loads, or a document past docs' width
		if !ok && len(w.terms) == maxOrdinalTerms || w.inBlock == maxNumericBlockValues || docNum > math.MaxUint32 {
			w.ids, w.terms, w.docs, w.vals = nil, nil, nil, nil
			break
		}
		if !ok {
			term := string(t)
			id = uint16(len(w.terms)) // #nosec G115 -- below maxOrdinalTerms.
			w.ids[term] = id
			w.terms = append(w.terms, term)
		}
		if len(w.docs) == cap(w.docs) {
			// doubled, as append grows large slices by only a quarter
			w.docs, w.vals = slices.Grow(w.docs, max(len(w.docs), minOrdinalValues)),
				slices.Grow(w.vals, max(len(w.vals), minOrdinalValues))
		}
		w.docs, w.vals = append(w.docs, uint32(docNum)), append(w.vals, id) // #nosec G115 -- checked above.
		w.inBlock++
	}
	return w.content.Add(docNum, terms)
}

// Write writes the doc values to out and returns their kind and range.
func (w *termDvWriter) Write(out *countHashWriter) (kind, start, end uint64, err error) {
	err = w.content.Close()
	if err == nil {
		_, err = w.content.Write()
	}
	if err != nil {
		return 0, 0, 0, err
	}
	data, kind := w.buf.Bytes(), dvKindTerms
	if w.ids != nil {
		var ord bytes.Buffer
		err = w.writeOrdinals(&ord)
		if err != nil {
			return 0, 0, 0, err
		}
		if ord.Len() < len(data) {
			data, kind = ord.Bytes(), dvKindOrdinal
		}
	}
	start = uint64(out.Count()) // #nosec G115 -- countHashWriter counts nonnegative writes.
	if _, err = out.Write(data); err != nil {
		return 0, 0, 0, err
	}
	return kind, start, uint64(out.Count()), nil // #nosec G115 -- see above.
}

func (w *termDvWriter) writeOrdinals(out *bytes.Buffer) error {
	sorted := slices.Clone(w.terms)
	slices.Sort(sorted)
	rank := make([]int64, len(w.terms))
	for i, t := range sorted {
		rank[w.ids[t]] = int64(i)
	}
	col := newNumericColumnWriter(w.numDocs)
	for i, d := range w.docs {
		if err := col.Add(uint64(d), rank[w.vals[i]]); err != nil {
			return err
		}
	}
	colLen, err := col.Write(out)
	if err != nil {
		return err
	}
	for _, t := range sorted {
		out.Write(binary.AppendUvarint(out.AvailableBuffer(), uint64(len(t))))
		out.WriteString(t)
	}
	var trailer [ordinalTrailerLen]byte
	binary.BigEndian.PutUint64(trailer[:8], uint64(colLen)) // #nosec G115 -- a nonnegative length.
	binary.BigEndian.PutUint64(trailer[8:], uint64(len(sorted)))
	out.Write(trailer[:])
	return nil
}

// ordinalColumn is a loaded dvKindOrdinal field.
type ordinalColumn struct {
	col   *numericColumn
	terms [][]byte
}

func (c *ordinalColumn) size() int {
	n := c.col.size() + len(c.terms)*sizeOfPtr*3
	for _, t := range c.terms {
		n += len(t)
	}
	return n
}

func (s *Segment) loadOrdinalColumn(field string, start, end uint64) (*ordinalColumn, error) {
	dataLen := s.data.Len()
	if dataLen < 0 || start > end || end > uint64(dataLen) || end-start < ordinalTrailerLen {
		return nil, fmt.Errorf("invalid ordinal doc value range: %d-%d", start, end)
	}
	// #nosec G115 -- bounded by data.Len(), an int.
	tail, err := s.data.Read(int(end-ordinalTrailerLen), int(end))
	if err != nil {
		return nil, err
	}
	colLen, numTerms := binary.BigEndian.Uint64(tail[:8]), binary.BigEndian.Uint64(tail[8:])
	dictLen := end - start - ordinalTrailerLen
	if colLen > dictLen || numTerms > dictLen-colLen {
		return nil, fmt.Errorf("invalid ordinal doc values for field %s", field)
	}
	col, err := s.loadNumericColumn(field, start, start+colLen)
	if err != nil {
		return nil, err
	}
	// #nosec G115 -- bounded by data.Len(), an int.
	dict, err := s.data.Read(int(start+colLen), int(end-ordinalTrailerLen))
	if err != nil {
		return nil, err
	}
	rv := &ordinalColumn{col: col, terms: make([][]byte, numTerms)}
	for i := range rv.terms {
		n, read := binary.Uvarint(dict)
		if read <= 0 || n > uint64(len(dict)-read) { // #nosec G115 -- a nonnegative length.
			return nil, fmt.Errorf("invalid ordinal doc value term for field %s", field)
		}
		end := read + int(n) // #nosec G115 -- n is bounded by len(dict) above.
		// capped: a visitor appending to a term must not write the segment
		rv.terms[i], dict = dict[read:end:end], dict[end:]
	}
	for _, b := range col.blocks {
		if b.count > 0 && (b.min < 0 || uint64(b.max) >= numTerms) { // #nosec G115 -- b.max >= b.min >= 0.
			return nil, fmt.Errorf("ordinal out of range for field %s", field)
		}
	}
	return rv, nil
}

// ordinalCursor visits one ordinal field's terms of documents in order.
type ordinalCursor struct {
	ords    *ordinalColumn
	cur     *numericColumnCursor
	visitor segment.DocumentValueVisitor
	visit   func(field string, ord int64)
}

func newOrdinalCursor(ords *ordinalColumn) *ordinalCursor {
	c := &ordinalCursor{ords: ords, cur: newNumericColumnCursor(ords.col)}
	c.visit = func(field string, ord int64) {
		// a corrupt block can decode past its validated max
		if uint64(ord) < uint64(len(c.ords.terms)) { // #nosec G115 -- negative wraps out of range.
			c.visitor(field, c.ords.terms[ord])
		}
	}
	return c
}

func (c *ordinalCursor) visitDoc(s *Segment, docNum uint64, visitor segment.DocumentValueVisitor) error {
	c.visitor = visitor
	defer func() { c.visitor = nil }()
	return c.cur.visit(s, docNum, c.visit)
}

// iterate calls visitor with each document's terms, as iterateAllDocValues.
func (c *ordinalColumn) iterate(s *Segment, visitor docNumTermsVisitor) error {
	var terms []byte
	cur := uint64(docDropped)
	err := newNumericColumnCursor(c.col).iterate(s, func(docNum uint64, ord int64) error {
		if docNum != cur && len(terms) > 0 {
			if err := visitor(cur, terms); err != nil {
				return err
			}
			terms = terms[:0]
		}
		cur = docNum
		if uint64(ord) >= uint64(len(c.terms)) { // #nosec G115 -- negative wraps out of range.
			return fmt.Errorf("ordinal %d out of range for %d terms", ord, len(c.terms))
		}
		terms = append(append(terms, c.terms[ord]...), termSeparator)
		return nil
	})
	if err != nil || len(terms) == 0 {
		return err
	}
	return visitor(cur, terms)
}

// visitStoredDocValue visits docNum's stored value of a dvKindStored field,
// reading it through vdc, which keeps its chunk for the next document.
func (s *Segment) visitStoredDocValue(vdc *visitDocumentCtx, docNum uint64, field string,
	visitor segment.DocumentValueVisitor) error {
	return s.visitDocument(vdc, docNum, func(f string, v []byte) bool {
		if f != field {
			return true
		}
		// capped: a visitor appending to the value must not write the chunk
		visitor(field, v[:len(v):len(v)])
		return false
	})
}

// iterateStoredDocValues calls visitor with each document's stored value of
// a dvKindStored field, as iterateAllDocValues, skipping the documents that
// mapping drops without reading their stored fields.
func (s *Segment) iterateStoredDocValues(field string, mapping []uint64, visitor docNumTermsVisitor) error {
	var terms []byte
	vdc := &visitDocumentCtx{keep: true}
	for docNum := range s.footer.numDocs {
		if docNum < uint64(len(mapping)) && mapping[docNum] == docDropped {
			continue
		}
		terms = terms[:0]
		err := s.visitStoredDocValue(vdc, docNum, field, func(_ string, v []byte) {
			terms = append(append(terms, v...), termSeparator)
		})
		if err != nil {
			return err
		}
		if len(terms) > 0 {
			if err := visitor(docNum, terms); err != nil {
				return err
			}
		}
	}
	return nil
}

// storedDocValue tracks whether a field qualifies for dvKindStored: every
// document with a value stores it once as its only term.
type storedDocValue struct {
	seen, bad bool

	// the field check visits: its value and terms so far
	value []byte
	terms int
	visit segment.VisitTerm // v.term, bound once so check allocates nothing
}

func (v *storedDocValue) ok() bool { return v.seen && !v.bad }

// check records one document's field: its stored values and terms.
func (v *storedDocValue) check(field segment.Field) {
	if v.bad {
		return
	}
	if !field.Store() || !field.IndexDocValues() {
		v.bad = true
		return
	}
	if nf, ok := field.(NumericField); ok {
		if _, isNum := nf.NumericValue(); isNum {
			v.bad = true
			return
		}
	}
	if v.visit == nil {
		v.visit = v.term
	}
	v.value, v.terms = field.Value(), 0
	field.EachTerm(v.visit)
	v.bad = v.bad || v.terms != 1
	v.value, v.seen = nil, true
}

func (v *storedDocValue) term(t segment.FieldTerm) {
	v.terms++
	if v.terms > 1 || !bytes.Equal(t.Term(), v.value) {
		v.bad = true
	}
}

// result is v's outcome alone, for another owner to hold.
func (v *storedDocValue) result() storedDocValue {
	return storedDocValue{seen: v.seen, bad: v.bad}
}
