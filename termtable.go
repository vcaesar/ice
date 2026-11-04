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

import "hash/maphash"

// termTable maps a field's terms to their local ids, the way Tantivy's
// indexing hash map does: an open-addressed table of (hash, id+1) words
// probed linearly, over the terms' bytes held elsewhere (Builder.keys).
// It holds no pointers but the term strings, so growing it copies and the
// GC scans a word per slot, not a map's buckets.
type termTable struct {
	slots []uint64 // hash<<32 | id+1; 0 when empty
	terms []string // local term id -> its bytes
}

var termSeed = maphash.MakeSeed()

func termHash(term []byte) uint32 {
	return uint32(maphash.Bytes(termSeed, term)) // #nosec G115 -- the low hash bits.
}

func (t *termTable) len() int { return len(t.terms) }

// find returns the id of term, whose hash is h.
func (t *termTable) find(h uint32, term []byte) (uint32, bool) {
	if len(t.slots) == 0 {
		return 0, false
	}
	mask := uint32(len(t.slots) - 1) // #nosec G115 -- a power of two below MaxUint32.
	for i := h & mask; ; i = (i + 1) & mask {
		s := t.slots[i]
		if s == 0 {
			return 0, false
		}
		if id := uint32(s) - 1; uint32(s>>32) == h && t.terms[id] == string(term) { // #nosec G115 -- the words put packed.
			return id, true
		}
	}
}

// add defines term, which find did not find, as the next id; term must
// not change afterwards.
func (t *termTable) add(h uint32, term string) uint32 {
	if 4*(len(t.terms)+1) > 3*len(t.slots) {
		old := t.slots
		t.slots = make([]uint64, max(2*len(old), 64))
		for _, s := range old {
			if s != 0 {
				t.put(uint32(s>>32), uint32(s)-1) // #nosec G115 -- see find.
			}
		}
	}
	id := uint32(len(t.terms)) // #nosec G115 -- fewer terms than postings.
	t.terms = append(t.terms, term)
	t.put(h, id)
	return id
}

func (t *termTable) put(h, id uint32) {
	mask := uint32(len(t.slots) - 1) // #nosec G115 -- see find.
	i := h & mask
	for t.slots[i] != 0 {
		i = (i + 1) & mask
	}
	t.slots[i] = uint64(h)<<32 | uint64(id+1)
}

// size approximates the heap the table takes.
func (t *termTable) size() int {
	return 8*len(t.slots) + 16*len(t.terms)
}
