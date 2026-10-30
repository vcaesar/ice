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
	"fmt"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/vellum"
	segment "github.com/vcaesar/bluge_segment_api"
)

// Dictionary is the representation of the term dictionary
type Dictionary struct {
	sb        *Segment
	field     string
	fieldID   uint16
	fst       *vellum.FST
	fstReader *vellum.Reader
}

// represents an immutable, empty postings list
var emptyDictionary = &Dictionary{}

// PostingsList returns the postings list for the specified term
func (d *Dictionary) PostingsList(term []byte, except *roaring.Bitmap,
	prealloc segment.PostingsList) (segment.PostingsList, error) {
	var preallocPL *PostingsList
	pl, ok := prealloc.(*PostingsList)
	if ok && pl != nil {
		preallocPL = pl
	}
	return d.postingsList(term, except, preallocPL)
}

func (d *Dictionary) postingsList(term []byte, except *roaring.Bitmap, rv *PostingsList) (*PostingsList, error) {
	if d.fstReader == nil {
		if rv == nil || rv == emptyPostingsList {
			return emptyPostingsList, nil
		}
		return d.postingsListInit(rv, except), nil
	}

	postingsOffset, exists, err := d.fstReader.Get(term)
	if err != nil {
		return nil, fmt.Errorf("vellum err: %v", err)
	}
	if !exists {
		if rv == nil || rv == emptyPostingsList {
			return emptyPostingsList, nil
		}
		return d.postingsListInit(rv, except), nil
	}

	return d.postingsListFromOffset(postingsOffset, except, rv)
}

func (d *Dictionary) postingsListFromOffset(postingsOffset uint64, except *roaring.Bitmap, rv *PostingsList) (*PostingsList, error) {
	rv = d.postingsListInit(rv, except)

	err := rv.read(postingsOffset, d)
	if err != nil {
		return nil, err
	}

	return rv, nil
}

func (d *Dictionary) postingsListInit(rv *PostingsList, except *roaring.Bitmap) *PostingsList {
	if rv == nil || rv == emptyPostingsList {
		rv = &PostingsList{}
	} else {
		postings := rv.postings
		if postings != nil {
			postings.Clear()
		}

		*rv = PostingsList{} // clear the struct

		rv.postings = postings
	}
	rv.sb = d.sb
	rv.except = except
	return rv
}

func (d *Dictionary) Contains(key []byte) (bool, error) {
	if d.fst != nil {
		return d.fst.Contains(key)
	}
	return false, nil
}

func (d *Dictionary) Close() error {
	return nil
}

// Iterator returns an iterator which only visits terms
// having the the vellum automaton and start/end key range
func (d *Dictionary) Iterator(a segment.Automaton,
	startKeyInclusive, endKeyExclusive []byte) segment.DictionaryIterator {
	if d.fst != nil {
		rv := &DictionaryIterator{
			d: d,
		}

		itr, err := d.fst.Search(a, startKeyInclusive, endKeyExclusive)
		if err == nil {
			rv.itr = itr
		} else if err != vellum.ErrIteratorDone {
			rv.err = err
		}

		return rv
	}
	return emptyDictionaryIterator
}

// represents an immutable, empty dictionary iterator
var emptyDictionaryIterator = &DictionaryIterator{}

// DictionaryIterator is an iterator for term dictionary
type DictionaryIterator struct {
	d         *Dictionary
	itr       vellum.Iterator
	err       error
	tmp       PostingsList
	entry     DictEntry
	omitCount bool
}

// Next returns the next entry in the dictionary
func (i *DictionaryIterator) Next() (segment.DictionaryEntry, error) {
	if i.err != nil && i.err != vellum.ErrIteratorDone {
		return nil, i.err
	} else if i.itr == nil || i.err == vellum.ErrIteratorDone {
		return nil, nil
	}
	term, postingsOffset := i.itr.Current()
	i.entry.term = string(term)
	if !i.omitCount {
		i.err = i.tmp.read(postingsOffset, i.d)
		if i.err != nil {
			return nil, i.err
		}
		i.entry.count = i.tmp.Count()
	}
	i.err = i.itr.Next()
	return &i.entry, nil
}

func (i *DictionaryIterator) Close() error {
	return nil
}

type DictEntry struct {
	term  string
	count uint64
}

func (d *DictEntry) Term() string {
	return d.term
}

func (d *DictEntry) Count() uint64 {
	return d.count
}

// TermsIterator is Iterator without reading each term's postings for its
// count: entries report a Count of 0. Use it when only terms are needed.
func (d *Dictionary) TermsIterator(a segment.Automaton,
	startKeyInclusive, endKeyExclusive []byte) segment.DictionaryIterator {
	rv := d.Iterator(a, startKeyInclusive, endKeyExclusive)
	if it, ok := rv.(*DictionaryIterator); ok && it != emptyDictionaryIterator {
		it.omitCount = true
	}
	return rv
}

// OrPostingsDense ORs the postings of every term the automaton accepts in
// [startKeyInclusive, endKeyExclusive) into dense, a bitset of the
// segment's documents, walking the dictionary once. It returns false,
// leaving dense partly written, if a posting falls outside dense.
func (d *Dictionary) OrPostingsDense(a segment.Automaton, startKeyInclusive, endKeyExclusive []byte,
	dense []uint64) (bool, error) {
	if d.fst == nil {
		return true, nil
	}
	itr, err := d.fst.Search(a, startKeyInclusive, endKeyExclusive)
	limit := uint64(len(dense)) * 64
	var pl PostingsList
	for err == nil {
		_, offset := itr.Current()
		if err = pl.read(offset, d); err != nil {
			return false, err
		}
		if pl.normBits1Hit != 0 {
			if pl.docNum1Hit >= limit {
				return false, nil
			}
			dense[pl.docNum1Hit/64] |= 1 << (pl.docNum1Hit % 64)
		} else if !pl.postings.IsEmpty() {
			if uint64(pl.postings.Maximum()) >= limit {
				return false, nil
			}
			pl.postings.WriteDenseTo(dense)
		}
		pl.normBits1Hit = 0
		err = itr.Next()
	}
	if err != vellum.ErrIteratorDone {
		return false, err
	}
	return true, nil
}
