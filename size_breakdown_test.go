package ice

import (
	"os"
	"path/filepath"
	"testing"

	segment "github.com/vcaesar/bluge_segment_api"
)

// TestSizeBreakdown prints the byte sections of the segment file named by
// ICE_SEG_FILE: stored chunks and index, then per field its FST, doc values
// and (the rest) postings. It is a measuring aid and skips without the file.
func TestSizeBreakdown(t *testing.T) {
	path := os.Getenv("ICE_SEG_FILE")
	if path == "" {
		t.Skip("ICE_SEG_FILE not set")
	}
	raw, err := os.ReadFile(path) // #nosec G304 G703 -- a file the developer names.
	if err != nil {
		t.Fatal(err)
	}
	seg, err := Load(segment.NewDataBytes(raw))
	if err != nil {
		t.Fatal(err)
	}
	s := seg.(*Segment)
	t.Logf("file=%d docs=%d version=%d", len(raw), s.footer.numDocs, s.footer.version)
	t.Logf("stored chunks=%d stored index=%d", s.footer.storedIndexOffset, storedIndexLen(s))
	total := 0
	for id, name := range s.fieldsInv {
		fst := 0
		if loc := s.dictLocs[id]; loc != 0 {
			n, err := readDataUvarint(s.data, &loc)
			if err != nil {
				t.Fatal(err)
			}
			fst = int(n) // #nosec G115 -- bounded by the file size.
		}
		dv := s.dvLen(id)
		total += fst + dv
		t.Logf("field %-10s fst=%d dv=%d", name, fst, dv)
	}
	// #nosec G115 -- offsets bounded by the file size.
	postings := int(s.footer.docValueOffset) - int(s.footer.storedIndexOffset) - storedIndexLen(s) - total
	t.Logf("postings (rest before dv index) ~ %d", postings)
}

func storedIndexLen(s *Segment) int {
	if s.footer.version >= versionNoStoredIndex {
		return 0
	}
	return 8 * int(s.footer.numDocs) // #nosec G115 -- a document count.
}

// dvLen is the byte length of field id's doc values.
func (s *Segment) dvLen(id int) int {
	if s.footer.docValueOffset == fieldNotUninverted {
		return 0
	}
	pos := s.footer.docValueOffset
	for f := 0; f <= id; f++ {
		var v [3]uint64 // kind, start, end
		for i := range v {
			var err error
			if v[i], err = readDataUvarint(s.data, &pos); err != nil {
				return -1
			}
		}
		if f == id {
			if v[1] == fieldNotUninverted {
				return 0
			}
			return int(v[2] - v[1]) // #nosec G115 -- bounded by the file size.
		}
	}
	return 0
}

// TestPostingsBreakdown prints, for the segment files named by ICE_SEG_FILE
// (os.PathListSeparator separated), how many bytes each part takes in total:
// stored chunks, FSTs, doc values and, per field, the parts of its postings.
//
//nolint:gocyclo,funlen // one walk over every part of a segment, a measuring aid.
func TestPostingsBreakdown(t *testing.T) {
	paths := os.Getenv("ICE_SEG_FILE")
	if paths == "" {
		t.Skip("ICE_SEG_FILE not set")
	}
	type part struct {
		terms, oneHit, postings, locs                                      int
		head, bm, tfHead, tfData, locHead, locData, locRaw, offData, offNZ int
		fst, dv                                                            int
	}
	parts := map[string]*part{}
	var files, size, docs, stored int
	for _, path := range filepath.SplitList(paths) {
		raw, err := os.ReadFile(path) // #nosec G304 G703 -- a file the developer names.
		if err != nil {
			t.Fatal(err)
		}
		seg, err := Load(segment.NewDataBytes(raw))
		if err != nil {
			t.Logf("skip %s (%d bytes): %v", filepath.Base(path), len(raw), err)
			continue
		}
		s := seg.(*Segment)
		files++
		size += len(raw)
		docs += int(s.footer.numDocs)                                 // #nosec G115 -- a document count.
		stored += int(s.footer.storedIndexOffset) + storedIndexLen(s) // #nosec G115 -- bounded by the file size.
		for id, name := range s.fieldsInv {
			p := parts[name]
			if p == nil {
				p = &part{}
				parts[name] = p
			}
			p.dv += s.dvLen(id)
			d, err := s.dictionary(name)
			if err != nil {
				t.Fatal(err)
			}
			if d.fst == nil {
				continue
			}
			loc := s.dictLocs[id]
			n, err := readDataUvarint(s.data, &loc)
			if err != nil {
				t.Fatal(err)
			}
			p.fst += int(n) // #nosec G115 -- bounded by the file size.
			itr, err := d.fst.Iterator(nil, nil)
			for ; err == nil; err = itr.Next() {
				_, val := itr.Current()
				p.terms++
				if val&fSTValEncodingMask == fSTValEncoding1Hit {
					p.oneHit++
					continue
				}
				var pl PostingsList
				pl.sb = s
				if err := pl.read(val, d); err != nil {
					t.Fatal(err)
				}
				p.postings += int(pl.postings.GetCardinality()) // #nosec G115 -- a document count.
				pos := val
				for range 3 {
					if _, err := readDataUvarint(s.data, &pos); err != nil {
						t.Fatal(err)
					}
				}
				bmLen, err := readDataUvarint(s.data, &pos)
				if err != nil {
					t.Fatal(err)
				}
				if s.footer.version >= versionSlimPostings {
					bmLen >>= 1 // the low bit flags a delta list
				}
				p.head += int(pos - val) // #nosec G115 -- a few bytes.
				p.bm += int(bmLen)       // #nosec G115 -- bounded by the file size.
				for k, off := range []uint64{pl.freqOffset, pl.locOffset, pl.offOffset} {
					if off == 0 {
						continue
					}
					dec, err := newChunkedIntDecoder(s.data, off, nil, k == 0, true)
					if err != nil {
						t.Fatal(err)
					}
					h := int(dec.dataStartOffset - off) // #nosec G115 -- a header length.
					n := 0
					if len(dec.chunkOffsets) > 0 {
						n = int(dec.chunkOffsets[len(dec.chunkOffsets)-1]) // #nosec G115 -- bounded by the file size.
					}
					switch k {
					case 0:
						p.tfHead += h
						p.tfData += n
					case 1:
						p.locHead += h
						p.locData += n
						for c := range dec.chunkOffsets {
							if err := dec.loadChunk(c); err != nil {
								t.Fatal(err)
							}
							p.locRaw += len(dec.curChunkBytes)
						}
					default:
						p.offData += h + n
						for c := range dec.chunkOffsets {
							if err := dec.loadChunk(c); err != nil {
								t.Fatal(err)
							}
							for r := dec.r; r.Len() > 0; {
								n, err := r.ReadUvarint()
								if err != nil {
									t.Fatal(err)
								}
								for _, b := range r.S[r.C : r.C+int(n)] { // #nosec G115 -- a record length.
									if b != 0 {
										p.offNZ++
									}
								}
								p.locs += int(n) / 2 // #nosec G115 -- a record length.
								r.SkipBytes(int(n))  // #nosec G115 -- a record length.
							}
						}
					}
				}
			}
		}
	}
	t.Logf("files=%d bytes=%d docs=%d (%d/doc) stored=%d (%d/doc)", files, size, docs, size/docs, stored, stored/docs)
	for name, p := range parts {
		sum := p.fst + p.dv + p.head + p.bm + p.tfHead + p.tfData + p.locHead + p.locData + p.offData
		t.Logf("%-16s total=%d (%d/doc) terms=%d 1hit=%d postings=%d locs=%d | fst=%d dv=%d header=%d bitmap=%d "+
			"tfHead=%d tfData=%d locHead=%d locData=%d (raw %d) off=%d (nonzero bytes %d)",
			name, sum, sum/docs, p.terms, p.oneHit, p.postings, p.locs, p.fst, p.dv, p.head, p.bm,
			p.tfHead, p.tfData, p.locHead, p.locData, p.locRaw, p.offData, p.offNZ)
	}
}

// TestPostingsEstimate prints, per field of the ICE_SEG_FILE segments, what
// other encodings of the doc lists and location records would take.
//
//nolint:gocyclo,funlen // one walk over every posting, a measuring aid.
func TestPostingsEstimate(t *testing.T) {
	paths := os.Getenv("ICE_SEG_FILE")
	if paths == "" {
		t.Skip("ICE_SEG_FILE not set")
	}
	uvLen := func(v uint64) int {
		n := 1
		for ; v >= 0x80; v >>= 7 {
			n++
		}
		return n
	}
	type est struct {
		terms, postings, locs, ownLocs, backward  int
		bm, list, best                            int
		byCard                                    [5]int // terms with card <=4, <=16, <=64, <=1024, more
		bmByCard, listByCard                      [5]int
		locNow, locNoLen, locNoField, locUnsigned int
	}
	ests := map[string]*est{}
	for _, path := range filepath.SplitList(paths) {
		raw, err := os.ReadFile(path) // #nosec G304 G703 -- a file the developer names.
		if err != nil {
			t.Fatal(err)
		}
		seg, err := Load(segment.NewDataBytes(raw))
		if err != nil {
			continue
		}
		s := seg.(*Segment)
		for _, name := range s.fieldsInv {
			d, err := s.dictionary(name)
			if err != nil {
				t.Fatal(err)
			}
			if d.fst == nil {
				continue
			}
			e := ests[name]
			if e == nil {
				e = &est{}
				ests[name] = e
			}
			itr, err := d.fst.Iterator(nil, nil)
			for ; err == nil; err = itr.Next() {
				_, val := itr.Current()
				if val&fSTValEncodingMask == fSTValEncoding1Hit {
					continue
				}
				pl, err := d.postingsListFromOffset(val, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				e.terms++
				card := int(pl.postings.GetCardinality()) // #nosec G115 -- a document count.
				e.postings += card
				bm := int(pl.postings.GetSerializedSizeInBytes()) // #nosec G115 -- a byte count.
				bm += uvLen(uint64(bm))                           // #nosec G115 -- see above.
				list := uvLen(uint64(card))                       // #nosec G115 -- see above.
				var prev uint32
				for it := pl.postings.Iterator(); it.HasNext(); {
					v := it.Next()
					list += uvLen(uint64(v - prev))
					prev = v
				}
				list += uvLen(uint64(list)) // #nosec G115 -- a byte count.
				k := 4
				for i, lim := range []int{4, 16, 64, 1024} {
					if card <= lim {
						k = i
						break
					}
				}
				e.byCard[k]++
				e.bmByCard[k] += bm
				e.listByCard[k] += list
				e.bm += bm
				e.list += list
				e.best += min(bm, list)

				pi, err := pl.iterator(true, true, true, nil)
				if err != nil {
					t.Fatal(err)
				}
				for {
					p, err := pi.Next()
					if err != nil {
						t.Fatal(err)
					}
					if p == nil {
						break
					}
					rec, noField, unsigned := 0, 0, 0
					prevPos, own, fwd := 0, true, true
					for _, l := range p.Locations() {
						e.locs++
						f := 1
						if l.Field() == name {
							e.ownLocs++
						} else {
							own = false
						}
						delta := l.Pos() - prevPos
						if delta < 0 {
							e.backward++
							fwd = false
						}
						z := uvLen(zigzag(int64(delta)))
						rec += f + z
						noField += z
						unsigned += uvLen(uint64(max(delta, 0))) // #nosec G115 -- clamped.
						prevPos = l.Pos()
					}
					if len(p.Locations()) == 0 {
						continue
					}
					e.locNow += uvLen(uint64(rec)) + rec // #nosec G115 -- a byte count.
					e.locNoLen += rec
					if !own {
						noField, unsigned = rec, rec
					} else if !fwd {
						unsigned = noField
					}
					e.locNoField += noField
					e.locUnsigned += unsigned
				}
			}
		}
	}
	for name, e := range ests {
		t.Logf("%-10s terms=%d postings=%d | doc lists: roaring=%d varint-list=%d best-of=%d | "+
			"terms by card (<=4,<=16,<=64,<=1024,more)=%v roaring=%v list=%v",
			name, e.terms, e.postings, e.bm, e.list, e.best, e.byCard, e.bmByCard, e.listByCard)
		t.Logf("%-10s locs=%d own-field=%d backward=%d | raw loc bytes: now=%d no-length=%d no-length-no-own-field=%d +unsigned-pos=%d",
			name, e.locs, e.ownLocs, e.backward, e.locNow, e.locNoLen, e.locNoField, e.locUnsigned)
	}
}
