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
	"encoding/binary"
	"sync"
)

// The Builder records each term's postings as they are added, varint
// encoded, the way Tantivy's and riot-go's indexing recorders do: into a
// chain of blocks of a termArena, each block twice the previous up to
// maxTermBlock bytes and ending with the address of the next one.
const (
	firstTermBlockShift = 4
	maxTermBlockShift   = 15
	arenaPageBits       = 20 // a page holds up to 1 MiB of blocks
	maxArenaPages       = 1 << (32 - arenaPageBits)
	firstArenaPage      = 4 << 10
	blockLinkLen        = 4
)

// termArena allocates the term blocks in pages that double up to
// 1<<arenaPageBits bytes; an address is a page and an offset in it. Full
// size pages are recycled through arenaPages, see release.
type termArena struct {
	pages [][]byte
	size  int // bytes the pages take
}

var arenaPages = sync.Pool{New: func() any { return make([]byte, 0, 1<<arenaPageBits) }}

// alloc returns the address of n zero bytes, n at most 1<<maxTermBlockShift,
// and false when the arena is full.
func (a *termArena) alloc(n int) (uint32, bool) {
	p := len(a.pages) - 1
	if p < 0 || cap(a.pages[p])-len(a.pages[p]) < n {
		if len(a.pages) == maxArenaPages {
			return 0, false
		}
		c := firstArenaPage
		if p >= 0 {
			c = min(2*cap(a.pages[p]), 1<<arenaPageBits)
		}
		var page []byte
		if c == 1<<arenaPageBits {
			page = arenaPages.Get().([]byte)
			clear(page[:cap(page)])
		} else {
			page = make([]byte, 0, c)
		}
		a.pages = append(a.pages, page)
		a.size += c
		p++
	}
	off := len(a.pages[p])
	a.pages[p] = a.pages[p][:off+n]
	return uint32(p)<<arenaPageBits | uint32(off), true // #nosec G115 -- bounded by maxArenaPages and the page size.
}

// release empties a, giving its full size pages to the next Builders:
// nothing may refer into a afterwards.
func (a *termArena) release() {
	for _, p := range a.pages {
		if cap(p) == 1<<arenaPageBits {
			arenaPages.Put(p[:0])
		}
	}
	a.pages, a.size = nil, 0
}

// at returns the bytes of the page from address addr on.
func (a *termArena) at(addr uint32) []byte {
	return a.pages[addr>>arenaPageBits][addr&(1<<arenaPageBits-1):]
}

// termStream is the write end of a term's chain of blocks.
type termStream struct {
	head    uint32 // first block
	tail    uint32 // next byte to write
	lastDoc uint32 // of the last posting, which the next is a delta to
	left    uint16 // bytes left in the current block, its link excluded
	shift   uint8  // log2 of the current block's size
}

// newTermStream starts a chain; false when the arena is full.
func (a *termArena) newTermStream() (termStream, bool) {
	addr, ok := a.alloc(1 << firstTermBlockShift)
	return termStream{head: addr, tail: addr, left: 1<<firstTermBlockShift - blockLinkLen,
		shift: firstTermBlockShift}, ok
}

// write appends p to t's chain; false when the arena is full.
func (a *termArena) write(t *termStream, p []byte) bool {
	for len(p) > 0 {
		if t.left == 0 {
			shift := min(t.shift+1, maxTermBlockShift)
			next, ok := a.alloc(1 << shift)
			if !ok {
				return false
			}
			binary.LittleEndian.PutUint32(a.at(t.tail), next)
			t.tail, t.left, t.shift = next, 1<<shift-blockLinkLen, shift
		}
		n := copy(a.at(t.tail)[:t.left], p)
		t.tail += uint32(n) // #nosec G115 -- at most a block.
		t.left -= uint16(n) // #nosec G115 -- see above.
		p = p[n:]
	}
	return true
}

// termReader reads a term's chain of blocks back.
type termReader struct {
	a     *termArena
	addr  uint32 // of the current block
	blk   []byte // the current block, its link included
	i     int
	shift uint8
}

func (a *termArena) reader(t *termStream) termReader {
	return termReader{a: a, addr: t.head, blk: a.at(t.head)[:1<<firstTermBlockShift], shift: firstTermBlockShift}
}

// at reports whether r has read up to address tail.
func (r *termReader) at(tail uint32) bool {
	return r.addr+uint32(r.i) == tail // #nosec G115 -- at most a block.
}

func (r *termReader) byte() byte {
	if r.i == len(r.blk)-blockLinkLen {
		r.addr = binary.LittleEndian.Uint32(r.blk[r.i:])
		r.shift = min(r.shift+1, maxTermBlockShift)
		r.blk, r.i = r.a.at(r.addr)[:1<<r.shift], 0
	}
	b := r.blk[r.i]
	r.i++
	return b
}

func (r *termReader) uvarint() uint64 {
	var v uint64
	for s := uint(0); ; s += 7 {
		b := r.byte()
		if b < 0x80 {
			return v | uint64(b)<<s
		}
		v |= uint64(b&0x7f) << s
	}
}

func (r *termReader) varint() int64 { return unzigzag(r.uvarint()) }
