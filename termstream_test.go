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
	"math/rand/v2"
	"testing"
)

// Interleaved streams read back what was written to each, across blocks
// up to the largest and across arena pages.
func TestTermArena(t *testing.T) {
	var a termArena
	streams := make([]termStream, 50)
	want := make([][]uint64, len(streams))
	for i := range streams {
		var ok bool
		if streams[i], ok = a.newTermStream(); !ok {
			t.Fatal("arena full")
		}
	}
	rng := rand.New(rand.NewPCG(3, 4))
	var buf []byte
	for range 200000 {
		i := rng.IntN(len(streams))
		if i >= 40 { // a few long streams, the rest short
			i = 40 + rng.IntN(len(streams)-40)
		} else if rng.IntN(100) != 0 {
			i = 45
		}
		v := rng.Uint64() >> rng.IntN(64)
		buf = binary.AppendUvarint(buf[:0], v)
		if !a.write(&streams[i], buf) {
			t.Fatal("arena full")
		}
		want[i] = append(want[i], v)
	}
	if len(a.pages) < 3 {
		t.Fatalf("%d pages, want several", len(a.pages))
	}
	for i := range streams {
		r := a.reader(&streams[i])
		for j, v := range want[i] {
			if r.at(streams[i].tail) {
				t.Fatalf("stream %d ended after %d of %d values", i, j, len(want[i]))
			}
			if got := r.uvarint(); got != v {
				t.Fatalf("stream %d value %d: %d, want %d", i, j, got, v)
			}
		}
		if !r.at(streams[i].tail) {
			t.Fatalf("stream %d has more than its %d values", i, len(want[i]))
		}
	}
}

// A stream whose blocks fill exactly ends at the link of its last block.
func TestTermStreamFullBlock(t *testing.T) {
	var a termArena
	s, _ := a.newTermStream()
	data := make([]byte, 1<<firstTermBlockShift-blockLinkLen)
	for i := range data {
		data[i] = byte(i + 1)
	}
	if !a.write(&s, data) {
		t.Fatal("arena full")
	}
	if s.left != 0 {
		t.Fatalf("%d bytes left, want a full block", s.left)
	}
	r := a.reader(&s)
	for i := range data {
		if r.at(s.tail) {
			t.Fatalf("ended after %d bytes", i)
		}
		if b := r.byte(); b != data[i] {
			t.Fatalf("byte %d: %d", i, b)
		}
	}
	if !r.at(s.tail) {
		t.Fatal("did not end at the full block")
	}
}

// release gives an arena's full size pages to the next arenas, zeroed, so
// a stream of flushes does not allocate a fresh arena each time.
func TestTermArenaReleaseReusesPages(t *testing.T) {
	var a termArena
	s, _ := a.newTermStream()
	data := make([]byte, 1<<maxTermBlockShift)
	for i := range data {
		data[i] = 0xff
	}
	for len(a.pages) < 3 || cap(a.pages[len(a.pages)-1]) < 1<<arenaPageBits {
		if !a.write(&s, data) {
			t.Fatal("arena full")
		}
	}
	full := 0
	for _, p := range a.pages {
		if cap(p) == 1<<arenaPageBits {
			full++
		}
	}
	a.release()
	if a.size != 0 || len(a.pages) != 0 {
		t.Fatalf("after release: %d pages, size %d", len(a.pages), a.size)
	}
	var b termArena
	got := 0
	for len(b.pages) <= full {
		s, _ = b.newTermStream()
		for cap(b.pages[len(b.pages)-1]) < 1<<arenaPageBits {
			if !b.write(&s, data) {
				t.Fatal("arena full")
			}
		}
		p := b.pages[len(b.pages)-1]
		for _, c := range p[:cap(p)][len(p):] {
			if c != 0 {
				t.Fatal("a reused page was not zeroed")
			}
		}
		got++
		if !b.write(&s, data) {
			t.Fatal("arena full")
		}
	}
	if got == 0 {
		t.Fatal("no full page reached")
	}
}
