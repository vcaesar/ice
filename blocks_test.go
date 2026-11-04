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
	"testing"
)

func TestBlocks(t *testing.T) {
	var l blocks[int]
	if l.len() != 0 || l.cap() != 0 {
		t.Fatalf("empty: len %d cap %d", l.len(), l.cap())
	}
	l.append(7)
	if c := l.cap(); c < 1 || c >= blockLen {
		t.Fatalf("one entry: cap %d, want a small first block", c)
	}
	n := 3*blockLen + 5
	for i := 1; i < n; i++ {
		l.append(7 + i)
	}
	if l.len() != n || len(l.b) != 4 {
		t.Fatalf("len %d in %d blocks, want %d in 4", l.len(), len(l.b), n)
	}
	for i := range n {
		if v := l.at(i); v != 7+i {
			t.Fatalf("at(%d) = %d", i, v)
		}
	}
	if c := l.cap(); c != 4*blockLen {
		t.Fatalf("cap %d, want %d", c, 4*blockLen)
	}
}

func TestByteBlocks(t *testing.T) {
	var l byteBlocks
	var want [][]byte
	for i := range 3000 {
		n := (i * 37) % 300
		switch i % 500 {
		case 7:
			n = 0
		case 11:
			n = 3 * byteBlockLen // a block of its own
		}
		v := bytes.Repeat([]byte{byte(i)}, n)
		want = append(want, v)
		l.append(v)
	}
	if first := cap(l.b[0]); first != 256 {
		t.Fatalf("first block %d bytes, want 256", first)
	}
	total := 0
	for _, blk := range l.b {
		total += cap(blk)
	}
	if l.cap != total {
		t.Fatalf("cap %d, blocks hold %d", l.cap, total)
	}
	r := byteReader{l: &l}
	for i, v := range want {
		if got := r.next(len(v)); !bytes.Equal(got, v) {
			t.Fatalf("value %d: %d bytes, want %d", i, len(got), len(v))
		}
	}
}
