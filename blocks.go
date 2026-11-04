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

const (
	blockBits = 13
	blockLen  = 1 << blockBits
)

// blocks is an append-only list kept in blocks of blockLen entries, so
// growing it never copies what it holds, nor holds it twice. The first
// block grows as a slice, so a short list stays small.
type blocks[T any] struct {
	b [][]T
	n int
}

func (l *blocks[T]) len() int { return l.n }

func (l *blocks[T]) append(v T) {
	switch {
	case l.n == 0:
		l.b = append(l.b, nil)
	case l.n&(blockLen-1) == 0:
		l.b = append(l.b, make([]T, 0, blockLen))
	}
	last := &l.b[len(l.b)-1]
	*last = append(*last, v)
	l.n++
}

func (l *blocks[T]) at(i int) T { return l.b[i>>blockBits][i&(blockLen-1)] }

// ptr returns the address of entry i, valid until the next append.
func (l *blocks[T]) ptr(i int) *T { return &l.b[i>>blockBits][i&(blockLen-1)] }

// cap returns the entries l has room for.
func (l *blocks[T]) cap() int {
	if len(l.b) == 0 {
		return 0
	}
	return (len(l.b)-1)*blockLen + cap(l.b[len(l.b)-1])
}

// byteBlockLen is the size of a full byteBlocks block.
const byteBlockLen = 64 << 10

// byteBlocks holds byte strings back to back in blocks that double up to
// byteBlockLen bytes, a longer string taking a block of its own, so
// growing never copies what it holds. byteReader reads them back in order.
type byteBlocks struct {
	b   [][]byte
	cap int
}

// append appends v and returns its copy.
func (l *byteBlocks) append(v []byte) []byte {
	n := len(l.b)
	if n == 0 || cap(l.b[n-1])-len(l.b[n-1]) < len(v) {
		c := 256
		if n > 0 {
			c = 2 * cap(l.b[n-1])
		}
		l.b = append(l.b, make([]byte, 0, max(min(c, byteBlockLen), len(v))))
		l.cap += cap(l.b[n])
		n++
	}
	at := len(l.b[n-1])
	l.b[n-1] = append(l.b[n-1], v...)
	return l.b[n-1][at:]
}

type byteReader struct {
	l       *byteBlocks
	blk, at int
}

// next returns the next string, of n bytes.
func (r *byteReader) next(n int) []byte {
	if r.at+n > len(r.l.b[r.blk]) {
		r.blk, r.at = r.blk+1, 0
	}
	v := r.l.b[r.blk][r.at : r.at+n]
	r.at += n
	return v
}
