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

package compress

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/pierrec/lz4/v4"
)

// lz4MaxRatio bounds the decoded size an lz4 block of n bytes can claim:
// each length byte extends a match by at most 255 bytes.
const lz4MaxRatio = 256

var errLZ4Corrupt = errors.New("lz4: corrupt input")

// lz4.Compressor holds a hash table and is not safe for concurrent use.
var lz4Compressors = sync.Pool{New: func() any { return new(lz4.Compressor) }}

// LZ4Compress compresses a block using lz4 algorithm. The lz4 block format
// does not record the decoded size, so the block is prefixed with it as a
// uvarint.
func LZ4Compress(dst, src []byte) ([]byte, error) {
	if n := binary.MaxVarintLen64 + lz4.CompressBlockBound(len(src)); cap(dst) < n {
		dst = make([]byte, n)
	}
	dst = dst[:cap(dst)]
	h := binary.PutUvarint(dst, uint64(len(src)))
	if len(src) == 0 {
		return dst[:h], nil
	}
	c := lz4Compressors.Get().(*lz4.Compressor)
	n, err := c.CompressBlock(src, dst[h:])
	lz4Compressors.Put(c)
	if err != nil {
		return nil, err
	}
	return dst[:h+n], nil
}

// LZ4Decompress decompresses a block written by LZ4Compress.
func LZ4Decompress(dst, src []byte) ([]byte, error) {
	dLen, h := binary.Uvarint(src)
	if h <= 0 || dLen > lz4MaxRatio*uint64(len(src)) {
		return nil, errLZ4Corrupt
	}
	n := int(dLen) // #nosec G115 -- bounded by lz4MaxRatio*len(src).
	if n <= cap(dst) {
		dst = dst[:n]
	} else {
		dst = make([]byte, n)
	}
	if n == 0 {
		if h != len(src) {
			return nil, errLZ4Corrupt
		}
		return dst, nil
	}
	m, err := lz4.UncompressBlock(src[h:], dst)
	if err != nil {
		return nil, err
	}
	if m != n {
		return nil, errLZ4Corrupt
	}
	return dst, nil
}
