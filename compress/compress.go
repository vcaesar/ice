/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package compress

import (
	"errors"
	"fmt"
	"strings"
)

const (
	SNAPPY = iota
	S2
	ZSTD
	MINLZ
	LZ4
)

// Algorithm is the codec Compress and Decompress use. Segments do not
// record it, so it must stay the same for an index's lifetime.
var Algorithm = S2

// algorithms maps the names SetAlgorithm accepts to their codec.
var algorithms = map[string]int{
	"snappy": SNAPPY,
	"s2":     S2,
	"zstd":   ZSTD,
	"minlz":  MINLZ,
	"lz4":    LZ4,
}

// SetAlgorithm sets Algorithm by name (snappy, s2, zstd, minlz or lz4,
// case insensitive), as read from user configuration.
func SetAlgorithm(name string) error {
	algorithm, ok := algorithms[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return fmt.Errorf("unknown compress algorithm %q", name)
	}
	Algorithm = algorithm
	return nil
}

func Decompress(dst, src []byte) ([]byte, error) {
	switch Algorithm {
	case SNAPPY:
		return SnappyDecompress(dst, src)
	case S2:
		return S2Decompress(dst, src)
	case ZSTD:
		return ZSTDDecompress(dst, src)
	case MINLZ:
		return MinLZDecompress(dst, src)
	case LZ4:
		return LZ4Decompress(dst, src)
	default:
		return nil, errors.New("unknown compress algorithm")
	}
}

func Compress(dst, src []byte) ([]byte, error) {
	switch Algorithm {
	case SNAPPY:
		return SnappyCompress(dst, src)
	case S2:
		return S2Compress(dst, src)
	case ZSTD:
		return ZSTDCompress(dst, src, ZSTDCompressionLevel)
	case MINLZ:
		return MinLZCompress(dst, src, MinLZCompressionLevel)
	case LZ4:
		return LZ4Compress(dst, src)
	default:
		return nil, errors.New("unknown compress algorithm")
	}
}
