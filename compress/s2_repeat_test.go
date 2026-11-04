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
	"bytes"
	"math/rand/v2"
	"testing"

	s2c "github.com/klauspost/compress/s2"
)

// repeatInputs are blocks of every shape s2 encodes: literals of each
// length size, short and long overlapping copies (the postings chunks of a
// term in every document), repeats and far copies.
func repeatInputs() map[string][]byte {
	rng := rand.New(rand.NewPCG(1, 2)) // #nosec G404 -- deterministic test data.
	random := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.Uint32()) // #nosec G115 -- any byte.
		}
		return b
	}
	var mixed []byte
	for i := range 300 {
		mixed = append(mixed, random(rng.IntN(40))...)
		mixed = append(mixed, bytes.Repeat([]byte{byte(i), 7, 9}[:1+i%3], rng.IntN(300))...) // #nosec G115 -- any byte.
		if i%50 == 0 {
			mixed = append(mixed, mixed[:rng.IntN(len(mixed)+1)]...) // far copies
		}
	}
	return map[string][]byte{
		"empty":     nil,
		"one":       {42},
		"freqNorm":  bytes.Repeat([]byte{3, 60}, 1020),
		"locs":      bytes.Repeat([]byte{2, 1, 4}, 1020),
		"byte":      make([]byte, 1<<17),
		"period7":   bytes.Repeat([]byte("abcdefg"), 5000),
		"period9":   bytes.Repeat([]byte("abcdefghi"), 5000),
		"random":    random(70000), // literals with 1-3 length bytes
		"mixed":     mixed,
		"repeatEnd": append(random(100), bytes.Repeat([]byte{5, 6}, 40000)...),
	}
}

func TestDecodeRepeatsMatchesS2(t *testing.T) {
	encoders := map[string]func([]byte, []byte) []byte{
		"encode": s2c.Encode, "better": s2c.EncodeBetter, "best": s2c.EncodeBest, "snappy": s2c.EncodeSnappy,
	}
	for name, src := range repeatInputs() {
		for ename, enc := range encoders {
			block := enc(nil, src)
			for _, dst := range [][]byte{nil, make([]byte, 0, len(src)+64)} {
				got, err := decodeRepeats(dst, block)
				if err != nil {
					t.Fatalf("%s/%s: %v", name, ename, err)
				}
				if !bytes.Equal(got, src) {
					t.Fatalf("%s/%s: decoded %d bytes differ from the %d encoded", name, ename, len(got), len(src))
				}
			}
			got, err := S2Decompress(nil, block)
			if err != nil || !bytes.Equal(got, src) {
				t.Fatalf("%s/%s: S2Decompress: %v", name, ename, err)
			}
		}
	}
}

// Corrupt blocks must fail as they do for s2c.Decode, never panic, and a
// block both accept must decode to the same bytes.
func TestDecodeRepeatsCorrupt(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4)) // #nosec G404 -- deterministic test data.
	for name, src := range repeatInputs() {
		block := s2c.EncodeBetter(nil, src)
		for range 2000 {
			bad := bytes.Clone(block)
			switch rng.IntN(3) {
			case 0:
				if len(bad) > 0 {
					bad[rng.IntN(len(bad))] ^= byte(1 + rng.IntN(255)) // #nosec G115 -- 1 to 255.
				}
			case 1:
				bad = bad[:rng.IntN(len(bad)+1)]
			default:
				bad = append(bad, byte(rng.Uint32())) // #nosec G115 -- any byte.
			}
			want, wantErr := s2c.Decode(nil, bad)
			got, err := decodeRepeats(nil, bad)
			if (err != nil) != (wantErr != nil) {
				t.Fatalf("%s: error %v, s2 error %v", name, err, wantErr)
			}
			if err == nil && !bytes.Equal(got, want) {
				t.Fatalf("%s: decoded bytes differ from s2", name)
			}
		}
	}
}

func BenchmarkS2Decompress(b *testing.B) {
	inputs := repeatInputs()
	for _, name := range []string{"freqNorm", "locs", "period9", "mixed", "random"} {
		block := s2c.EncodeBetter(nil, inputs[name])
		dst := make([]byte, len(inputs[name]))
		for _, dec := range []struct {
			name string
			fn   func([]byte, []byte) ([]byte, error)
		}{{"s2", s2c.Decode}, {"repeats", decodeRepeats}} {
			b.Run(name+"/"+dec.name, func(b *testing.B) {
				b.SetBytes(int64(len(dst)))
				for b.Loop() {
					if _, err := dec.fn(dst, block); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
