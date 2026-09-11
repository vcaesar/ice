// Copyright (c) 2026 The Riot Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package vec encodes stored vectors and computes exact similarity scores.
package vec

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Metric selects a similarity function. All scores are higher-is-better.
type Metric string

const (
	Cosine     Metric = "cosine"
	DotProduct Metric = "dot_product"
	L2         Metric = "l2"
)

// Match identifies a segment-local document and its similarity score.
type Match struct {
	Number uint64
	Score  float64
}

const magic = "ICEV"
const headerSize = 9 // magic, version byte, little-endian uint32 dimension
const float32Size = 4

// Encode serializes finite, nonempty float32 vectors without metric normalization.
func Encode(vector []float32) ([]byte, error) {
	if err := Validate(vector, DotProduct); err != nil {
		return nil, err
	}
	if uint64(len(vector)) > math.MaxUint32 || len(vector) > (int(^uint(0)>>1)-headerSize)/4 {
		return nil, fmt.Errorf("vector dimension too large")
	}
	data := make([]byte, headerSize+4*len(vector))
	copy(data, magic)
	data[4] = 1
	binary.LittleEndian.PutUint32(data[5:9], uint32(len(vector))) // #nosec G115 -- len(vector) <= MaxUint32 above.
	for i, v := range vector {
		binary.LittleEndian.PutUint32(data[headerSize+4*i:], math.Float32bits(v))
	}
	return data, nil
}

// Decode strictly validates the header, exact payload length, and finite values.
// The returned vector owns its memory.
func Decode(data []byte) ([]float32, error) {
	if len(data) < headerSize || string(data[:4]) != magic || data[4] != 1 {
		return nil, fmt.Errorf("invalid vector header or version")
	}
	n := uint64(binary.LittleEndian.Uint32(data[5:9]))
	// #nosec G115 -- the header check above guarantees len(data) >= headerSize.
	if n == 0 || n*4 != uint64(len(data)-headerSize) {
		return nil, fmt.Errorf("invalid vector payload length")
	}
	vector := make([]float32, (len(data)-headerSize)/float32Size)
	for i := range vector {
		vector[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[headerSize+4*i:]))
	}
	if err := Validate(vector, DotProduct); err != nil {
		return nil, err
	}
	return vector, nil
}

// Validate rejects unsupported metrics, empty or nonfinite vectors, and zero
// vectors for cosine similarity. Zero vectors are valid for dot product and L2.
func Validate(vector []float32, metric Metric) error {
	switch metric {
	case Cosine, DotProduct, L2:
	default:
		return fmt.Errorf("unsupported vector metric %q", metric)
	}
	if len(vector) == 0 {
		return fmt.Errorf("vector must not be empty")
	}
	nonzero := false
	for i, v := range vector {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return fmt.Errorf("nonfinite vector value at dimension %d", i)
		}
		nonzero = nonzero || v != 0
	}
	if metric == Cosine && !nonzero {
		return fmt.Errorf("cosine requires a nonzero vector")
	}
	return nil
}

// Score computes cosine similarity, dot product, or negative squared L2 distance.
// Arithmetic is float64, including multiplication and subtraction, to avoid
// float32 overflow and underflow for finite float32 inputs.
func Score(query, vector []float32, metric Metric) (float64, error) {
	if err := Validate(query, metric); err != nil {
		return 0, err
	}
	if err := Validate(vector, metric); err != nil {
		return 0, err
	}
	if len(query) != len(vector) {
		return 0, fmt.Errorf("vector dimension mismatch: query %d, vector %d", len(query), len(vector))
	}
	var dot, qnorm, vnorm, distance float64
	for i, q := range query {
		a, b := float64(q), float64(vector[i])
		switch metric {
		case DotProduct:
			dot += a * b
		case Cosine:
			dot += a * b
			qnorm += a * a
			vnorm += b * b
		case L2:
			d := a - b
			distance += d * d
		}
	}
	switch metric {
	case Cosine:
		return math.Max(-1, math.Min(1, dot/(math.Sqrt(qnorm)*math.Sqrt(vnorm)))), nil
	case L2:
		return -distance, nil
	default:
		return dot, nil
	}
}
