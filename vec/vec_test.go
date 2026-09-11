// Copyright (c) 2026 The Roit Authors.
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

package vec

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestCodec(t *testing.T) {
	vector := []float32{0, math.Float32frombits(1 << 31), 1, -2, math.MaxFloat32, math.SmallestNonzeroFloat32}
	data, err := Encode(vector)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	for i := range vector {
		if math.Float32bits(vector[i]) != math.Float32bits(decoded[i]) {
			t.Fatalf("dimension %d differs", i)
		}
	}
	again, err := Encode(decoded)
	if err != nil || !bytes.Equal(data, again) {
		t.Fatalf("roundtrip: %v", err)
	}
	decoded[0] = 9
	if binary.LittleEndian.Uint32(data[9:]) != 0 {
		t.Fatal("decode aliases input")
	}
	golden, err := Encode([]float32{1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(golden, []byte{'I', 'C', 'E', 'V', 1, 1, 0, 0, 0, 0, 0, 128, 63}) {
		t.Fatalf("wire format: %x", golden)
	}
}

func TestCodecRejectsMalformedData(t *testing.T) {
	data, err := Encode([]float32{1, -2})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(data); i++ {
		if _, decodeErr := Decode(data[:i]); decodeErr == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
	for _, kind := range []string{"magic", "version", "zero", "overflow", "trailing", "nan", "inf"} {
		t.Run(kind, func(t *testing.T) {
			bad := append([]byte(nil), data...)
			switch kind {
			case "magic":
				bad[0]++
			case "version":
				bad[4]++
			case "zero":
				binary.LittleEndian.PutUint32(bad[5:], 0)
			case "overflow":
				binary.LittleEndian.PutUint32(bad[5:], math.MaxUint32)
			case "trailing":
				bad = append(bad, 0)
			case "nan":
				binary.LittleEndian.PutUint32(bad[9:], math.Float32bits(float32(math.NaN())))
			case "inf":
				binary.LittleEndian.PutUint32(bad[9:], math.Float32bits(float32(math.Inf(1))))
			}
			if _, decodeErr := Decode(bad); decodeErr == nil {
				t.Fatal("accepted invalid data")
			}
		})
	}
}

func TestMetrics(t *testing.T) {
	for _, tc := range []struct {
		metric Metric
		want   float64
	}{{Cosine, 0.6}, {DotProduct, 3}, {L2, -20}} {
		score, err := Score([]float32{1, 0}, []float32{3, 4}, tc.metric)
		if err != nil || math.Abs(score-tc.want) > 1e-15 {
			t.Fatalf("%s: %g, %v", tc.metric, score, err)
		}
	}
	for _, metric := range []Metric{Cosine, DotProduct, L2} {
		for _, magnitude := range []float32{math.MaxFloat32, math.SmallestNonzeroFloat32} {
			a, b := []float32{magnitude, magnitude}, []float32{-magnitude, -magnitude}
			score, err := Score(a, b, metric)
			var want float64
			switch metric {
			case Cosine:
				want = -1
			case DotProduct:
				want = -2 * float64(magnitude) * float64(magnitude)
			case L2:
				want = -8 * float64(magnitude) * float64(magnitude)
			}
			if err != nil || math.IsInf(score, 0) || math.IsNaN(score) || math.Abs((score-want)/want) > 1e-14 {
				t.Fatalf("%s magnitude %g: %g want %g, %v", metric, magnitude, score, want, err)
			}
		}
	}
}

func TestInvalid(t *testing.T) {
	for _, v := range [][]float32{nil, {}, {float32(math.NaN())}, {float32(math.Inf(1))}, {float32(math.Inf(-1))}} {
		if _, err := Encode(v); err == nil {
			t.Fatalf("Encode accepted %v", v)
		}
		for _, metric := range []Metric{Cosine, DotProduct, L2} {
			if err := Validate(v, metric); err == nil {
				t.Fatal("Validate accepted invalid vector")
			}
			if _, err := Score(v, []float32{1}, metric); err == nil {
				t.Fatal("invalid query")
			}
			if _, err := Score([]float32{1}, v, metric); err == nil {
				t.Fatal("invalid vector")
			}
		}
	}
	for _, metric := range []Metric{Cosine, DotProduct, L2} {
		if _, err := Score([]float32{1}, []float32{1, 2}, metric); err == nil {
			t.Fatal("dimension mismatch")
		}
	}
	if err := Validate([]float32{1}, Metric("bad")); err == nil {
		t.Fatal("unknown metric")
	}
	if _, err := Score([]float32{1}, []float32{1}, Metric("bad")); err == nil {
		t.Fatal("unknown metric score")
	}
	if err := Validate([]float32{0, 0}, Cosine); err == nil {
		t.Fatal("zero cosine")
	}
	for _, metric := range []Metric{DotProduct, L2} {
		if score, err := Score([]float32{0}, []float32{0}, metric); err != nil || score != 0 {
			t.Fatalf("zero %s: %g %v", metric, score, err)
		}
	}
}
