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

package ice

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"

	"github.com/vcaesar/ice/vec"
)

func vectorField(t *testing.T, values ...float32) *FakeField {
	t.Helper()
	data, err := vec.Encode(values)
	if err != nil {
		t.Fatal(err)
	}
	return &FakeField{N: "vector", V: data, S: true}
}

func vectorSegment(t *testing.T, docs ...FakeDocument) *Segment {
	t.Helper()
	input := make([]segment.Document, len(docs))
	for i := range docs {
		input[i] = &docs[i]
	}
	s, _, err := New(input, encodeNorm)
	if err != nil {
		t.Fatal(err)
	}
	return s.(*Segment)
}

func checkVectorMatches(t *testing.T, s *Segment, k int, accept func(uint64) bool, want []vec.Match) {
	t.Helper()
	got, err := s.SearchVectors(context.Background(), "vector", []float32{1, 0}, k, vec.DotProduct, accept)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("matches %v want %v: %v", got, want, err)
	}
}

func TestSearchVectorsPersistenceMerge(t *testing.T) {
	s := vectorSegment(t,
		FakeDocument{vectorField(t, 1, 0), vectorField(t, 3, 0)},
		FakeDocument{vectorField(t, 3, 0)},
		FakeDocument{vectorField(t, 5, 0)},
		FakeDocument{&FakeField{N: "other", V: []byte("not a vector"), S: true}},
	)
	want := []vec.Match{{Number: 2, Score: 5}, {Number: 0, Score: 3}, {Number: 1, Score: 3}}
	for _, k := range []int{1, 2, 3, 100} {
		n := k
		if n > len(want) {
			n = len(want)
		}
		checkVectorMatches(t, s, k, nil, want[:n])
	}
	checkVectorMatches(t, s, 10, func(n uint64) bool { return n != 2 }, want[1:])
	path := t.TempDir()
	diskPath := filepath.Join(path, "vector.ice")
	if err := persistToFile(s, diskPath); err != nil {
		t.Fatal(err)
	}
	reopened, closeFile, err := openFromFile(diskPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := closeFile(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	checkVectorMatches(t, reopened, 10, nil, want)
	if reopened.Version() != Version {
		t.Fatal("segment version changed")
	}
	second := vectorSegment(t, FakeDocument{vectorField(t, 4, 0)})
	mergedPath := filepath.Join(path, "merged.ice")
	if _, err = mergeSegments([]segment.Segment{reopened, second}, []*roaring.Bitmap{roaring.BitmapOf(2), nil}, mergedPath); err != nil {
		t.Fatal(err)
	}
	merged, closeMerged, err := openFromFile(mergedPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := closeMerged(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	checkVectorMatches(t, merged, 10, nil, []vec.Match{{Number: 3, Score: 4}, {Number: 0, Score: 3}, {Number: 1, Score: 3}})
}

func TestSearchVectorsValidation(t *testing.T) {
	for _, bad := range []*FakeField{
		{N: "vector", V: []byte("invalid"), S: true}, vectorField(t, 1), vectorField(t, 0, 0),
	} {
		s := vectorSegment(t, FakeDocument{vectorField(t, 1, 0), bad})
		if _, err := s.SearchVectors(context.Background(), "vector", []float32{1, 0}, 1, vec.Cosine, nil); err == nil {
			t.Fatal("invalid repeated value accepted")
		}
		got, err := s.SearchVectors(context.Background(), "vector", []float32{1, 0}, 1, vec.Cosine, func(uint64) bool { return false })
		if err != nil || len(got) != 0 {
			t.Fatalf("prefilter should skip invalid values: %v %v", got, err)
		}
	}
	s := vectorSegment(t, FakeDocument{vectorField(t, 1, 0)})
	for _, k := range []int{0, -1} {
		if _, err := s.SearchVectors(context.Background(), "vector", []float32{1, 0}, k, vec.DotProduct, nil); err == nil {
			t.Fatal("invalid k")
		}
	}
	for _, query := range [][]float32{nil, {0, 0}} {
		if _, err := s.SearchVectors(context.Background(), "missing", query, 1, vec.Cosine, nil); err == nil {
			t.Fatal("invalid query on missing field")
		}
	}
	if _, err := s.SearchVectors(context.Background(), "missing", []float32{1}, 1, vec.Metric("bad"), nil); err == nil {
		t.Fatal("invalid metric")
	}
	for _, source := range []*Segment{s, vectorSegment(t)} {
		got, err := source.SearchVectors(context.Background(), "missing", []float32{1}, 1, vec.DotProduct, nil)
		if err != nil || len(got) != 0 {
			t.Fatalf("missing field: %v %v", got, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SearchVectors(ctx, "vector", []float32{1, 0}, 1, vec.DotProduct, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	if _, err := s.SearchVectors(ctx, "vector", []float32{1, 0}, 1, vec.DotProduct,
		func(uint64) bool { cancel(); return false }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel during filter: %v", err)
	}
}

func TestSearchVectorsTopKAgainstFullSort(t *testing.T) {
	var docs []FakeDocument
	var all []vec.Match
	for i := 0; i < 137; i++ {
		score := float32((i*37)%23 - 11)
		docs = append(docs, FakeDocument{vectorField(t, score, 0)})
		all = append(all, vec.Match{Number: uint64(i), Score: float64(score)})
	}
	s := vectorSegment(t, docs...)
	for _, filtered := range []bool{false, true} {
		accept := func(n uint64) bool { return !filtered || n%3 != 0 }
		var want []vec.Match
		for _, match := range all {
			if accept(match.Number) {
				want = append(want, match)
			}
		}
		sort.Slice(want, func(i, j int) bool {
			if want[i].Score == want[j].Score {
				return want[i].Number < want[j].Number
			}
			return want[i].Score > want[j].Score
		})
		for _, k := range []int{1, 2, 7, 32, 137, 1000} {
			n := k
			if n > len(want) {
				n = len(want)
			}
			checkVectorMatches(t, s, k, accept, want[:n])
		}
	}
}

func TestSearchVectorsMetrics(t *testing.T) {
	s := vectorSegment(t, FakeDocument{vectorField(t, -1, 0)}, FakeDocument{vectorField(t, 1, 1)}, FakeDocument{vectorField(t, 1, 0)})
	for _, metric := range []vec.Metric{vec.Cosine, vec.L2} {
		got, err := s.SearchVectors(context.Background(), "vector", []float32{1, 0}, 2, metric, nil)
		if err != nil || len(got) != 2 || got[0].Number != 2 || got[1].Number != 1 {
			t.Fatalf("%s: %v %v", metric, got, err)
		}
	}
}
