// Copyright (c) 2026 The Bluge Authors.
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
	"bytes"
	"math"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/vcaesar/bluge_segment_api"
)

type timestampDocument struct {
	*FakeDocument
	timestamp int64
}

func (d timestampDocument) Timestamp() int64 { return d.timestamp }

func TestTimestampBounds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		times    []int64
		min, max int64
	}{
		{"empty", nil, 0, 0},
		{"positive", []int64{30, 10, 20}, 10, 30},
		{"negative", []int64{-30, -10, -20}, -30, -10},
		{"mixed", []int64{-10, 20}, -10, 20},
		{"maximum", []int64{math.MaxInt64}, math.MaxInt64, math.MaxInt64},
		{"minimum", []int64{math.MinInt64}, math.MinInt64, math.MinInt64},
		{"unknown first", []int64{0, 10}, 0, 0},
		{"unknown last", []int64{10, 0}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var docs []segment.Document
			var segments []*Segment
			for _, timestamp := range tc.times {
				doc := timestampDocument{&FakeDocument{NewFakeField("_id", "a", true, false, false)}, timestamp}
				docs = append(docs, doc)
				seg, _, err := New([]segment.Document{doc}, encodeNorm)
				if err != nil {
					t.Fatal(err)
				}
				segments = append(segments, seg.(*Segment))
			}
			s := &interim{results: docs}
			if minimum, maximum := s.calcTimestamp(); minimum != tc.min || maximum != tc.max {
				t.Fatalf("bounds=(%d,%d), want=(%d,%d)", minimum, maximum, tc.min, tc.max)
			}
			var buf bytes.Buffer
			_, footer, err := mergeToWriter(segments, make([]*roaring.Bitmap, len(segments)),
				defaultChunkMode, newCountHashWriter(&buf), nil)
			if err != nil {
				t.Fatal(err)
			}
			minimum, maximum := (&Segment{footer: footer}).Timestamp()
			if minimum != tc.min || maximum != tc.max {
				t.Fatalf("merged bounds=(%d,%d), want=(%d,%d)", minimum, maximum, tc.min, tc.max)
			}
		})
	}
}
