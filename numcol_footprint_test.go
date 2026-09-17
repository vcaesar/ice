//  Copyright (c) 2026 The Bluge Authors.
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
	"fmt"
	"math/rand"
	"testing"
	"time"

	segment "github.com/vcaesar/bluge_segment_api"
)

// Compares on-disk bytes, resident size and build time of a numeric field
// stored as a typed column (v4) versus term doc values (v3 layout).
func TestNumericColumnFootprint(t *testing.T) {
	const n = 100_000
	rnd := rand.New(rand.NewSource(1)) // #nosec G404 -- deterministic test data.
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(rnd.Intn(10000))
	}
	build := func(typed bool) (*Segment, int, time.Duration) {
		docs := make([]segment.Document, n)
		for i := range docs {
			id := NewFakeField("_id", fmt.Sprint(i), true, false, false)
			var price segment.Field
			if typed {
				price = NewFakeNumericField("price", vals[i])
			} else {
				price = fakeTermField("price", prefixCodedInt64(vals[i]))
			}
			docs[i] = &numericDoc{fields: []segment.Field{id, price}}
		}
		start := time.Now()
		seg, _, err := newWithChunkMode(docs, encodeNorm, defaultChunkMode, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		var buf bytes.Buffer
		if _, err = seg.WriteTo(&buf, nil); err != nil {
			t.Fatal(err)
		}
		return seg.(*Segment), buf.Len(), elapsed
	}
	col, colBytes, colTime := build(true)
	term, termBytes, termTime := build(false)

	colDv := col.fieldNumCols[col.fieldsMap["price"]-1]
	termDv := term.fieldDvReaders[term.fieldsMap["price"]-1]
	if colDv == nil || termDv == nil {
		t.Fatal("expected column and term doc values")
	}
	colRegion := colDv.dataEnd + uint64(len(colDv.blocks))*8
	var termRegion uint64
	if len(termDv.chunkOffsets) > 0 {
		termRegion = termDv.chunkOffsets[len(termDv.chunkOffsets)-1]
	}
	t.Logf("docs=%d values 0..9999", n)
	t.Logf("segment bytes:   column=%d  terms=%d  (%.1f%%)", colBytes, termBytes, 100*float64(colBytes)/float64(termBytes))
	t.Logf("price dv region: column=%d  terms=%d", colRegion, termRegion)
	t.Logf("resident meta:   column=%d  terms=%d (Segment.Size column=%d terms=%d)",
		colDv.size(), termDv.size(), col.Size(), term.Size())
	t.Logf("build time:      column=%v  terms=%v", colTime, termTime)
	if colBytes > termBytes {
		t.Fatalf("column segment larger than term segment: %d > %d", colBytes, termBytes)
	}
}
