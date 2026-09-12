// Copyright (c) 2020 The Bluge Authors.
//
// Licensed under Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with License.
// You may obtain copy of License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under License is distributed on "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See License for specific language governing permissions and
// limitations under License.

package ice

import (
	"path/filepath"
	"testing"
)

func checkCollectionStats(t *testing.T, seg *Segment) {
	t.Helper()
	stats, err := seg.CollectionStats("desc")
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalDocumentCount() != 1 || stats.DocumentCount() != 1 || stats.SumTotalTermFrequency() != 2 {
		t.Fatalf("desc stats = (%d, %d, %d), want (1, 1, 2)",
			stats.TotalDocumentCount(), stats.DocumentCount(), stats.SumTotalTermFrequency())
	}
	// callers (e.g. bluge Snapshot.CollectionStats) Merge into the first
	// segment's result, so returned stats must not alias segment state
	stats.Merge(stats)
	again, err := seg.CollectionStats("desc")
	if err != nil {
		t.Fatal(err)
	}
	if again.TotalDocumentCount() != 1 || again.DocumentCount() != 1 || again.SumTotalTermFrequency() != 2 {
		t.Fatalf("Merge on returned stats mutated segment: got (%d, %d, %d), want (1, 1, 2)",
			again.TotalDocumentCount(), again.DocumentCount(), again.SumTotalTermFrequency())
	}
	missing, err := seg.CollectionStats("nope")
	if err != nil {
		t.Fatal(err)
	}
	if missing.TotalDocumentCount() != 0 || missing.DocumentCount() != 0 || missing.SumTotalTermFrequency() != 0 {
		t.Fatalf("unknown field stats should be zero, got (%d, %d, %d)",
			missing.TotalDocumentCount(), missing.DocumentCount(), missing.SumTotalTermFrequency())
	}
}

func TestCollectionStats(t *testing.T) {
	seg, err := buildTestSegment()
	if err != nil {
		t.Fatal(err)
	}
	checkCollectionStats(t, seg)

	path, cleanup := setupTestDir(t)
	defer cleanup()
	loaded, closeF, err := createDiskSegment(buildTestSegment, filepath.Join(path, "segment.ice"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cerr := closeF(); cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()
	checkCollectionStats(t, loaded)
}
