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
	"testing"

	segment "github.com/vcaesar/bluge_segment_api"
)

type gotPosting struct {
	num  uint64
	freq int
	norm int
	locs []string
}

func collectPostings(t *testing.T, seg *Segment, field, term string) []gotPosting {
	itr := expectTermInDictionary(t, expectFieldInSegment(t, seg, field), term)
	var rv []gotPosting
	p, err := itr.Next()
	for err == nil && p != nil {
		gp := gotPosting{num: p.Number(), freq: p.Frequency(), norm: decodeNorm("", float32(p.Norm()))}
		for _, l := range p.Locations() {
			gp.locs = append(gp.locs, l.Field())
		}
		rv = append(rv, gp)
		p, err = itr.Next()
	}
	if err != nil {
		t.Fatal(err)
	}
	return rv
}

// TestProcessDocumentDirectMatchesRollup builds the same logical content
// once with unique field names (direct path) and once with a repeated
// field name (rollup path) and checks both segments describe the same
// postings, including composite-field location field names.
func TestProcessDocumentDirectMatchesRollup(t *testing.T) {
	direct := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "wow", true, true, false),
		NewFakeField("desc", "some thing other", true, true, true),
	}
	direct.FakeComposite("_all", []string{"_id"})

	rollup := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "wow", true, true, false),
		NewFakeField("desc", "some thing", true, true, true),
		NewFakeField("desc", "other", true, true, true),
	}
	rollup.FakeComposite("_all", []string{"_id"})

	build := func(doc *FakeDocument) *Segment {
		seg, _, err := newWithChunkMode([]segment.Document{doc}, encodeNorm, defaultChunkMode)
		if err != nil {
			t.Fatal(err)
		}
		return seg.(*Segment)
	}
	dseg, rseg := build(direct), build(rollup)

	for _, tc := range []struct{ field, term string }{
		{"desc", "thing"}, {"desc", "other"}, {"name", "wow"}, {"_all", "thing"}, {"_all", "other"}, {"_all", "wow"},
	} {
		d, r := collectPostings(t, dseg, tc.field, tc.term), collectPostings(t, rseg, tc.field, tc.term)
		if len(d) != 1 || len(r) != 1 {
			t.Fatalf("%s:%s postings direct=%d rollup=%d, want 1 each", tc.field, tc.term, len(d), len(r))
		}
		if d[0].num != r[0].num || d[0].freq != r[0].freq || d[0].norm != r[0].norm || len(d[0].locs) != len(r[0].locs) {
			t.Fatalf("%s:%s direct %+v != rollup %+v", tc.field, tc.term, d[0], r[0])
		}
		for i := range d[0].locs {
			if d[0].locs[i] != r[0].locs[i] {
				t.Fatalf("%s:%s loc %d field direct %q rollup %q", tc.field, tc.term, i, d[0].locs[i], r[0].locs[i])
			}
		}
	}
	// spot-check absolute values on the direct segment
	other := collectPostings(t, dseg, "_all", "other")[0]
	if other.freq != 1 || other.norm != 4 || len(other.locs) != 1 || other.locs[0] != "desc" {
		t.Fatalf("_all:other = %+v, want freq 1 norm 4 locs [desc]", other)
	}
}
