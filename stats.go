//  Copyright (c) 2020 The Bluge Authors.
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
	segment "github.com/vcaesar/bluge_segment_api"
)

type CollectionStats struct {
	totalDocCount    uint64
	docCount         uint64
	sumTotalTermFreq uint64
}

func (c *CollectionStats) TotalDocumentCount() uint64 {
	return c.totalDocCount
}

func (c *CollectionStats) DocumentCount() uint64 {
	return c.docCount
}

func (c *CollectionStats) SumTotalTermFrequency() uint64 {
	return c.sumTotalTermFreq
}

func (c *CollectionStats) Merge(other segment.CollectionStats) {
	c.totalDocCount += other.TotalDocumentCount()
	c.docCount += other.DocumentCount()
	c.sumTotalTermFreq += other.SumTotalTermFrequency()
}

func (s *Segment) CollectionStats(field string) (segment.CollectionStats, error) {
	// copy: segment.CollectionStats exposes Merge, and callers (bluge
	// Snapshot.CollectionStats) Merge into the first segment's result.
	rv := &CollectionStats{}
	if fieldIDPlus1 := s.fieldsMap[field]; fieldIDPlus1 > 0 {
		*rv = s.fieldStats[fieldIDPlus1-1]
	}
	return rv, nil
}

// initFieldStats precomputes per-field stats once at segment open so
// CollectionStats avoids map lookups per query.
func (s *Segment) initFieldStats() {
	s.fieldStats = make([]CollectionStats, len(s.fieldsInv))
	for id := range s.fieldsInv {
		// #nosec G115 -- field ids are assigned from a uint16 counter.
		s.fieldStats[id] = CollectionStats{
			totalDocCount:    s.footer.numDocs,
			docCount:         s.fieldDocs[uint16(id)],
			sumTotalTermFreq: s.fieldFreqs[uint16(id)],
		}
	}
}
