//  Copyright (c) 2020 Couchbase, Inc.
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
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	segment "github.com/vcaesar/bluge_segment_api"

	"github.com/vcaesar/ice/compress"
)

func TestStoredChunkCacheLRU(t *testing.T) {
	var c storedChunkCache
	c.init(2)
	c.put(0, make([]byte, 0, 10))
	c.put(1, make([]byte, 0, 20))
	if _, ok := c.get(0); !ok {
		t.Fatal("chunk 0 should be cached")
	}
	// 1 is now least recently used; inserting 2 evicts it.
	c.put(2, make([]byte, 0, 40))
	if _, ok := c.get(1); ok {
		t.Fatal("chunk 1 should have been evicted")
	}
	if _, ok := c.get(0); !ok {
		t.Fatal("chunk 0 should survive eviction")
	}
	if got := c.len(); got != 2 {
		t.Fatalf("len=%d, want 2", got)
	}
	if got := c.size(); got != 50 {
		t.Fatalf("size=%d, want 50", got)
	}
	// Overwriting an existing key keeps len and refreshes data.
	c.put(2, make([]byte, 0, 8))
	if got := c.size(); got != 18 {
		t.Fatalf("size after overwrite=%d, want 18", got)
	}
	hits, misses, entries := c.stats()
	if hits != 2 || misses != 1 || entries != 2 {
		t.Fatalf("stats=(%d,%d,%d), want (2,1,2)", hits, misses, entries)
	}
}

func TestStoredChunkCacheDisabled(t *testing.T) {
	var c storedChunkCache
	c.init(0)
	c.put(0, []byte("x"))
	if _, ok := c.get(0); ok || c.len() != 0 {
		t.Fatal("disabled cache must not retain entries")
	}
}

// storedChunkSegment builds a segment with n single-doc chunks, each holding
// one stored field with payload of size payloadLen, caching cacheSize chunks.
func storedChunkSegment(t *testing.T, n, payloadLen, cacheSize int) *Segment {
	t.Helper()
	// #nosec G115 -- test fixture sizes are small positive constants.
	body := append(optimizationVarints(0, uint64(payloadLen)), bytes.Repeat([]byte("a"), payloadLen)...)
	compressed, err := compress.Compress(nil, body)
	if err != nil {
		t.Fatal(err)
	}
	// stored index: one offset per doc, all zero (doc is first in its chunk)
	numDocs := n * int(defaultDocumentChunkSize)
	data := make([]byte, fileAddrWidth*numDocs)
	offsets := make([]uint64, 0, n+1)
	for range n {
		offsets = append(offsets, uint64(len(data)))
		data = append(data, compressed...)
	}
	offsets = append(offsets, uint64(len(data)))
	s := &Segment{
		data: segment.NewDataBytes(data),
		// #nosec G115 -- numDocs derives from a small positive chunk count.
		footer:                  &footer{numDocs: uint64(numDocs)},
		storedFieldChunkOffsets: offsets,
	}
	s.initStoredChunkCache(cacheSize)
	s.updateSize()
	return s
}

func TestSegmentStoredChunkCacheBounded(t *testing.T) {
	const chunks = 5
	s := storedChunkSegment(t, chunks, 256, 2)
	base := s.Size()
	for doc := range uint64(chunks) {
		if _, _, err := s.getDocStoredMetaAndUnCompressed(doc * uint64(defaultDocumentChunkSize)); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.storedChunks.len(); got != 2 {
		t.Fatalf("resident chunks=%d, want 2", got)
	}
	if got := s.Size(); got <= base || got-base > 2*1024 {
		t.Fatalf("size grew by %d, expected bounded by 2 chunks", got-base)
	}
	hits, misses, _ := s.StoredChunkCacheStats()
	if hits != 0 || misses != chunks {
		t.Fatalf("stats=(%d,%d), want (0,%d)", hits, misses, chunks)
	}
	// Re-reading the most recent chunk is a hit.
	if _, _, err := s.getDocStoredMetaAndUnCompressed((chunks - 1) * uint64(defaultDocumentChunkSize)); err != nil {
		t.Fatal(err)
	}
	if hits, _, _ = s.StoredChunkCacheStats(); hits != 1 {
		t.Fatalf("hits=%d, want 1", hits)
	}
}

func TestSegmentStoredChunkCacheConcurrent(t *testing.T) {
	const chunks = 8
	s := storedChunkSegment(t, chunks, 64, 3)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				// #nosec G115 -- modulo result is in [0, chunks).
				doc := uint64((g+i)%chunks) * uint64(defaultDocumentChunkSize)
				meta, data, err := s.getDocStoredMetaAndUnCompressed(doc)
				if err != nil {
					t.Error(err)
					return
				}
				if len(meta) != 0 || len(data) != 64 {
					t.Errorf("bad payload meta=%x len=%d", meta, len(data))
					return
				}
				_ = s.Size()
			}
		}()
	}
	wg.Wait()
	if got := s.storedChunks.len(); got > 3 {
		t.Fatalf("resident chunks=%d, want <= 3", got)
	}
}

func TestStoredChunkCacheCoalescesMisses(t *testing.T) {
	var c storedChunkCache
	c.init(4)
	var loads atomic.Int32
	release := make(chan struct{})
	load := func() ([]byte, error) { //nolint:unparam // matches getOrLoad's loader signature
		loads.Add(1)
		<-release
		return []byte("v"), nil
	}
	const callers = 8
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := c.getOrLoad(7, load)
			if err != nil || string(data) != "v" {
				t.Errorf("getOrLoad=(%q,%v)", data, err)
			}
		}()
	}
	// Wait until one loader is inside load and the rest queue on it.
	for loads.Load() == 0 {
		runtime.Gosched()
	}
	c.m.Lock()
	for len(c.inflight) != 1 {
		c.m.Unlock()
		runtime.Gosched()
		c.m.Lock()
	}
	c.m.Unlock()
	close(release)
	wg.Wait()
	if got := loads.Load(); got != 1 {
		t.Fatalf("load ran %d times, want 1", got)
	}
	hits, misses, entries := c.stats()
	if hits+misses != callers || misses == 0 || entries != 1 {
		t.Fatalf("stats=(%d,%d,%d), want hits+misses=%d, misses>0, entries=1", hits, misses, entries, callers)
	}
	if len(c.inflight) != 0 {
		t.Fatal("inflight entry leaked")
	}
}

func TestStoredChunkCacheLoadErrorNotCached(t *testing.T) {
	var c storedChunkCache
	c.init(4)
	want := errors.New("boom")
	if _, err := c.getOrLoad(1, func() ([]byte, error) { return nil, want }); !errors.Is(err, want) {
		t.Fatalf("err=%v, want %v", err, want)
	}
	if c.len() != 0 || len(c.inflight) != 0 {
		t.Fatal("failed load must leave no cache or inflight entry")
	}
	data, err := c.getOrLoad(1, func() ([]byte, error) { return []byte("ok"), nil })
	if err != nil || string(data) != "ok" {
		t.Fatalf("retry=(%q,%v)", data, err)
	}
}

// A failing load caches nothing, so waiters released by it must still share
// that single attempt instead of racing into duplicate loads.
func TestStoredChunkCacheCoalescesFailedLoad(t *testing.T) {
	var c storedChunkCache
	c.init(4)
	want := errors.New("boom")
	var loads atomic.Int32
	release := make(chan struct{})
	load := func() ([]byte, error) { //nolint:unparam // matches getOrLoad's loader signature
		loads.Add(1)
		<-release
		return nil, want
	}
	const callers = 8
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.getOrLoad(3, load); !errors.Is(err, want) {
				t.Errorf("err=%v, want %v", err, want)
			}
		}()
	}
	// Release only once every caller has registered a miss, so all of them
	// are waiting on the single inflight load.
	c.m.Lock()
	for c.misses != callers || len(c.inflight) != 1 {
		c.m.Unlock()
		runtime.Gosched()
		c.m.Lock()
	}
	c.m.Unlock()
	close(release)
	wg.Wait()
	if got := loads.Load(); got != 1 {
		t.Fatalf("load ran %d times, want 1", got)
	}
	if c.len() != 0 || len(c.inflight) != 0 {
		t.Fatal("failed load must leave no cache or inflight entry")
	}
}

func TestStoredChunkCacheSizeAccounting(t *testing.T) {
	var c storedChunkCache
	c.init(2)
	c.put(0, make([]byte, 0, 16))
	c.put(1, make([]byte, 0, 32))
	if got := c.size(); got != 48 {
		t.Fatalf("size=%d, want 48", got)
	}
	c.put(0, make([]byte, 0, 4)) // overwrite shrinks
	if got := c.size(); got != 36 {
		t.Fatalf("size after overwrite=%d, want 36", got)
	}
	c.put(2, make([]byte, 0, 8)) // evicts chunk 1 (32)
	if got := c.size(); got != 12 {
		t.Fatalf("size after eviction=%d, want 12", got)
	}
	if _, err := c.getOrLoad(3, func() ([]byte, error) { return make([]byte, 0, 64), nil }); err != nil {
		t.Fatal(err)
	}
	if got := c.size(); got != 72 { // chunk 0 (4) evicted, 8 + 64 resident
		t.Fatalf("size after load=%d, want 72", got)
	}
	c.init(2)
	if got := c.size(); got != 0 {
		t.Fatalf("size after re-init=%d, want 0", got)
	}
}

func TestOptionsStoredChunkCacheSize(t *testing.T) {
	doc := &FakeDocument{NewFakeField(_idFieldName, "a", true, false, false)}
	for _, tc := range []struct {
		name string
		size int
	}{{"disabled", 0}, {"one", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{StoredChunkCacheSize: tc.size}
			seg, _, err := opts.New([]segment.Document{doc}, encodeNorm)
			if err != nil {
				t.Fatal(err)
			}
			s := seg.(*Segment)
			if s.storedChunks.capacity != tc.size {
				t.Fatalf("New capacity=%d, want %d", s.storedChunks.capacity, tc.size)
			}
			var buf bytes.Buffer
			if _, err = s.WriteTo(&buf, nil); err != nil {
				t.Fatal(err)
			}
			loaded, err := opts.Load(segment.NewDataBytes(buf.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			l := loaded.(*Segment)
			if l.storedChunks.capacity != tc.size {
				t.Fatalf("Load capacity=%d, want %d", l.storedChunks.capacity, tc.size)
			}
			if _, _, err = l.getDocStoredMetaAndUnCompressed(0); err != nil {
				t.Fatal(err)
			}
			if got := l.storedChunks.len(); (tc.size == 0 && got != 0) || (tc.size > 0 && got != 1) {
				t.Fatalf("resident=%d for size %d", got, tc.size)
			}
		})
	}
}
