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
	"container/list"
	"sync"
)

// StoredChunkCacheSize is the default number of decompressed stored-field
// chunks each segment keeps in its LRU cache; New and Load read it when a
// segment is opened. A chunk holds defaultDocumentChunkSize documents. Zero
// disables caching. Use Options to set it per index instead of globally.
var StoredChunkCacheSize = 100

// Options tunes segments opened by its New and Load methods, which match the
// signatures of the package-level New and Load.
type Options struct {
	// StoredChunkCacheSize bounds decompressed stored-field chunks kept per
	// segment; zero disables caching.
	StoredChunkCacheSize int
}

// DefaultOptions returns Options mirroring the package-level defaults.
func DefaultOptions() Options {
	return Options{StoredChunkCacheSize: StoredChunkCacheSize}
}

// storedChunkCache is a bounded LRU of decompressed stored-field chunks
// keyed by chunk index. Front of order is the most recently used entry.
// Concurrent misses on one chunk share a single load via inflight.
type storedChunkCache struct {
	m        sync.Mutex
	capacity int
	order    list.List
	items    map[uint64]*list.Element
	inflight map[uint64]*storedChunkLoad
	bytes    int
	hits     uint64
	misses   uint64
}

type storedChunkEntry struct {
	chunk uint64
	data  []byte
}

type storedChunkLoad struct {
	done chan struct{}
	data []byte
	err  error
}

func (c *storedChunkCache) init(capacity int) {
	c.m.Lock()
	c.capacity = capacity
	c.order.Init()
	c.items = make(map[uint64]*list.Element, capacity)
	c.inflight = make(map[uint64]*storedChunkLoad)
	c.bytes = 0
	c.hits, c.misses = 0, 0
	c.m.Unlock()
}

// getOrLoad returns the cached chunk or runs load once for all concurrent
// callers of the same chunk, caching its result.
func (c *storedChunkCache) getOrLoad(chunk uint64, load func() ([]byte, error)) ([]byte, error) {
	c.m.Lock()
	if el, ok := c.items[chunk]; ok {
		c.hits++
		c.order.MoveToFront(el)
		data := el.Value.(*storedChunkEntry).data
		c.m.Unlock()
		return data, nil
	}
	c.misses++
	if l, ok := c.inflight[chunk]; ok {
		c.m.Unlock()
		<-l.done
		return l.data, l.err
	}
	l := &storedChunkLoad{done: make(chan struct{})}
	if c.inflight == nil { // zero-value cache: caching disabled, still coalesce
		c.inflight = make(map[uint64]*storedChunkLoad)
	}
	c.inflight[chunk] = l
	c.m.Unlock()

	l.data, l.err = load()

	// Publish the result, drop the inflight entry and signal waiters while
	// holding the lock, so a concurrent caller either joins this load or
	// observes the cached value instead of starting a duplicate load.
	c.m.Lock()
	if l.err == nil {
		c.putLocked(chunk, l.data)
	}
	delete(c.inflight, chunk)
	close(l.done)
	c.m.Unlock()
	return l.data, l.err
}

func (c *storedChunkCache) get(chunk uint64) ([]byte, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	el, ok := c.items[chunk]
	if !ok {
		c.misses++
		return nil, false
	}
	c.hits++
	c.order.MoveToFront(el)
	return el.Value.(*storedChunkEntry).data, true
}

func (c *storedChunkCache) put(chunk uint64, data []byte) {
	c.m.Lock()
	c.putLocked(chunk, data)
	c.m.Unlock()
}

// putLocked inserts data for chunk; the caller must hold c.m.
func (c *storedChunkCache) putLocked(chunk uint64, data []byte) {
	if c.capacity <= 0 {
		return
	}
	if el, ok := c.items[chunk]; ok {
		entry := el.Value.(*storedChunkEntry)
		c.bytes += cap(data) - cap(entry.data)
		entry.data = data
		c.order.MoveToFront(el)
		return
	}
	for c.order.Len() >= c.capacity {
		last := c.order.Back()
		evicted := last.Value.(*storedChunkEntry)
		c.bytes -= cap(evicted.data)
		delete(c.items, evicted.chunk)
		c.order.Remove(last)
	}
	c.bytes += cap(data)
	c.items[chunk] = c.order.PushFront(&storedChunkEntry{chunk: chunk, data: data})
}

// size returns the total capacity of cached chunk data in bytes.
func (c *storedChunkCache) size() int {
	c.m.Lock()
	defer c.m.Unlock()
	return c.bytes
}

func (c *storedChunkCache) len() int {
	c.m.Lock()
	defer c.m.Unlock()
	return c.order.Len()
}

// stats returns cache hits, misses and the number of resident entries.
func (c *storedChunkCache) stats() (hits, misses uint64, entries int) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.hits, c.misses, c.order.Len()
}
