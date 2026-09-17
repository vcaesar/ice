package ice

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	segment "github.com/vcaesar/bluge_segment_api"

	"github.com/vcaesar/ice/compress"
)

func optimizationFileData(t *testing.T, contents []byte) *segment.Data {
	t.Helper()
	path := filepath.Join(t.TempDir(), "segment")
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := f.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	data, err := segment.NewDataFile(f)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestOptimizationDictionaryReadErrorsUnlock(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"length read", []byte{0}},
		{"FST read", append([]byte{0, 100}, make([]byte, 9)...)},
		{"FST decode", append([]byte{0, 1}, make([]byte, 9)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Segment{data: optimizationFileData(t, tc.data), fieldsMap: map[string]uint16{"field": 1}, dictLocs: []uint64{1}}
			if _, err := s.dictionary("field"); err == nil {
				t.Fatal("expected dictionary error")
			}
			done := make(chan error, 1)
			go func() {
				_, err := s.dictionary("field")
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("expected repeated dictionary error")
				}
			case <-time.After(time.Second):
				t.Fatal("dictionary mutex remained locked")
			}
		})
	}
}

func optimizationVarints(values ...uint64) []byte {
	var result []byte
	var buf [binary.MaxVarintLen64]byte
	for _, value := range values {
		n := binary.PutUvarint(buf[:], value)
		result = append(result, buf[:n]...)
	}
	return result
}

func TestOptimizationVisitDocumentValidationAndCleanup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		meta      []byte
		num       uint64
		stop      bool
		wantErr   bool
		wantCalls int
	}{
		{"valid", optimizationVarints(0, 0, 3), 0, false, false, 1},
		{"empty at end", optimizationVarints(0, 3, 0), 0, false, false, 1},
		{"early stop", optimizationVarints(0, 0, 3, 99, 0, 0), 0, true, false, 1},
		{"invalid document", nil, 1, false, false, 0},
		{"invalid field", optimizationVarints(1, 0, 1), 0, false, true, 0},
		{"huge field", optimizationVarints(^uint64(0), 0, 1), 0, false, true, 0},
		{"offset past end", optimizationVarints(0, 4, 0), 0, false, true, 0},
		{"length past end", optimizationVarints(0, 2, 2), 0, false, true, 0},
		{"range overflow", optimizationVarints(0, 1, ^uint64(0)), 0, false, true, 0},
		{"offset overflow", optimizationVarints(0, ^uint64(0), 2), 0, false, true, 0},
		{"truncated field", []byte{0x80}, 0, false, true, 0},
		{"missing offset", []byte{0}, 0, false, true, 0},
		{"missing length", []byte{0, 0}, 0, false, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := optimizationVarints(uint64(len(tc.meta)), 3)
			payload = append(payload, tc.meta...)
			payload = append(payload, "abc"...)
			s := &Segment{data: segment.NewDataBytes(make([]byte, 8)), footer: &footer{numDocs: 1}, fieldsInv: []string{"field"}}
			s.initStoredChunkCache(StoredChunkCacheSize)
			s.storedChunks.put(0, payload)
			ctx := &visitDocumentCtx{}
			ctx.reader.Reset([]byte("previous data"))
			calls := 0
			err := s.visitDocument(ctx, tc.num, func(field string, value []byte) bool {
				calls++
				if field != "field" || (len(value) != 0 && string(value) != "abc") {
					t.Errorf("unexpected field/value %q/%q", field, value)
				}
				return !tc.stop
			})
			if (err != nil) != tc.wantErr || calls != tc.wantCalls {
				t.Fatalf("error=%v, calls=%d; want error=%v, calls=%d", err, calls, tc.wantErr, tc.wantCalls)
			}
			if !reflect.DeepEqual(ctx.reader, *bytes.NewReader(nil)) {
				t.Fatal("reader retained document metadata")
			}
		})
	}
}

func TestOptimizationStoredCacheSize(t *testing.T) {
	payload := append(optimizationVarints(0, 1024), bytes.Repeat([]byte("a"), 1024)...)
	compressed, err := compress.Compress(nil, payload)
	if err != nil {
		t.Fatal(err)
	}
	s := &Segment{
		data:                    segment.NewDataBytes(append(make([]byte, 8), compressed...)),
		footer:                  &footer{numDocs: 1},
		storedFieldChunkOffsets: []uint64{8, uint64(8 + len(compressed))},
	}
	s.initStoredChunkCache(StoredChunkCacheSize)
	s.updateSize()
	before := s.Size()
	wantBase := reflectStaticSizeSegment + s.data.Size() + 2*sizeOfUint64
	if before != wantBase {
		t.Fatalf("base size=%d, want %d", before, wantBase)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if s.Size() < before {
					t.Error("size fell below base")
				}
				if _, _, readErr := s.getDocStoredMetaAndUnCompressed(0); readErr != nil {
					t.Error(readErr)
					return
				}
			}
		}()
	}
	wg.Wait()
	chunk, ok := s.storedChunks.get(0)
	if !ok {
		t.Fatal("chunk 0 not cached")
	}
	want := before + cap(chunk)
	if got := s.Size(); got != want || got <= before {
		t.Fatalf("loaded size=%d, want %d > %d", got, want, before)
	}
	s.updateSize()
	if got := s.Size(); got != want {
		t.Fatalf("updateSize double counted data: got %d, want %d", got, want)
	}
	s.storedChunks.put(1, make([]byte, 1, 64))
	if got := s.Size(); got != want+64 {
		t.Fatalf("size did not include second chunk capacity: got %d, want %d", got, want+64)
	}
}

func TestOptimizationStoredReadFailureSizeAndCleanup(t *testing.T) {
	s := &Segment{
		data:                    optimizationFileData(t, make([]byte, 8)),
		footer:                  &footer{numDocs: 1},
		storedFieldChunkOffsets: []uint64{8, 16},
	}
	s.initStoredChunkCache(StoredChunkCacheSize)
	s.updateSize()
	before := s.Size()
	ctx := &visitDocumentCtx{}
	ctx.reader.Reset([]byte("previous metadata"))
	if err := s.visitDocument(ctx, 0, func(string, []byte) bool { return true }); err == nil {
		t.Fatal("expected stored chunk read error")
	}
	if ctx.reader.Size() != 0 {
		t.Fatal("reader retained metadata after read failure")
	}
	done := make(chan int, 1)
	go func() { done <- s.Size() }()
	select {
	case got := <-done:
		if got != before || s.storedChunks.len() != 0 {
			t.Fatal("failed read changed cache accounting")
		}
	case <-time.After(time.Second):
		t.Fatal("stored cache mutex remained locked")
	}
}
