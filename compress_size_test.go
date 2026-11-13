// Copyright 2026 The AtomAI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ice

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	segment "github.com/vcaesar/bluge_segment_api"

	"github.com/vcaesar/ice/compress"
)

// TestCompressSize reports the segment size for one compress setting.
// It only runs when ICE_SIZE_CORPUS is set (os.PathListSeparator separated
// dirs of .go/.md files); the zstd encoder level is fixed per process, so
// run one setting per `go test` invocation:
//
//	ICE_SIZE_CORPUS=. ICE_SIZE_ALGO=zstd ICE_SIZE_LEVEL=9 go test -run TestCompressSize -v
func TestCompressSize(t *testing.T) {
	dirs := os.Getenv("ICE_SIZE_CORPUS")
	if dirs == "" {
		t.Skip("ICE_SIZE_CORPUS not set")
	}
	algo := os.Getenv("ICE_SIZE_ALGO")
	prevAlgo, prevZSTD, prevMinLZ := compress.Algorithm, compress.ZSTDCompressionLevel, compress.MinLZCompressionLevel
	t.Cleanup(func() {
		compress.Algorithm, compress.ZSTDCompressionLevel, compress.MinLZCompressionLevel = prevAlgo, prevZSTD, prevMinLZ
	})
	if err := compress.SetAlgorithm(algo); err != nil {
		t.Fatal(err)
	}
	if lv := os.Getenv("ICE_SIZE_LEVEL"); lv != "" {
		n, err := strconv.Atoi(lv)
		if err != nil {
			t.Fatal(err)
		}
		compress.ZSTDCompressionLevel, compress.MinLZCompressionLevel = n, n
	}

	docs, raw := sizeCorpus(t, filepath.SplitList(dirs))

	start := time.Now()
	seg, _, err := New(docs, encodeNorm)
	if err != nil {
		t.Fatal(err)
	}
	build := time.Since(start)
	var buf bytes.Buffer
	if _, err = seg.(*Segment).WriteTo(&buf, nil); err != nil {
		t.Fatal(err)
	}

	loaded, err := Load(segment.NewDataBytes(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	s := loaded.(*Segment)
	start = time.Now()
	var got int
	for i := uint64(0); i < s.Count(); i++ {
		err = s.VisitStoredFields(i, func(_ string, v []byte) bool {
			got += len(v)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	fetch := time.Since(start)
	if got != raw {
		t.Fatalf("stored bytes = %d, want %d", got, raw)
	}

	t.Logf("RESULT algo=%s level=%s docs=%d raw=%d seg=%d build=%v fetchAll=%v",
		algo, os.Getenv("ICE_SIZE_LEVEL"), len(docs), raw, buf.Len(), build, fetch)
}

// sizeCorpus turns every 30-line chunk of the .go/.md files under dirs into
// a document with _id, path (keyword, doc values) and body (text, stored).
func sizeCorpus(t *testing.T, dirs []string) (docs []segment.Document, raw int) {
	t.Helper()
	for _, dir := range dirs {
		root, err := os.OpenRoot(dir)
		if err != nil {
			t.Fatal(err)
		}
		fsys := root.FS()
		err = fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && (d.Name() == ".git" || d.Name() == "tmp") {
				return filepath.SkipDir
			}
			if ext := filepath.Ext(path); d.IsDir() || (ext != ".go" && ext != ".md") {
				return nil
			}
			data, err := fs.ReadFile(fsys, path)
			if err != nil {
				return err
			}
			path = filepath.Join(dir, path)
			lines := strings.Split(string(data), "\n")
			for i := 0; i < len(lines); i += 30 {
				body := strings.Join(lines[i:min(i+30, len(lines))], "\n")
				id := path + "#" + strconv.Itoa(i)
				docs = append(docs, sizeDoc(id, path, body))
				raw += len(id) + len(path) + len(body)
			}
			return nil
		})
		if cerr := root.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	return docs, raw
}

func sizeDoc(id, path, body string) segment.Document {
	words := strings.FieldsFunc(strings.ToLower(body), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	text := NewFakeField("body", strings.Join(words, " "), false, true, false)
	text.V, text.S = []byte(body), true
	doc := &FakeDocument{
		NewFakeField("_id", id, true, false, false),
		NewFakeField("path", path, true, false, true),
		text,
	}
	doc.FakeComposite("_all", []string{"_id"})
	return doc
}
