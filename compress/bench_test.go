package compress

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"testing"
)

func TestSetAlgorithm(t *testing.T) {
	previous := Algorithm
	t.Cleanup(func() { Algorithm = previous })
	for name, want := range map[string]int{
		"snappy": SNAPPY, "s2": S2, "zstd": ZSTD, "minlz": MINLZ, "lz4": LZ4, " MinLZ ": MINLZ, "LZ4": LZ4,
	} {
		if err := SetAlgorithm(name); err != nil {
			t.Fatalf("%q: %v", name, err)
		}
		if Algorithm != want {
			t.Fatalf("%q: Algorithm = %d, want %d", name, Algorithm, want)
		}
	}
	Algorithm = S2
	if err := SetAlgorithm("brotli"); err == nil {
		t.Fatal("expected unknown algorithm to fail")
	}
	if Algorithm != S2 {
		t.Fatalf("failed SetAlgorithm changed Algorithm to %d", Algorithm)
	}
}

// Corrupt lz4 blocks must fail or decode, never panic or allocate more than
// lz4MaxRatio times their size.
func TestLZ4DecompressCorrupt(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6)) // #nosec G404 -- deterministic test data.
	for name, src := range repeatInputs() {
		block, err := LZ4Compress(nil, src)
		if err != nil {
			t.Fatal(err)
		}
		for range 2000 {
			bad := bytes.Clone(block)
			switch rng.IntN(3) {
			case 0:
				bad[rng.IntN(len(bad))] ^= byte(1 + rng.IntN(255)) // #nosec G115 -- 1 to 255.
			case 1:
				bad = bad[:rng.IntN(len(bad)+1)]
			default:
				bad = append(bad, byte(rng.Uint32())) // #nosec G115 -- any byte.
			}
			if got, err := LZ4Decompress(nil, bad); err == nil && len(got) > lz4MaxRatio*len(bad) {
				t.Fatalf("%s: decoded %d bytes from %d", name, len(got), len(bad))
			}
		}
	}
	huge := binary.AppendUvarint(nil, 1<<40)
	if _, err := LZ4Decompress(nil, append(huge, 0)); err == nil {
		t.Fatal("expected an oversized length to fail")
	}
}

// benchInputs are the chunk shapes ice compresses: stored documents
// (JSON), analyzed text, postings varints and the s2 edge cases.
func benchInputs() map[string][]byte {
	rng := rand.New(rand.NewPCG(7, 8)) // #nosec G404 -- deterministic test data.
	words := []string{"search", "index", "segment", "posting", "term", "query", "field",
		"vector", "score", "merge", "shard", "golang", "compress", "token", "analyzer", "bluge"}
	var docs, text, postings []byte
	for i := 0; len(docs) < 64<<10; i++ {
		docs = fmt.Appendf(docs, `{"_id":"doc-%08d","@timestamp":"2026-11-%02dT%02d:%02d:%02dZ","level":"%s","status":%d,"latency_ms":%d,"message":"%s %s %s"}`,
			i, 1+rng.IntN(28), rng.IntN(24), rng.IntN(60), rng.IntN(60),
			[]string{"info", "warn", "error"}[rng.IntN(3)], []int{200, 201, 404, 500}[rng.IntN(4)], rng.IntN(5000),
			words[rng.IntN(len(words))], words[rng.IntN(len(words))], words[rng.IntN(len(words))])
	}
	for len(text) < 64<<10 {
		text = append(text, words[rng.IntN(len(words))]...)
		text = append(text, ' ')
	}
	for range 16 << 10 {
		postings = binary.AppendUvarint(postings, uint64(1+rng.ExpFloat64()*20)) // doc deltas
		postings = binary.AppendUvarint(postings, uint64(1+rng.IntN(4)))        // freq
	}
	inputs := repeatInputs()
	return map[string][]byte{
		"docs": docs, "text": text, "postings": postings,
		"locs": inputs["locs"], "mixed": inputs["mixed"], "random": inputs["random"],
	}
}

var benchAlgorithms = []struct {
	name       string
	compress   func(dst, src []byte) ([]byte, error)
	decompress func(dst, src []byte) ([]byte, error)
}{
	{"snappy", SnappyCompress, SnappyDecompress},
	{"s2", S2Compress, S2Decompress},
	// the zstd encoder is built once, so only ZSTDCompressionLevel applies
	{"zstd", func(d, s []byte) ([]byte, error) { return ZSTDCompress(d, s, ZSTDCompressionLevel) }, ZSTDDecompress},
	{"minlz1", func(d, s []byte) ([]byte, error) { return MinLZCompress(d, s, 1) }, MinLZDecompress},
	{"minlz2", func(d, s []byte) ([]byte, error) { return MinLZCompress(d, s, 2) }, MinLZDecompress},
	{"lz4", LZ4Compress, LZ4Decompress},
}

func TestBenchAlgorithmsRoundTrip(t *testing.T) {
	for name, src := range benchInputs() {
		for _, alg := range benchAlgorithms {
			encoded, err := alg.compress(nil, src)
			if err != nil {
				t.Fatalf("%s/%s: %v", name, alg.name, err)
			}
			decoded, err := alg.decompress(nil, encoded)
			if err != nil || !bytes.Equal(decoded, src) {
				t.Fatalf("%s/%s: round trip: %v", name, alg.name, err)
			}
		}
	}
}

// BenchmarkCompress reports, per algorithm and input, compress and
// decompress CPU (ns/op, MB/s), memory (B/op, allocs/op) and the saved
// space as a percentage of the input (saved%).
func BenchmarkCompress(b *testing.B) {
	inputs := benchInputs()
	for _, input := range []string{"docs", "text", "postings", "locs", "mixed", "random"} {
		src := inputs[input]
		for _, alg := range benchAlgorithms {
			encoded, err := alg.compress(nil, src)
			if err != nil {
				b.Fatal(err)
			}
			saved := 100 * (1 - float64(len(encoded))/float64(len(src)))
			b.Run(input+"/"+alg.name+"/compress", func(b *testing.B) {
				dst := make([]byte, 0, 2*len(src)+1024)
				b.SetBytes(int64(len(src)))
				b.ReportAllocs()
				for b.Loop() {
					if _, err := alg.compress(dst, src); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(saved, "saved%")
			})
			b.Run(input+"/"+alg.name+"/decompress", func(b *testing.B) {
				dst := make([]byte, 0, len(src))
				b.SetBytes(int64(len(src)))
				b.ReportAllocs()
				for b.Loop() {
					if _, err := alg.decompress(dst, encoded); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(saved, "saved%")
			})
		}
	}
}
