package compress

import (
	"bytes"
	"testing"
)

func TestCompressionRoundTrip(t *testing.T) {
	previous := Algorithm
	t.Cleanup(func() { Algorithm = previous })
	for _, tc := range []struct {
		name      string
		algorithm int
	}{
		{"snappy", SNAPPY},
		{"s2", S2},
		{"zstd", ZSTD},
	} {
		t.Run(tc.name, func(t *testing.T) {
			Algorithm = tc.algorithm
			for _, src := range [][]byte{nil, []byte("ice"), bytes.Repeat([]byte("stored field data"), 10000)} {
				for _, reuse := range []bool{false, true} {
					var encoded, decoded []byte
					if reuse {
						encoded = bytes.Repeat([]byte{0xff}, len(src)+1024)
						decoded = bytes.Repeat([]byte{0xff}, len(src)+1024)
					}
					encoded, err := Compress(encoded, src)
					if err != nil {
						t.Fatal(err)
					}
					decoded, err = Decompress(decoded, encoded)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(decoded, src) {
						t.Fatalf("round trip failed: length=%d reuse=%v", len(src), reuse)
					}
				}
			}
			if _, err := Decompress(nil, []byte{0xff}); err == nil {
				t.Fatal("expected corrupt data to fail")
			}
		})
	}
}
