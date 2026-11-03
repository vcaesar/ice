module github.com/vcaesar/ice

go 1.26.0

require (
	github.com/RoaringBitmap/roaring/v2 v2.30.0
	github.com/blevesearch/vellum v1.2.1
	github.com/klauspost/compress v1.20.3
	github.com/spf13/cobra v1.10.2
	github.com/vcaesar/bluge_segment_api v1.2.2
	github.com/zedsearch/mmap-go v1.3.0
)

require (
	github.com/bits-and-blooms/bitset v1.29.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	golang.org/x/sys v0.49.0 // indirect
)

replace github.com/blevesearch/vellum => github.com/zedsearch/vellum v1.2.2
