package client

import "github.com/Azure/azure-storage-azcopy/v10/common"

type BenchmarkOptions struct {
	SizePerFile int64

	FileCount       uint
	NumberOfFolders uint
	DeleteTestData  bool // Default true

	BlockSizeMB   float64
	PutBlobSizeMB float64
	BlobType      common.BlobType
	PutMd5        bool

	CheckLength bool                 // Default true
	Mode        common.BenchMarkMode // TODO (gapra-msft): This should be BenchmarkMode.
}

func (cc Client) Benchmark(resource string, options BenchmarkOptions) error {
	return nil
}
