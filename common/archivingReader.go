// +build windows darwin
package common
import "C"


type ArchivingReader struct {
}

func NewArchivingReader(sourcePath string, sourceFile CloseableReaderAt, rawDataSize int64) (*ArchivingReader, error) {
	return &ArchivingReader{}, nil
}

func (c *ArchivingReader) Read(p []byte) (n int, err error) {
	return 0, nil
}

func (c *ArchivingReader) Close() error {
	return nil
}



