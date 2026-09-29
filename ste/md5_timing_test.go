package ste

import (
	"bytes"
	"crypto/md5"
	"testing"
	"time"
)

const (
	md5TimingChunkSize = 8 * 1024 * 1024  // Matches AzCopy's default upload chunk size.
	md5TimingFileSize  = 10 * 1024 * 1024 // 10 MiB sample file.
)

// TestMD5HashingTiming mirrors how AzCopy accumulates an MD5 digest in
// scheduleSendChunks: a single hash.Hash is fed sequential chunks of the
// source file. It records the time to hash one chunk and the time to hash
// a 10 MiB file processed chunk-by-chunk.
func TestMD5HashingTiming(t *testing.T) {
	chunk := makeDeterministicMD5Data(md5TimingChunkSize)

	oneChunkHasher := md5.New()
	oneChunkStart := time.Now()

	if _, err := oneChunkHasher.Write(chunk); err != nil {
		t.Fatalf("failed to hash one chunk: %v", err)
	}

	oneChunkDuration := time.Since(oneChunkStart)
	oneChunkDigest := oneChunkHasher.Sum(nil)

	expectedChunkDigest := md5.Sum(chunk)
	if !bytes.Equal(oneChunkDigest, expectedChunkDigest[:]) {
		t.Fatal("MD5 digest for one chunk does not match the expected digest")
	}

	t.Logf(
		"MD5 timing: one chunk: size_bytes=%d duration=%s",
		len(chunk),
		oneChunkDuration,
	)

	fileData := makeDeterministicMD5Data(md5TimingFileSize)
	fileHasher := md5.New()
	fileHashingStart := time.Now()

	for offset := 0; offset < len(fileData); offset += md5TimingChunkSize {
		end := offset + md5TimingChunkSize
		if end > len(fileData) {
			end = len(fileData)
		}

		if _, err := fileHasher.Write(fileData[offset:end]); err != nil {
			t.Fatalf("failed to hash file chunk at offset %d: %v", offset, err)
		}
	}

	fileHashingDuration := time.Since(fileHashingStart)
	fileDigest := fileHasher.Sum(nil)

	expectedFileDigest := md5.Sum(fileData)
	if !bytes.Equal(fileDigest, expectedFileDigest[:]) {
		t.Fatal("MD5 digest for the 10 MiB file does not match the expected digest")
	}

	t.Logf(
		"MD5 timing: 10 MiB file: size_bytes=%d chunk_size_bytes=%d duration=%s",
		len(fileData),
		md5TimingChunkSize,
		fileHashingDuration,
	)
}

func makeDeterministicMD5Data(size int) []byte {
	data := make([]byte, size)
	for index := range data {
		data[index] = byte((index*31 + index/17) % 251)
	}

	return data
}

func BenchmarkMD5HashOneChunk(b *testing.B) {
	chunk := makeDeterministicMD5Data(md5TimingChunkSize)
	b.SetBytes(int64(len(chunk)))
	b.ResetTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		hasher := md5.New()

		if _, err := hasher.Write(chunk); err != nil {
			b.Fatalf("failed to hash chunk: %v", err)
		}

		_ = hasher.Sum(nil)
	}
}
