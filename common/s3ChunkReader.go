// Author : Parmananda Banakar
package common

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"time"
)

// S3ChunkReader streams or buffers a chunk of data sourced from S3.
type S3ChunkReader struct {
	ctx             context.Context
	slicePool       ByteSlicePooler
	cacheLimiter    CacheLimiter
	chunkLogger     ChunkStatusLogger
	generalLogger   ILogger
	chunkId         ChunkID
	length          int64
	positionInChunk int64
	buffer          []byte
	muMaster        *sync.Mutex
	muClose         *sync.Mutex
	isClosed        bool

	sourceInfoProvider interface {
		GetObjectRange(offset, length int64) (io.ReadCloser, error)
	}

	useStreaming  bool
	stream        *s3StreamReader
	streamHasher  hash.Hash
	reservedBytes int64
}

func NewS3ChunkReader(
	ctx context.Context,
	sourceInfoProvider interface {
		GetObjectRange(offset, length int64) (io.ReadCloser, error)
	},
	chunkId ChunkID,
	length int64,
	chunkLogger ChunkStatusLogger,
	generalLogger ILogger,
	slicePool ByteSlicePooler,
	cacheLimiter CacheLimiter,
	allowStreaming bool,
) SingleChunkReader {
	if length <= 0 {
		return &emptyChunkReader{}
	}

	useStreaming := shouldUseS3Streaming(length, allowStreaming)

	return &S3ChunkReader{
		ctx:                ctx,
		sourceInfoProvider: sourceInfoProvider,
		chunkId:            chunkId,
		length:             length,
		chunkLogger:        chunkLogger,
		generalLogger:      generalLogger,
		slicePool:          slicePool,
		cacheLimiter:       cacheLimiter,
		muMaster:           &sync.Mutex{},
		muClose:            &sync.Mutex{},
		useStreaming:       useStreaming,
	}
}

func (cr *S3ChunkReader) use() {
	cr.muMaster.Lock()
	cr.muClose.Lock()
}

func (cr *S3ChunkReader) unuse() {
	cr.muClose.Unlock()
	cr.muMaster.Unlock()
}

func (cr *S3ChunkReader) BlockingPrefetch(_ io.ReaderAt, isRetry bool) error {
	cr.use()
	defer cr.unuse()
	return cr.ensurePrefetchLocked(isRetry)
}

func (cr *S3ChunkReader) ensurePrefetchLocked(isRetry bool) error {
	if cr.useStreaming {
		if cr.stream != nil {
			return nil
		}

		if cr.chunkLogger != nil {
			cr.chunkLogger.LogChunkStatus(cr.chunkId, EWaitReason.RAMToSchedule())
		}

		reservation := s3StreamingReservation(cr.length)
		if cr.reservedBytes == 0 && reservation > 0 {
			if err := cr.cacheLimiter.WaitUntilAdd(cr.ctx, reservation, func() bool { return isRetry }); err != nil {
				return err
			}
			cr.reservedBytes = reservation
		}

		if cr.chunkLogger != nil {
			cr.chunkLogger.LogChunkStatus(cr.chunkId, EWaitReason.DiskIO())
		}

		cr.stream = newS3StreamReader(cr.ctx, cr.sourceInfoProvider, cr.chunkId, cr.length)
		return nil
	}

	if cr.buffer != nil {
		return nil
	}

	if cr.chunkLogger != nil {
		cr.chunkLogger.LogChunkStatus(cr.chunkId, EWaitReason.RAMToSchedule())
	}
	err := cr.cacheLimiter.WaitUntilAdd(cr.ctx, cr.length, func() bool { return isRetry })
	if err != nil {
		return err
	}

	if cr.chunkLogger != nil {
		cr.chunkLogger.LogChunkStatus(cr.chunkId, EWaitReason.DiskIO())
	}

	targetBuffer := cr.slicePool.RentSlice(cr.length)
	if S3StreamingVerbose() {
		GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: allocated buffer len=%d chunkId=%v", cr.length, cr.chunkId))
	}

	opName := fmt.Sprintf("s3 chunk prefetch (chunkId=%v)", cr.chunkId)
	prefetchStart := time.Now()

	_, err = WithNetworkRetry(cr.ctx, nil, opName, func() (struct{}, error) {
		start := time.Now()
		cr.muClose.Unlock()

		body, getErr := cr.sourceInfoProvider.GetObjectRange(cr.chunkId.offsetInFile, cr.length)
		var n int
		var readErr error
		if getErr == nil {
			func() {
				defer func() {
					if body != nil {
						body.Close()
					}
				}()
				n, readErr = io.ReadFull(body, targetBuffer)
			}()
		}

		cr.muClose.Lock()
		if getErr != nil {
			if S3StreamingVerbose() {
				GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: GetObjectRange failed chunkId=%v err=%v", cr.chunkId, getErr))
			}
			return struct{}{}, getErr
		}
		if readErr != nil {
			if S3StreamingVerbose() {
				GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: read response failed chunkId=%v err=%v", cr.chunkId, readErr))
			}
			return struct{}{}, readErr
		}
		if cr.isClosed {
			return struct{}{}, errors.New("closed while reading")
		}
		if err := cr.ctx.Err(); err != nil {
			return struct{}{}, err
		}
		if int64(n) != cr.length {
			mismatchErr := fmt.Errorf("bytes read not equal to expected length: got %d expected %d", n, cr.length)
			if S3StreamingVerbose() {
				GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: length check failed chunkId=%v err=%v", cr.chunkId, mismatchErr))
			}
			return struct{}{}, mismatchErr
		}

		cr.buffer = targetBuffer
		if S3StreamingVerbose() {
			GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: fetched %d bytes in %v chunkId=%v", n, time.Since(start), cr.chunkId))
		}
		return struct{}{}, nil
	})
	if err != nil {
		cr.slicePool.ReturnSlice(targetBuffer)
		cr.cacheLimiter.Remove(cr.length)
		return err
	}

	RecordS3DownloadMetric(cr.chunkId, cr.length, time.Since(prefetchStart), "prefetch")

	return nil
}

func (cr *S3ChunkReader) retryBlockingPrefetchIfNecessary() error {
	return cr.ensurePrefetchLocked(true)
}

func (cr *S3ChunkReader) Seek(offset int64, whence int) (int64, error) {
	DocumentationForDependencyOnChangeDetection()

	cr.use()
	defer cr.unuse()

	if cr.useStreaming {
		return cr.seekStreamingLocked(offset, whence)
	}

	newPosition := cr.positionInChunk

	switch whence {
	case io.SeekStart:
		newPosition = offset
	case io.SeekCurrent:
		newPosition += offset
	case io.SeekEnd:
		newPosition = cr.length - offset
	}

	if newPosition < 0 {
		return 0, errors.New("cannot seek to before beginning")
	}
	if newPosition > cr.length {
		newPosition = cr.length
	}

	cr.positionInChunk = newPosition
	return cr.positionInChunk, nil
}

func (cr *S3ChunkReader) Read(p []byte) (n int, err error) {
	DocumentationForDependencyOnChangeDetection()

	cr.use()
	defer cr.unuse()

	if cr.useStreaming {
		return cr.streamReadLocked(p, true)
	}

	return cr.doReadLocked(p, true)
}

func (cr *S3ChunkReader) doReadLocked(p []byte, freeBufferOnEof bool) (n int, err error) {
	if cr.positionInChunk >= cr.length {
		return 0, io.EOF
	}

	if err = cr.retryBlockingPrefetchIfNecessary(); err != nil {
		return 0, err
	}

	if cr.buffer == nil {
		panic("unexpected nil buffer")
	}
	if cr.positionInChunk >= cr.length {
		panic("unexpected EOF")
	}
	if cr.length != int64(len(cr.buffer)) {
		panic("unexpected buffer length discrepancy")
	}

	bytesCopied := copy(p, cr.buffer[cr.positionInChunk:])
	cr.positionInChunk += int64(bytesCopied)

	isEof := cr.positionInChunk >= cr.length
	if isEof {
		if freeBufferOnEof {
			cr.closeBufferLocked()
		}
		return bytesCopied, io.EOF
	}

	return bytesCopied, nil
}

func (cr *S3ChunkReader) streamReadLocked(p []byte, freeBufferOnEof bool) (int, error) {
	if cr.positionInChunk >= cr.length {
		return 0, io.EOF
	}

	if err := cr.ensurePrefetchLocked(false); err != nil {
		return 0, err
	}

	if cr.stream == nil {
		return 0, errors.New("stream not initialised")
	}

	bytesRead, err := cr.stream.Read(p)
	if bytesRead > 0 {
		cr.positionInChunk += int64(bytesRead)
		if cr.streamHasher != nil {
			if _, hashErr := cr.streamHasher.Write(p[:bytesRead]); hashErr != nil {
				return bytesRead, hashErr
			}
		}
	}

	if err == io.EOF {
		if freeBufferOnEof {
			cr.closeBufferLocked()
		}
		return bytesRead, io.EOF
	}

	return bytesRead, err
}

func (cr *S3ChunkReader) closeBuffer() {
	DocumentationForDependencyOnChangeDetection()
	cr.muMaster.Lock()
	defer cr.muMaster.Unlock()
	cr.closeBufferLocked()
}

func (cr *S3ChunkReader) closeBufferLocked() {
	if cr.useStreaming {
		cr.releaseStreamLocked()
		return
	}

	if cr.buffer == nil {
		return
	}
	cr.returnSlice(cr.buffer)
	cr.buffer = nil
}

func (cr *S3ChunkReader) returnSlice(slice []byte) {
	cr.slicePool.ReturnSlice(slice)
	cr.cacheLimiter.Remove(int64(len(slice)))
}

func (r *S3ChunkReader) Length() int64 {
	return r.length
}

func (cr *S3ChunkReader) Close() error {
	if cr.positionInChunk < cr.length && cr.ctx.Err() == nil {
		cr.generalLogger.Log(LogInfo, "Early close of chunk in S3ChunkReader with context still active")
	}

	cr.muClose.Lock()
	defer cr.muClose.Unlock()

	cr.closeBufferLocked()
	cr.isClosed = true
	cr.chunkLogger = nil

	return nil
}

func (cr *S3ChunkReader) GetPrologueState() PrologueState {
	cr.use()

	const mimeRecognitionLen = 512

	if cr.useStreaming {
		leadingBytes, err := cr.fetchStreamingPrologue(mimeRecognitionLen)
		cr.unuse()
		if err != nil {
			return PrologueState{}
		}
		_, _ = cr.Seek(0, io.SeekStart)
		return PrologueState{LeadingBytes: leadingBytes}
	}

	leadingBytes := make([]byte, mimeRecognitionLen)
	n, err := cr.doReadLocked(leadingBytes, false)
	if err != nil && err != io.EOF {
		cr.unuse()
		return PrologueState{}
	}
	if n < len(leadingBytes) {
		leadingBytes = leadingBytes[:n]
	}
	cr.unuse()
	_, _ = cr.Seek(0, io.SeekStart)
	return PrologueState{LeadingBytes: leadingBytes}
}

func (cr *S3ChunkReader) fetchStreamingPrologue(maxLen int) ([]byte, error) {
	if cr.length == 0 || maxLen == 0 {
		return []byte{}, nil
	}

	requestLen := int64(maxLen)
	if requestLen > cr.length {
		requestLen = cr.length
	}

	body, err := cr.sourceInfoProvider.GetObjectRange(cr.chunkId.offsetInFile, requestLen)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	buf := make([]byte, requestLen)
	n, err := io.ReadFull(body, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
}

func (cr *S3ChunkReader) HasPrefetchedEntirelyZeros() bool {
	if cr.useStreaming {
		return false
	}

	cr.use()
	defer cr.unuse()

	if cr.buffer == nil {
		return false
	}

	for _, b := range cr.buffer {
		if b != 0 {
			return false
		}
	}
	return true
}

func (cr *S3ChunkReader) WriteBufferTo(h hash.Hash) {
	DocumentationForDependencyOnChangeDetection()

	cr.use()
	defer cr.unuse()

	if cr.useStreaming {
		cr.streamHasher = h
		return
	}

	if cr.buffer == nil {
		panic("invalid state. No prefetch buffer is present")
	}
	_, err := h.Write(cr.buffer)
	if err != nil {
		panic("documentation of hash.Hash.Write says it will never return an error")
	}
}

func (cr *S3ChunkReader) seekStreamingLocked(offset int64, whence int) (int64, error) {
	newPosition := cr.positionInChunk

	switch whence {
	case io.SeekStart:
		newPosition = offset
	case io.SeekCurrent:
		newPosition += offset
	case io.SeekEnd:
		newPosition = cr.length - offset
	}

	if newPosition < 0 {
		return 0, errors.New("cannot seek to before beginning")
	}
	if newPosition > cr.length {
		newPosition = cr.length
	}

	cr.positionInChunk = newPosition
	if cr.stream != nil {
		if _, err := cr.stream.Seek(newPosition, io.SeekStart); err != nil {
			return 0, err
		}
	}

	return cr.positionInChunk, nil
}

func (cr *S3ChunkReader) releaseStreamLocked() {
	if cr.stream != nil {
		_ = cr.stream.Close()
		cr.stream = nil
	}
	if cr.reservedBytes > 0 {
		cr.cacheLimiter.Remove(cr.reservedBytes)
		cr.reservedBytes = 0
	}
	cr.streamHasher = nil
}

// s3StreamReader provides a streaming view over an S3 range while supporting seeks for retries.
type s3StreamReader struct {
	ctx      context.Context
	provider interface {
		GetObjectRange(offset, length int64) (io.ReadCloser, error)
	}
	chunkID         ChunkID
	length          int64
	position        int64
	body            io.ReadCloser
	downloadStart   time.Time
	metricsEnabled  bool
	metricsRecorded bool
}

func newS3StreamReader(
	ctx context.Context,
	provider interface {
		GetObjectRange(offset, length int64) (io.ReadCloser, error)
	},
	chunkID ChunkID,
	length int64,
) *s3StreamReader {
	return &s3StreamReader{
		ctx:            ctx,
		provider:       provider,
		chunkID:        chunkID,
		length:         length,
		metricsEnabled: S3StreamingMetricsEnabled(),
	}
}

func (sr *s3StreamReader) ensureBody() error {
	if sr.body != nil {
		return nil
	}
	if sr.position >= sr.length {
		return io.EOF
	}

	offset := sr.chunkID.offsetInFile + sr.position
	remaining := sr.length - sr.position
	body, err := sr.provider.GetObjectRange(offset, remaining)
	if err != nil {
		return err
	}

	sr.body = body
	if sr.metricsEnabled {
		sr.downloadStart = time.Now()
		sr.metricsRecorded = false
	}
	return nil
}

func (sr *s3StreamReader) Read(p []byte) (int, error) {
	if err := sr.ensureBody(); err != nil {
		return 0, err
	}

	n, err := sr.body.Read(p)
	sr.position += int64(n)

	if err == io.EOF || sr.position >= sr.length {
		_ = sr.closeBody()
		if sr.metricsEnabled && !sr.metricsRecorded && sr.length > 0 {
			duration := time.Since(sr.downloadStart)
			RecordS3DownloadMetric(sr.chunkID, sr.length, duration, "stream")
			sr.metricsRecorded = true
		}
		return n, io.EOF
	}

	return n, err
}

func (sr *s3StreamReader) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = sr.position + offset
	case io.SeekEnd:
		target = sr.length + offset
	default:
		return 0, errors.New("invalid whence")
	}

	if target < 0 {
		return 0, errors.New("cannot seek to before beginning")
	}
	if target > sr.length {
		target = sr.length
	}

	sr.position = target
	_ = sr.closeBody()
	return sr.position, nil
}

func (sr *s3StreamReader) Close() error {
	return sr.closeBody()
}

func (sr *s3StreamReader) closeBody() error {
	if sr.body == nil {
		return nil
	}
	err := sr.body.Close()
	sr.body = nil
	return err
}
