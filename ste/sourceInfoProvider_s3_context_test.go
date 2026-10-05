package ste

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

type s3SourceContextKey struct{}

type s3SourceContextManager struct {
	IJobPartTransferMgr
	ctx context.Context
}

func (m s3SourceContextManager) Context() context.Context { return m.ctx }

type s3SourceMemoryTransport struct {
	t     *testing.T
	calls int
}

func (transport *s3SourceMemoryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	require.Equal(transport.t, "transfer-context", request.Context().Value(s3SourceContextKey{}))
	transport.calls++
	body := []byte("data")
	headers := http.Header{
		"Content-Length": {"4"},
		"Last-Modified":  {time.Unix(1700000000, 0).UTC().Format(http.TimeFormat)},
		"Etag":           {`"8d777f385d3dfec8815d20f7496026dc"`},
	}
	if request.Method == http.MethodHead {
		body = nil
	}
	return &http.Response{StatusCode: http.StatusOK, Header: headers, Body: io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)), Request: request}, nil
}

func TestM6S3SourceUsesTransferContextForAllReads(t *testing.T) {
	transport := &s3SourceMemoryTransport{t: t}
	client, err := minio.New("unit-test.invalid", &minio.Options{
		Creds:  credentials.NewStatic("", "", "", credentials.SignatureAnonymous),
		Secure: true, Region: "us-east-1", Transport: transport,
	})
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), s3SourceContextKey{}, "transfer-context")
	source := s3SourceInfoProvider{
		jptm: s3SourceContextManager{ctx: ctx}, s3Client: client,
		s3URLPart:    common.S3URLParts{BucketName: "unit-test-bucket", ObjectKey: "object"},
		transferInfo: &TransferInfo{S2SGetPropertiesInBackend: true},
	}
	_, err = source.Properties()
	require.NoError(t, err)
	_, err = source.GetFreshFileLastModifiedTime()
	require.NoError(t, err)
	md5, err := source.GetMD5(0, 4)
	require.NoError(t, err)
	require.Len(t, md5, 16)
	reader, err := source.GetObjectRange(0, 4)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, []byte("data"), data)
	require.GreaterOrEqual(t, transport.calls, 4)
}
