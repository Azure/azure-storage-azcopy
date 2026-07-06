package cred

import "github.com/minio/minio-go/v7/pkg/credentials"

// S3CredentialInfo contains essential credential info which need to build up S3 client.
type S3CredentialInfo struct {
	Endpoint   string
	Region     string
	BucketName string               // Bucket name from S3URLParts, used for GCS private network transfers
	Provider   credentials.Provider //credential provider implementation for custom credential management
}

type GCPCredentialInfo struct {
}
