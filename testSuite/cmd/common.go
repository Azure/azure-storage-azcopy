package cmd

import (
	gcpUtils "cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"net/http"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// validateString compares the two strings.
func validateString(expected string, actual string) bool {
	if strings.Compare(expected, actual) != 0 {
		return false
	}
	return true
}

type createS3ResOptions struct {
	Location string
}

func createS3ClientWithMinio(o createS3ResOptions) *minio.Client {
	accessKeyID := common.GetEnvironmentVariable(common.EEnvironmentVariable.AWSAccessKeyID())
	secretAccessKey := common.GetEnvironmentVariable(common.EEnvironmentVariable.AWSSecretAccessKey())

	if accessKeyID == "" || secretAccessKey == "" {
		fmt.Println("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY should be set before creating the S3 client")
		os.Exit(1)
	}

	s3Client, err := minio.NewWithRegion("s3.amazonaws.com", accessKeyID, secretAccessKey, true, o.Location)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return s3Client
}

func createGCPClientWithGCSSDK() (*gcpUtils.Client, error) {
	jsonKey := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if jsonKey == "" {
		return nil, fmt.Errorf("GOOGLE_APPLICATION_CREDENTIALS should be set before creating the GCP Client")
	}
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		return nil, fmt.Errorf("GOOGLE_CLOUD_PROJECT should be set before creating GCP Client for testing")
	}
	ctx := context.Background()
	gcpClient, err := gcpUtils.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return gcpClient, nil
}

func ignoreStorageConflictStatus(err error) error {
	if err != nil {
		// Skip the error, when resource already exists.
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			if respErr.StatusCode != http.StatusConflict {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}
