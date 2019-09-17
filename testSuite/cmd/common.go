package cmd

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	minio "github.com/minio/minio-go"

	"github.com/Azure/azure-storage-azcopy/common"
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
	lcm := common.GetLifecycleMgr()
	accessKeyID := lcm.GetEnvironmentVariable(common.EEnvironmentVariable.AWSAccessKeyID())
	secretAccessKey := lcm.GetEnvironmentVariable(common.EEnvironmentVariable.AWSSecretAccessKey())

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

func ignoreStorageConflictStatus(err error) error {
	if err != nil {
		// Skip the error, when resource already exists.
		if stgErr, ok := err.(azblob.StorageError); !ok ||
			(stgErr.Response().StatusCode != http.StatusConflict) {
			return err
		}
	}

	return nil
}
