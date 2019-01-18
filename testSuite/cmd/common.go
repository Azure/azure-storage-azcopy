package cmd

import (
	"fmt"
	"os"
	"strings"

	minio "github.com/minio/minio-go"
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
	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

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
