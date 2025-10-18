package cmd

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"net/url"
	"os"
	"time"

	gcpUtils "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	sharedirectory "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	sharefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"

	"io"
	"math/rand"
	"net/http"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	minio "github.com/minio/minio-go"
	"github.com/spf13/cobra"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func createStringWithRandomChars(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Int()%len(charset)]
	}
	return string(b)
}

var genMD5 = false

// initializes the create command, its aliases and description.
func init() {
	resourceURL := ""

	serviceType := EServiceType.Blob()
	resourceType := EResourceType.SingleFile()
	serviceTypeStr := ""
	resourceTypeStr := ""

	blobSize := uint32(0)
	metadata := ""
	contentType := ""
	contentEncoding := ""
	contentDisposition := ""
	contentLanguage := ""
	cacheControl := ""
	contentMD5 := ""
	location := ""
	var tier *blob.AccessTier = nil

	createCmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"create"},
		Short:   "create creates resource.",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return fmt.Errorf("invalid arguments for create command")
			}
			resourceURL = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			err := (&serviceType).Parse(serviceTypeStr)
			if err != nil {
				panic(fmt.Errorf("fail to parse service type %q, %v", serviceTypeStr, err))
			}
			err = (&resourceType).Parse(resourceTypeStr)
			if err != nil {
				panic(fmt.Errorf("fail to parse resource type %q, %v", resourceTypeStr, err))
			}

			var md5 []byte
			if contentMD5 != "" {
				md5 = []byte(contentMD5)
			}

			switch serviceType {
			case EServiceType.Blob():
				switch resourceType {
				case EResourceType.Bucket():
					createContainer(resourceURL)
				case EResourceType.SingleFile():
					createBlob(
						resourceURL,
						blobSize,
						getMetadata(metadata),
						&blob.HTTPHeaders{
							BlobContentType:        &contentType,
							BlobContentDisposition: &contentDisposition,
							BlobContentEncoding:    &contentEncoding,
							BlobContentLanguage:    &contentLanguage,
							BlobContentMD5:         md5,
							BlobCacheControl:       &cacheControl,
						}, tier)
				default:
					panic(fmt.Errorf("not implemented %v", resourceType))
				}
			case EServiceType.File():
				switch resourceType {
				case EResourceType.Bucket():
					createShareOrDirectory(resourceURL)
				case EResourceType.SingleFile():
					createFile(
						resourceURL,
						blobSize,
						getMetadata(metadata),
						&sharefile.HTTPHeaders{
							ContentType:        &contentType,
							ContentDisposition: &contentDisposition,
							ContentEncoding:    &contentEncoding,
							ContentLanguage:    &contentLanguage,
							ContentMD5:         md5,
							CacheControl:       &cacheControl,
						})
				default:
					panic(fmt.Errorf("not implemented %v", resourceType))
				}
			case EServiceType.S3():
				switch resourceType {
				case EResourceType.Bucket():
					createBucket(resourceURL)
				case EResourceType.SingleFile():
					// For S3, no content-MD5 will be returned during HEAD, i.e. no content-MD5 will be preserved during copy.
					// And content-MD5 header is not set during upload. E.g. in S3 management portal, no property content-MD5 can be set.
					// So here create object without content-MD5 as common practice.
					createObject(
						resourceURL,
						blobSize,
						minio.PutObjectOptions{
							ContentType:        contentType,
							ContentDisposition: contentDisposition,
							ContentEncoding:    contentEncoding,
							ContentLanguage:    contentLanguage,
							CacheControl:       cacheControl,
							UserMetadata:       getS3Metadata(metadata),
						})
				default:
					panic(fmt.Errorf("not implemented %v", resourceType))
				}
			case EServiceType.GCP():
				switch resourceType {
				case EResourceType.Bucket():
					createGCPBucket(resourceURL)
				case EResourceType.SingleFile():
					createGCPObject(resourceURL, blobSize, gcpUtils.ObjectAttrsToUpdate{
						ContentType:        contentType,
						ContentDisposition: contentDisposition,
						ContentEncoding:    contentEncoding,
						ContentLanguage:    contentLanguage,
						CacheControl:       cacheControl,
						Metadata:           getS3Metadata(metadata),
					})
				}
			case EServiceType.BlobFS():
				panic(fmt.Errorf("not implemented %v", serviceType))
			default:
				panic(fmt.Errorf("illegal resourceType %q", resourceType))
			}
		},
	}
	rootCmd.AddCommand(createCmd)

	createCmd.PersistentFlags().StringVar(&serviceTypeStr, "serviceType", "Blob", "Service type, could be blob, file or blobFS currently.")
	createCmd.PersistentFlags().StringVar(&resourceTypeStr, "resourceType", "SingleFile", "Resource type, could be a single file, bucket.")
	createCmd.PersistentFlags().Uint32Var(&blobSize, "blob-size", 0, "")
	createCmd.PersistentFlags().StringVar(&metadata, "metadata", "", "metadata for blob.")
	createCmd.PersistentFlags().StringVar(&contentType, "content-type", "", "content type for blob.")
	createCmd.PersistentFlags().StringVar(&contentEncoding, "content-encoding", "", "content encoding for blob.")
	createCmd.PersistentFlags().StringVar(&contentDisposition, "content-disposition", "", "content disposition for blob.")
	createCmd.PersistentFlags().StringVar(&contentLanguage, "content-language", "", "content language for blob.")
	createCmd.PersistentFlags().StringVar(&cacheControl, "cache-control", "", "cache control for blob.")
	createCmd.PersistentFlags().StringVar(&contentMD5, "content-md5", "", "content MD5 for blob.")
	createCmd.PersistentFlags().StringVar(&location, "location", "", "Location of the Azure account or S3 bucket to create")
	createCmd.PersistentFlags().BoolVar(&genMD5, "generate-md5", false, "auto-generate MD5 for a new blob")

}

func getMetadata(metadataString string) map[string]*string {
	var metadata map[string]*string

	if len(metadataString) > 0 {
		metadata = map[string]*string{}
		for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
			kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
			metadata[kv[0]] = to.Ptr(kv[1])
		}
	}

	return metadata
}

func getS3Metadata(metadataString string) map[string]string {
	metadata := make(map[string]string)

	if len(metadataString) > 0 {
		for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
			kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
			metadata[kv[0]] = kv[1]
		}
	}

	return metadata
}

// Can be used for overwrite scenarios.
func createContainer(containerURL string) {
	containerClient, _ := container.NewClientWithNoCredential(containerURL, nil)
	_, err := containerClient.Create(context.Background(), nil)

	if ignoreStorageConflictStatus(err) != nil {
		fmt.Println("fail to create container, ", err)
		os.Exit(1)
	}
}

func createBlob(blobURL string, blobSize uint32, metadata map[string]*string, blobHTTPHeaders *blob.HTTPHeaders, tier *blob.AccessTier) {
	blobClient, _ := blockblob.NewClientWithNoCredential(blobURL, nil)

	randomString := createStringWithRandomChars(int(blobSize))
	if blobHTTPHeaders.BlobContentType == nil {
		blobHTTPHeaders.BlobContentType = to.Ptr(strings.Split(http.DetectContentType([]byte(randomString)), ";")[0])
	}

	// Generate a content MD5 for the new blob if requested
	if genMD5 {
		md5hasher := md5.New()
		md5hasher.Write([]byte(randomString))
		blobHTTPHeaders.BlobContentMD5 = md5hasher.Sum(nil)
	}

	_, err := blobClient.Upload(context.Background(), streaming.NopCloser(strings.NewReader(randomString)),
		&blockblob.UploadOptions{
			HTTPHeaders: blobHTTPHeaders,
			Metadata:    metadata,
			Tier:        tier,
		})
	if err != nil {
		fmt.Printf("error uploading the blob %v\n", err)
		os.Exit(1)
	}
}

func createShareOrDirectory(shareOrDirectoryURLStr string) {
	fileURLParts, err := sharefile.ParseURL(shareOrDirectoryURLStr)
	if err != nil {
		fmt.Println("error createShareOrDirectory with URL, ", err)
		os.Exit(1)
	}

	isShare := false
	if fileURLParts.ShareName != "" && fileURLParts.DirectoryOrFilePath == "" {
		isShare = true
		// This is a share
		shareClient, _ := share.NewClientWithNoCredential(shareOrDirectoryURLStr, nil)
		_, err := shareClient.Create(context.Background(), nil)
		if ignoreStorageConflictStatus(err) != nil {
			fmt.Println("fail to create share, ", err)
			os.Exit(1)
		}
	}

	directoryClient, _ := sharedirectory.NewClientWithNoCredential(shareOrDirectoryURLStr, nil) // i.e. root directory, in share's case
	if !isShare {
		_, err := directoryClient.Create(context.Background(), nil)
		if ignoreStorageConflictStatus(err) != nil {
			fmt.Println("fail to create directory, ", err)
			os.Exit(1)
		}
	}

	// Finally valdiate if directory with specified URL exists, if doesn't exist, then report create failure.
	time.Sleep(1 * time.Second)

	_, err = directoryClient.GetProperties(context.Background(), nil)
	if err != nil {
		fmt.Println("error createShareOrDirectory with URL, ", err)
		os.Exit(1)
	}
}

func createFile(fileURLStr string, fileSize uint32, metadata map[string]*string, fileHTTPHeaders *sharefile.HTTPHeaders) {
	fileClient, _ := sharefile.NewClientWithNoCredential(fileURLStr, nil)

	randomString := createStringWithRandomChars(int(fileSize))
	if fileHTTPHeaders.ContentType == nil {
		fileHTTPHeaders.ContentType = to.Ptr(strings.Split(http.DetectContentType([]byte(randomString)), ";")[0])
	}

	// Generate a content MD5 for the new blob if requested
	if genMD5 {
		md5hasher := md5.New()
		md5hasher.Write([]byte(randomString))
		fileHTTPHeaders.ContentMD5 = md5hasher.Sum(nil)
	}

	_, err := fileClient.Create(context.Background(), int64(fileSize), &sharefile.CreateOptions{HTTPHeaders: fileHTTPHeaders, Metadata: metadata})

	if err != nil {
		fmt.Printf("error creating the file %v\n", err)
		os.Exit(1)
	}

	if fileSize > 0 {
		err = fileClient.UploadBuffer(context.Background(), []byte(randomString), nil)

		if err != nil {
			fmt.Printf("error uploading the file %v\n", err)
			os.Exit(1)
		}
	}
}

func createBucket(bucketURLStr string) {
	u, err := url.Parse(bucketURLStr)

	if err != nil {
		fmt.Println("fail to parse the bucket URL, ", err)
		os.Exit(1)
	}

	s3URLParts, err := common.NewS3URLParts(*u)
	if err != nil {
		fmt.Println("new S3 URL parts, ", err)
		os.Exit(1)
	}

	s3Client := createS3ClientWithMinio(createS3ResOptions{
		Location: s3URLParts.Region,
	})

	if err := s3Client.MakeBucket(s3URLParts.BucketName, s3URLParts.Region); err != nil {
		exists, err := s3Client.BucketExists(s3URLParts.BucketName)
		if err != nil || !exists {
			fmt.Println("fail to create bucket, ", err)
			os.Exit(1)
		}
	}
}

func createGCPBucket(bucketURLStr string) {
	u, err := url.Parse(bucketURLStr)

	if err != nil {
		fmt.Println("fail to parse the bucket URL, ", err)
		os.Exit(1)
	}

	gcpURLParts, err := common.NewGCPURLParts(*u)
	if err != nil {
		fmt.Println("new GCP URL parts, ", err)
		os.Exit(1)
	}

	gcpClient, err := createGCPClientWithGCSSDK()
	if err != nil {
		fmt.Println("Failed to create GCS Client: ", err)
	}
	bkt := gcpClient.Bucket(gcpURLParts.BucketName)
	err = bkt.Create(context.Background(), os.Getenv("GOOGLE_CLOUD_PROJECT"), &gcpUtils.BucketAttrs{})
	if err != nil {
		bkt := gcpClient.Bucket(gcpURLParts.BucketName)
		_, err := bkt.Attrs(context.Background())
		if err == nil {
			fmt.Println("fail to create bucket, ", err)
			os.Exit(1)
		}
	}
}

func createObject(objectURLStr string, objectSize uint32, o minio.PutObjectOptions) {
	u, err := url.Parse(objectURLStr)
	if err != nil {
		fmt.Println("fail to parse the object URL, ", err)
		os.Exit(1)
	}

	s3URLParts, err := common.NewS3URLParts(*u)
	if err != nil {
		fmt.Println("new S3 URL parts, ", err)
		os.Exit(1)
	}

	s3Client := createS3ClientWithMinio(createS3ResOptions{
		Location: s3URLParts.Region,
	})

	randomString := createStringWithRandomChars(int(objectSize))
	if o.ContentType == "" {
		o.ContentType = strings.Split(http.DetectContentType([]byte(randomString)), ";")[0]
	}

	_, err = s3Client.PutObject(s3URLParts.BucketName, s3URLParts.ObjectKey, bytes.NewReader([]byte(randomString)), int64(objectSize), o)

	if err != nil {
		fmt.Println("fail to upload file to S3 object, ", err)
		os.Exit(1)
	}
}

func createGCPObject(objectURLStr string, objectSize uint32, o gcpUtils.ObjectAttrsToUpdate) {
	u, err := url.Parse(objectURLStr)
	if err != nil {
		fmt.Println("fail to parse the object URL, ", err)
		os.Exit(1)
	}

	gcpURLParts, err := common.NewGCPURLParts(*u)
	if err != nil {
		fmt.Println("new GCP URL parts, ", err)
		os.Exit(1)
	}

	gcpClient, _ := createGCPClientWithGCSSDK()

	randomString := createStringWithRandomChars(int(objectSize))
	if o.ContentType == "" {
		o.ContentType = http.DetectContentType([]byte(randomString))
	}

	obj := gcpClient.Bucket(gcpURLParts.BucketName).Object(gcpURLParts.ObjectKey)
	wc := obj.NewWriter(context.Background())
	reader := strings.NewReader(randomString)
	_, _ = io.Copy(wc, reader)
	_ = wc.Close()

	_, err = obj.Update(context.Background(), o)
	if err != nil {
		fmt.Println("fail to upload file to S3 object, ", err)
		os.Exit(1)
	}
}
