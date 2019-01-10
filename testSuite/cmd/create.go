package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
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

// initializes the create command, its aliases and description.
func init() {
	resourceURL := ""

	serviceType := EServiceType.Blob()
	resourceType := EResourceType.SingleFile()
	serviceTypeStr := ""
	resourceTypeStr := ""

	blobSize := uint32(0)
	metaData := ""
	contentType := ""
	contentEncoding := ""
	contentDisposition := ""
	contentLanguage := ""
	cacheControl := ""
	contentMD5 := ""

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

			switch serviceType {
			case EServiceType.Blob():
				switch resourceType {
				case EResourceType.Bucket():
					createContainer(resourceURL)
				case EResourceType.SingleFile():
					createBlob(
						resourceURL,
						blobSize,
						getBlobMetadata(metaData),
						azblob.BlobHTTPHeaders{
							ContentType:        contentType,
							ContentDisposition: contentDisposition,
							ContentEncoding:    contentEncoding,
							ContentLanguage:    contentLanguage,
							ContentMD5:         []byte(contentMD5),
							CacheControl:       cacheControl,
						})
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
						getFileMetadata(metaData),
						azfile.FileHTTPHeaders{
							ContentType:        contentType,
							ContentDisposition: contentDisposition,
							ContentEncoding:    contentEncoding,
							ContentLanguage:    contentLanguage,
							ContentMD5:         []byte(contentMD5),
							CacheControl:       cacheControl,
						})
				default:
					panic(fmt.Errorf("not implemented %v", resourceType))
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
	createCmd.PersistentFlags().StringVar(&metaData, "metadata", "", "metadata for blob.")
	createCmd.PersistentFlags().StringVar(&contentType, "content-type", "", "content type for blob.")
	createCmd.PersistentFlags().StringVar(&contentEncoding, "content-encoding", "", "content encoding for blob.")
	createCmd.PersistentFlags().StringVar(&contentDisposition, "content-disposition", "", "content disposition for blob.")
	createCmd.PersistentFlags().StringVar(&contentLanguage, "content-language", "", "content language for blob.")
	createCmd.PersistentFlags().StringVar(&cacheControl, "cache-control", "", "cache control for blob.")
	createCmd.PersistentFlags().StringVar(&contentMD5, "content-md5", "", "content MD5 for blob.")

}

func getBlobMetadata(metadataString string) azblob.Metadata {
	var metadata azblob.Metadata

	if len(metadataString) > 0 {
		metadata = azblob.Metadata{}
		for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
			kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
			metadata[kv[0]] = kv[1]
		}
	}

	return metadata
}

func getFileMetadata(metadataString string) azfile.Metadata {
	var metadata azfile.Metadata

	if len(metadataString) > 0 {
		metadata = azfile.Metadata{}
		for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
			kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
			metadata[kv[0]] = kv[1]
		}
	}

	return metadata
}

// Can be used for overwrite scenarios.
func createContainer(container string) {
	u, err := url.Parse(container)

	if err != nil {
		fmt.Println("error parsing the container URL with SAS ", err)
		os.Exit(1)
	}

	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	containerURL := azblob.NewContainerURL(*u, p)
	_, err = containerURL.Create(context.Background(), azblob.Metadata{}, azblob.PublicAccessNone)

	if err != nil {
		fmt.Println("error createContainer, ", err)
		os.Exit(1)
	}
}

func createBlob(blobURL string, blobSize uint32, metadata azblob.Metadata, blobHTTPHeaders azblob.BlobHTTPHeaders) {
	url, err := url.Parse(blobURL)
	if err != nil {
		fmt.Println("error parsing the blob sas ", err)
		os.Exit(1)
	}
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	blobUrl := azblob.NewBlockBlobURL(*url, p)

	randomString := createStringWithRandomChars(int(blobSize))
	if blobHTTPHeaders.ContentType == "" {
		blobHTTPHeaders.ContentType = http.DetectContentType([]byte(randomString))
	}
	putBlobResp, err := blobUrl.Upload(
		context.Background(),
		strings.NewReader(randomString),
		blobHTTPHeaders,
		metadata,
		azblob.BlobAccessConditions{})
	if err != nil {
		fmt.Println(fmt.Sprintf("error uploading the blob %v", err))
		os.Exit(1)
	}
	if putBlobResp.Response() != nil {
		io.Copy(ioutil.Discard, putBlobResp.Response().Body)
		putBlobResp.Response().Body.Close()
	}
}

func createShareOrDirectory(shareOrDirectoryURLStr string) {
	u, err := url.Parse(shareOrDirectoryURLStr)

	if err != nil {
		fmt.Println("error parsing the share or directory URL with SAS ", err)
		os.Exit(1)
	}

	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})

	// Suppose it's a directory or share, try create and doesn't care if error happened.
	dirURL := azfile.NewDirectoryURL(*u, p)
	_, _ = dirURL.Create(context.Background(), azfile.Metadata{})

	shareURL := azfile.NewShareURL(*u, p)
	_, _ = shareURL.Create(context.Background(), azfile.Metadata{}, 0)

	// Finally valdiate if directory with specified URL exists, if doesn't exist, then report create failure.
	time.Sleep(1 * time.Second)

	_, err = dirURL.GetProperties(context.Background())
	if err != nil {
		fmt.Println("error createShareOrDirectory with URL, ", err)
		os.Exit(1)
	}
}

func createFile(fileURLStr string, fileSize uint32, metadata azfile.Metadata, fileHTTPHeaders azfile.FileHTTPHeaders) {
	url, err := url.Parse(fileURLStr)
	if err != nil {
		fmt.Println("error parsing the blob sas ", err)
		os.Exit(1)
	}
	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	fileURL := azfile.NewFileURL(*url, p)

	randomString := createStringWithRandomChars(int(fileSize))
	if fileHTTPHeaders.ContentType == "" {
		fileHTTPHeaders.ContentType = http.DetectContentType([]byte(randomString))
	}

	err = azfile.UploadBufferToAzureFile(context.Background(), []byte(randomString), fileURL, azfile.UploadToAzureFileOptions{
		FileHTTPHeaders: fileHTTPHeaders,
		Metadata:        metadata,
	})
	if err != nil {
		fmt.Println(fmt.Sprintf("error uploading the file %v", err))
		os.Exit(1)
	}
}
