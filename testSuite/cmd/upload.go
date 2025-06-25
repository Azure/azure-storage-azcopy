package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

type testUploader struct {
	srcPath   string
	destURL   url.URL
	recursive bool
}

type testUploadTransfer struct {
	source           string
	destURL          url.URL
	lastModifiedTime time.Time
	sourceSize       int64
}

type uploadFunc func(testUploadTransfer) error

// initializes the upload command, its aliases and description.
func init() {
	uploader := testUploader{}

	serviceType := EServiceType.S3()
	serviceTypeStr := ""
	recursive := false

	uploadCmd := &cobra.Command{
		Use:     "upload",
		Aliases: []string{"upload"},
		Short:   "upload resources for testing.",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("invalid arguments for upload command")
			}
			uploader.srcPath = args[0]
			destURL, err := url.Parse(args[1])
			if strings.Contains(destURL.Host, "amazon") {
				serviceType = EServiceType.S3()
			} else if strings.Contains(destURL.Host, "google") {
				serviceType = EServiceType.GCP()
			}
			if err != nil {
				return errors.New("invalid destination, should be a valid URL")
			}
			uploader.destURL = *destURL
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			err := (&serviceType).Parse(serviceTypeStr)
			if err != nil {
				panic(fmt.Errorf("fail to parse service type %q, %v", serviceTypeStr, err))
			}

			switch serviceType {
			case EServiceType.S3():
				uploader.recursive = recursive
				uploader.uploadToS3()
			case EServiceType.GCP():
				uploader.recursive = recursive
				uploader.uploadToGCP()
			default:
				panic(fmt.Errorf("illegal serviceType %q", serviceType))
			}
		},
	}
	rootCmd.AddCommand(uploadCmd)

	uploadCmd.PersistentFlags().StringVar(&serviceTypeStr, "serviceType", "S3", "Service type, could be S3 currently.")
	uploadCmd.PersistentFlags().BoolVar(&recursive, "recursive", false, "Whether to upload recursively.")
}

// Supports:
// a. upload single local file to single S3 object.
// b. upload single local directory to S3 bucket recursively or non-recursively.
func (u *testUploader) uploadToS3() {
	s3URLParts, err := common.NewS3URLParts(u.destURL)
	if err != nil {
		fmt.Println("fail to upload to S3, ", err)
		os.Exit(1)
	}

	s3Client := createS3ClientWithMinio(createS3ResOptions{
		Location: s3URLParts.Region,
	})

	uf := func(t testUploadTransfer) error {
		f, err := os.Open(t.source)
		if err != nil {
			return err
		}

		s3URLPartsForFile, err := common.NewS3URLParts(t.destURL)
		if err != nil {
			return err
		}
		if _, err := s3Client.PutObject(s3URLPartsForFile.BucketName, s3URLPartsForFile.ObjectKey, f, t.sourceSize, minio.PutObjectOptions{}); err != nil {
			return err
		}

		fmt.Printf("%q uploaded to %q successfully\n", t.source, t.destURL.String())

		return nil
	}

	if err := u.enumerateLocalDir(uf); err != nil {
		fmt.Println("fail to upload to S3, ", err)
		os.Exit(1)
	}
}

func (u *testUploader) uploadToGCP() {
	_, err := common.NewGCPURLParts(u.destURL)
	if err != nil {
		fmt.Println("fail to upload to GCP, ", err)
		os.Exit(1)
	}

	gcpClient, _ := createGCPClientWithGCSSDK()

	uf := func(t testUploadTransfer) error {
		f, err := os.Open(t.source)
		if err != nil {
			return err
		}

		gcpURLPartsForFile, err := common.NewGCPURLParts(t.destURL)
		if err != nil {
			return err
		}
		obj := gcpClient.Bucket(gcpURLPartsForFile.BucketName).Object(gcpURLPartsForFile.ObjectKey)
		wc := obj.NewWriter(context.Background())
		_, _ = io.Copy(wc, f)
		err = wc.Close()
		if err != nil {
			return err
		}

		fmt.Printf("%q uploaded to %q successfully\n", t.source, t.destURL.String())

		return nil
	}

	if err := u.enumerateLocalDir(uf); err != nil {
		fmt.Println("fail to upload to GCP, ", err)
		os.Exit(1)
	}
}

// Common func for enumerating local dir
func (u *testUploader) enumerateLocalDir(uf uploadFunc) error {
	// list the source files and directories
	listOfFilesAndDirectories, err := filepath.Glob(u.srcPath)
	if err != nil || len(listOfFilesAndDirectories) == 0 {
		return errors.New("cannot find source to upload")
	}

	// Single file upload
	if len(listOfFilesAndDirectories) == 1 {
		f, err := os.Stat(listOfFilesAndDirectories[0])
		if err != nil {
			return errors.New("cannot find source to upload")
		}

		if f.Mode().IsRegular() {
			if err = uf(testUploadTransfer{
				source:           listOfFilesAndDirectories[0],
				destURL:          u.destURL,
				lastModifiedTime: f.ModTime(),
				sourceSize:       f.Size(),
			}); err != nil {
				return err
			}
			return nil
		}
	}

	// Upload from directory
	for _, fileOrDirectoryPath := range listOfFilesAndDirectories {
		tempDestURL := u.destURL
		f, err := os.Stat(fileOrDirectoryPath)
		if err == nil {
			// directories are uploaded only if recursive is on
			if f.IsDir() && u.recursive {
				// walk goes through the entire directory tree
				return filepath.Walk(fileOrDirectoryPath, func(pathToFile string, f os.FileInfo, err error) error {
					if err != nil {
						return fmt.Errorf("Accessing %s failed with error %s", pathToFile, err.Error())
					}
					if f.IsDir() {
						// skip dir
						return nil
					} else if f.Mode().IsRegular() { // If the resource is file
						// replace the OS path separator in pathToFile string with AZCOPY_PATH_SEPARATOR
						// this replacement is done to handle the windows file paths where path separator "\\"
						pathToFile = strings.Replace(pathToFile, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
						fileOrDirectoryPath = strings.Replace(fileOrDirectoryPath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

						tempDestURL.Path = combineURLStr(u.destURL.Path, getRelativePath(fileOrDirectoryPath, pathToFile))
						if err = uf(testUploadTransfer{
							source:           pathToFile,
							destURL:          tempDestURL,
							lastModifiedTime: f.ModTime(),
							sourceSize:       f.Size(),
						}); err != nil {
							return err
						}
					} else {
						return fmt.Errorf("Special file %q, with mode %q not supported", fileOrDirectoryPath, f.Mode())
					}
					return nil
				})
			} else if f.Mode().IsRegular() {
				// replace the OS path separator in fileOrDirectoryPath string with AZCOPY_PATH_SEPARATOR
				// this replacement is done to handle the windows file paths where path separator "\\"
				pathToFile := strings.Replace(fileOrDirectoryPath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

				tempDestURL.Path = combineURLStr(u.destURL.Path, f.Name())
				if err = uf(testUploadTransfer{
					source:           pathToFile,
					destURL:          tempDestURL,
					lastModifiedTime: f.ModTime(),
					sourceSize:       f.Size(),
				}); err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("error %s accessing the filepath %s", err.Error(), fileOrDirectoryPath)
		}
	}

	return nil
}

func combineURLStr(destinationPath, fileName string) string {
	if strings.LastIndex(destinationPath, "/") == len(destinationPath)-1 {
		return fmt.Sprintf("%s%s", destinationPath, fileName)
	}
	return fmt.Sprintf("%s/%s", destinationPath, fileName)
}

func getRelativePath(rootPath, filePath string) string {
	// root path contains the entire absolute path to the root directory, so we need to take away everything except the root directory from filePath
	// example: rootPath = "/dir1/dir2/dir3" filePath = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt" scrubAway="/dir1/dir2/"
	if len(rootPath) == 0 {
		return filePath
	}

	// replace the path separator in filepath with AZCOPY_PATH_SEPARATOR
	// this replacement is required to handle the windows filepath
	filePath = strings.Replace(filePath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
	var scrubAway string
	// test if root path finishes with a /, if yes, ignore it
	if rootPath[len(rootPath)-1:] == common.AZCOPY_PATH_SEPARATOR_STRING {
		scrubAway = rootPath[:strings.LastIndex(rootPath[:len(rootPath)-1], common.AZCOPY_PATH_SEPARATOR_STRING)+1]
	} else {
		// +1 because we want to include the / at the end of the dir
		scrubAway = rootPath[:strings.LastIndex(rootPath, common.AZCOPY_PATH_SEPARATOR_STRING)+1]
	}

	result := strings.Replace(filePath, scrubAway, "", 1)

	return result
}
