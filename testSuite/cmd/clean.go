package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/JeffreyRichter/enum/enum"
	"github.com/spf13/cobra"
)

var EResourceType = ResourceType(0)

// ResourceType defines the different types of credentials
type ResourceType uint8

func (ResourceType) SingleFile() ResourceType { return ResourceType(0) }
func (ResourceType) Bucket() ResourceType     { return ResourceType(1) }
func (ResourceType) Account() ResourceType    { return ResourceType(2) } // For SAS or public.

func (ct ResourceType) String() string {
	return enum.StringInt(ct, reflect.TypeOf(ct))
}
func (ct *ResourceType) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(ct), s, true, true)
	if err == nil {
		*ct = val.(ResourceType)
	}
	return err
}

var EServiceType = ServiceType(0)

// ServiceType defines the different types of credentials
type ServiceType uint8

func (ServiceType) Blob() ServiceType   { return ServiceType(0) }
func (ServiceType) File() ServiceType   { return ServiceType(1) }
func (ServiceType) BlobFS() ServiceType { return ServiceType(2) } // For SAS or public.
func (ServiceType) S3() ServiceType     { return ServiceType(3) }

func (ct ServiceType) String() string {
	return enum.StringInt(ct, reflect.TypeOf(ct))
}
func (ct *ServiceType) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(ct), s, true, true)
	if err == nil {
		*ct = val.(ServiceType)
	}
	return err
}

// initializes the clean command, its aliases and description.
func init() {
	resourceURL := ""
	serviceType := EServiceType.Blob()
	resourceType := EResourceType.SingleFile()

	var serviceTypeStr string
	var resourceTypeStr string

	cleanCmd := &cobra.Command{
		Use:     "clean",
		Aliases: []string{"clean"},
		Short:   "clean deletes everything inside the container.",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return fmt.Errorf("invalid arguments for clean command")
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
					cleanContainer(resourceURL)
				case EResourceType.SingleFile():
					cleanBlob(resourceURL)
				case EResourceType.Account():
					cleanBlobAccount(resourceURL)
				}
			case EServiceType.File():
				switch resourceType {
				case EResourceType.Bucket():
					cleanShare(resourceURL)
				case EResourceType.SingleFile():
					cleanFile(resourceURL)
				case EResourceType.Account():
					cleanFileAccount(resourceURL)
				}
			case EServiceType.BlobFS():
				switch resourceType {
				case EResourceType.Bucket():
					cleanFileSystem(resourceURL)
				case EResourceType.SingleFile():
					cleanBfsFile(resourceURL)
				case EResourceType.Account():
					cleanBfsAccount(resourceURL)
				}
			case EServiceType.S3():
				switch resourceType {
				case EResourceType.Bucket():
					cleanBucket(resourceURL)
				case EResourceType.SingleFile():
					cleanObject(resourceURL)
				case EResourceType.Account():
					cleanS3Account(resourceURL)
				}
			default:
				panic(fmt.Errorf("illegal resourceType %q", resourceType))
			}
		},
	}
	rootCmd.AddCommand(cleanCmd)

	cleanCmd.PersistentFlags().StringVar(&resourceTypeStr, "resourceType", "SingleFile", "Resource type, could be single file, bucket or account currently.")
	cleanCmd.PersistentFlags().StringVar(&serviceTypeStr, "serviceType", "Blob", "Account type, could be blob, file or blobFS currently.")
}

func cleanContainer(container string) {
	containerURLBase, err := url.Parse(container)

	if err != nil {
		fmt.Println("error parsing the container sas, ", err)
		os.Exit(1)
	}

	p := createBlobPipeline(*containerURLBase)
	containerUrl := azblob.NewContainerURL(*containerURLBase, p)

	// Create the container. This will fail if it's already present but this saves us the pain of a container being missing for one reason or another.
	_, _ = containerUrl.Create(context.Background(), azblob.Metadata{}, azblob.PublicAccessNone)

	// perform a list blob
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		listBlob, err := containerUrl.ListBlobsFlatSegment(context.Background(), marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			fmt.Println("error listing blobs inside the container. Please check the container sas", err)
			os.Exit(1)
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			_, err := containerUrl.NewBlobURL(blobInfo.Name).Delete(context.Background(), "include", azblob.BlobAccessConditions{})
			if err != nil {
				fmt.Println("error deleting the blob from container ", blobInfo.Name)
				os.Exit(1)
			}
		}
		marker = listBlob.NextMarker
	}
}

func cleanBlob(blob string) {
	blobURLBase, err := url.Parse(blob)

	if err != nil {
		fmt.Println("error parsing the container sas ", err)
		os.Exit(1)
	}

	p := createBlobPipeline(*blobURLBase)
	blobUrl := azblob.NewBlobURL(*blobURLBase, p)

	_, err = blobUrl.Delete(context.Background(), "include", azblob.BlobAccessConditions{})
	if err != nil {
		fmt.Println("error deleting the blob ", err)
		os.Exit(1)
	}
}

func cleanShare(shareURLStr string) {
	u, err := url.Parse(shareURLStr)

	if err != nil {
		fmt.Println("error parsing the share URL with SAS ", err)
		os.Exit(1)
	}

	p := createFilePipeline(*u)
	shareURL := azfile.NewShareURL(*u, p)

	// Create the share. This will fail if it's already present but this saves us the pain of a container being missing for one reason or another.
	_, _ = shareURL.Create(context.Background(), azfile.Metadata{}, 0)

	_, err = shareURL.Delete(context.Background(), azfile.DeleteSnapshotsOptionInclude)
	if err != nil {
		sErr, sErrOk := err.(azfile.StorageError)
		if sErrOk && sErr.Response().StatusCode != http.StatusNotFound {
			fmt.Fprintf(os.Stdout, "error deleting the share for clean share, error '%v'\n", err)
			os.Exit(1)
		}
	}

	// Sleep seconds to wait the share deletion got succeeded
	time.Sleep(45 * time.Second)

	_, err = shareURL.Create(context.Background(), azfile.Metadata{}, 0)
	if err != nil {
		fmt.Fprintf(os.Stdout, "error creating the share for clean share, error '%v'\n", err)
		os.Exit(1)
	}
}

func cleanFile(fileURLStr string) {
	u, err := url.Parse(fileURLStr)

	if err != nil {
		fmt.Println("error parsing the file URL with SAS", err)
		os.Exit(1)
	}

	p := createFilePipeline(*u)
	fileURL := azfile.NewFileURL(*u, p)

	_, err = fileURL.Delete(context.Background())
	if err != nil {
		fmt.Println("error deleting the file ", err)
		os.Exit(1)
	}
}

func createBlobPipeline(u url.URL) pipeline.Pipeline {
	// Get name and key variables from environment.
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	blobURLParts := azblob.NewBlobURLParts(u)
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in the environment, and there is no SAS token present
	if (name == "" && key == "") && blobURLParts.SAS.Encode() == "" {
		fmt.Println("ACCOUNT_NAME and ACCOUNT_KEY should be set, or a SAS token should be supplied before cleaning the file system")
		os.Exit(1)
	}
	// create the pipeline, preferring SAS over account name/key
	if blobURLParts.SAS.Encode() != "" {
		return azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	}

	c, err := azblob.NewSharedKeyCredential(name, key)
	if err != nil {
		fmt.Println("Failed to create shared key credential!")
		os.Exit(1)
	}
	return azblob.NewPipeline(c, azblob.PipelineOptions{})
}

func createFilePipeline(u url.URL) pipeline.Pipeline {
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	fileURLParts := azfile.NewFileURLParts(u)
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in the environment, and there is no SAS token present
	if (name == "" && key == "") && fileURLParts.SAS.Encode() == "" {
		fmt.Println("ACCOUNT_NAME and ACCOUNT_KEY should be set, or a SAS token should be supplied before cleaning the file system")
		os.Exit(1)
	}

	// create the pipeline, preferring SAS over account name/key
	if fileURLParts.SAS.Encode() != "" {
		return azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	}

	c, err := azfile.NewSharedKeyCredential(name, key)
	if err != nil {
		fmt.Println("Failed to create shared key credential!")
		os.Exit(1)
	}
	return azfile.NewPipeline(c, azfile.PipelineOptions{})
}

func createBlobFSPipeline(u url.URL) pipeline.Pipeline {
	// Get the Account Name and Key variables from environment
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	bfsURLParts := azbfs.NewBfsURLParts(u)
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
	if (name == "" && key == "") && bfsURLParts.SAS.Encode() == "" {
		fmt.Println("ACCOUNT_NAME and ACCOUNT_KEY should be set, or a SAS token should be supplied before cleaning the file system")
		os.Exit(1)
	}
	// create the blob fs pipeline
	if bfsURLParts.SAS.Encode() != "" {
		return azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{})
	}

	c := azbfs.NewSharedKeyCredential(name, key)
	return azbfs.NewPipeline(c, azbfs.PipelineOptions{})
}

func cleanFileSystem(fsURLStr string) {
	ctx := context.Background()
	u, err := url.Parse(fsURLStr)

	if err != nil {
		fmt.Println("error parsing the file system URL", err)
		os.Exit(1)
	}

	fsURL := azbfs.NewFileSystemURL(*u, createBlobFSPipeline(*u))
	// Instead of error checking the delete, error check the create.
	// If the filesystem is deleted somehow, this recovers us from CI hell.
	_, _ = fsURL.Delete(ctx)

	// Sleep seconds to wait the share deletion got succeeded
	time.Sleep(45 * time.Second)

	_, err = fsURL.Create(ctx)
	if err != nil {
		fmt.Println(fmt.Fprintf(os.Stdout, "error creating the file system for cleaning, %v", err))
		os.Exit(1)
	}
}

func cleanBfsFile(fileURLStr string) {
	ctx := context.Background()
	u, err := url.Parse(fileURLStr)

	if err != nil {
		fmt.Println("error parsing the file system URL, ", err)
		os.Exit(1)
	}

	fileURL := azbfs.NewFileURL(*u, createBlobFSPipeline(*u))
	_, err = fileURL.Delete(ctx)
	if err != nil {
		fmt.Println(fmt.Sprintf("error deleting the blob FS file, %v", err))
		os.Exit(1)
	}
}

func cleanBlobAccount(resourceURL string) {
	accountURLBase, err := url.Parse(resourceURL)

	if err != nil {
		fmt.Println("error parsing the account sas ", err)
		os.Exit(1)
	}

	p := createBlobPipeline(*accountURLBase)
	accountURL := azblob.NewServiceURL(*accountURLBase, p)

	// perform a list account
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		lResp, err := accountURL.ListContainersSegment(context.Background(), marker, azblob.ListContainersSegmentOptions{})
		if err != nil {
			fmt.Println("error listing containers, please check the container sas, ", err)
			os.Exit(1)
		}

		for _, containerItem := range lResp.ContainerItems {
			_, err := accountURL.NewContainerURL(containerItem.Name).Delete(context.Background(), azblob.ContainerAccessConditions{})
			if err != nil {
				fmt.Println("error deleting the container from account, ", err)
				os.Exit(1)
			}
		}
		marker = lResp.NextMarker
	}
}

func cleanFileAccount(resourceURL string) {
	accountURLBase, err := url.Parse(resourceURL)

	if err != nil {
		fmt.Println("error parsing the account sas ", err)
		os.Exit(1)
	}

	p := createFilePipeline(*accountURLBase)
	accountURL := azfile.NewServiceURL(*accountURLBase, p)

	// perform a list account
	for marker := (azfile.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		lResp, err := accountURL.ListSharesSegment(context.Background(), marker, azfile.ListSharesOptions{})
		if err != nil {
			fmt.Println("error listing shares, please check the share sas, ", err)
			os.Exit(1)
		}

		for _, shareItem := range lResp.ShareItems {
			_, err := accountURL.NewShareURL(shareItem.Name).Delete(context.Background(), azfile.DeleteSnapshotsOptionInclude)
			if err != nil {
				fmt.Println("error deleting the share from account, ", err)
				os.Exit(1)
			}
		}
		marker = lResp.NextMarker
	}
}

func cleanS3Account(resourceURL string) {
	u, err := url.Parse(resourceURL)

	if err != nil {
		fmt.Println("fail to parse the S3 service URL, ", err)
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

	buckets, err := s3Client.ListBuckets()
	if err != nil {
		fmt.Println("error listing S3 service, ", err)
		os.Exit(1)
	}
	for _, bucket := range buckets {
		// Remove all the things in bucket with prefix
		if !strings.HasPrefix(bucket.Name, "s2scopybucket") {
			continue // skip buckets not created by s2s copy testings.
		}

		objectsCh := make(chan string)

		go func() {
			defer close(objectsCh)

			// List all objects from a bucket-name with a matching prefix.
			for object := range s3Client.ListObjectsV2(bucket.Name, "", true, context.Background().Done()) {
				if object.Err != nil {
					fmt.Printf("error listing the objects from bucket %q, %v\n", bucket.Name, err)
					return
				}
				objectsCh <- object.Key
			}
		}()

		// List bucket, and delete all the objects in the bucket
		_ = s3Client.RemoveObjects(bucket.Name, objectsCh)

		// Remove the bucket.
		if err := s3Client.RemoveBucket(bucket.Name); err != nil {
			fmt.Printf("error deleting the bucket %q from account, %v\n", bucket.Name, err)
		}
	}
}

func cleanBfsAccount(resourceURL string) {
	panic("not implemented: not used")
}

func cleanBucket(resourceURL string) {
	panic("not implemented: not used")
}

func cleanObject(resourceURL string) {
	panic("not implemented: not used")
}
