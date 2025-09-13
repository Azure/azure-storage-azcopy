package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	gcpUtils "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	datalakefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	sharefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"google.golang.org/api/iterator"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/JeffreyRichter/enum/enum"
	"github.com/minio/minio-go/v7"
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
func (ServiceType) GCP() ServiceType    { return ServiceType(4) }

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
			case EServiceType.GCP():
				switch resourceType {
				case EResourceType.Bucket():
					cleanBucket(resourceURL)
				case EResourceType.SingleFile():
					cleanObject(resourceURL)
				case EResourceType.Account():
					cleanGCPAccount(resourceURL)
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

func cleanContainer(resourceURL string) {
	containerClient := createContainerClient(resourceURL)

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// Create the container. This will fail if it's already present but this saves us the pain of a container being missing for one reason or another.
	_, _ = containerClient.Create(ctx, nil)

	// perform a list blob
	pager := containerClient.NewListBlobsFlatPager(nil)
	for pager.More() {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		listBlob, err := pager.NextPage(ctx)
		if err != nil {
			fmt.Println("error listing blobs inside the container. Please check the container sas", err)
			os.Exit(1)
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			_, err := containerClient.NewBlobClient(*blobInfo.Name).Delete(ctx, &blob.DeleteOptions{DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude)})
			if err != nil {
				fmt.Println("error deleting the blob from container ", blobInfo.Name)
				os.Exit(1)
			}
		}
	}
}

func cleanBlob(resourceURL string) {
	blobClient := createBlobClient(resourceURL)
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	_, err := blobClient.Delete(ctx, &blob.DeleteOptions{DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude)})
	if err != nil {
		fmt.Println("error deleting the blob ", err)
		os.Exit(1)
	}
}

func cleanShare(resourceURL string) {
	shareClient := createShareClient(resourceURL)
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// Create the share. This will fail if it's already present but this saves us the pain of a container being missing for one reason or another.
	_, _ = shareClient.Create(ctx, nil)

	_, err := shareClient.Delete(ctx, &share.DeleteOptions{DeleteSnapshots: to.Ptr(share.DeleteSnapshotsOptionTypeInclude)})
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode != http.StatusNotFound {
			fmt.Fprintf(os.Stdout, "error deleting the share for clean share, error '%v'\n", err)
			os.Exit(1)
		}
	}

	// Sleep seconds to wait the share deletion got succeeded
	time.Sleep(45 * time.Second)

	_, err = shareClient.Create(ctx, nil)
	if err != nil {
		fmt.Fprintf(os.Stdout, "error creating the share for clean share, error '%v'\n", err)
		os.Exit(1)
	}
}

func cleanFile(resourceURL string) {
	fileClient := createShareFileClient(resourceURL)
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	_, err := fileClient.Delete(ctx, nil)
	if err != nil {
		fmt.Println("error deleting the file ", err)
		os.Exit(1)
	}
}

func createBlobClient(resourceURL string) *blob.Client {
	blobURLParts, err := blob.ParseURL(resourceURL)
	if err != nil {
		fmt.Println("Failed to parse url")
		os.Exit(1)
	}
	containerClient := createContainerClient(resourceURL)
	blobClient := containerClient.NewBlobClient(blobURLParts.BlobName)
	if blobURLParts.Snapshot != "" {
		blobClient, err = blobClient.WithSnapshot(blobURLParts.Snapshot)
		if err != nil {
			fmt.Println("Failed to create snapshot client")
			os.Exit(1)
		}
	}
	if blobURLParts.VersionID != "" {
		blobClient, err = blobClient.WithVersionID(blobURLParts.VersionID)
		if err != nil {
			fmt.Println("Failed to create version id client")
			os.Exit(1)
		}
	}

	return blobClient
}

func createContainerClient(resourceURL string) *container.Client {
	blobURLParts, err := blob.ParseURL(resourceURL)
	if err != nil {
		fmt.Println("Failed to parse url")
		os.Exit(1)
	}
	return createBlobServiceClient(resourceURL).NewContainerClient(blobURLParts.ContainerName)
}

func createBlobServiceClient(resourceURL string) *blobservice.Client {
	blobURLParts, err := blob.ParseURL(resourceURL)
	if err != nil {
		fmt.Println("Failed to parse url")
		os.Exit(1)
	}
	blobURLParts.ContainerName = ""
	blobURLParts.BlobName = ""
	blobURLParts.VersionID = ""
	blobURLParts.Snapshot = ""

	// create the pipeline, preferring SAS over account name/key
	if blobURLParts.SAS.Encode() != "" {
		bsc, err := blobservice.NewClientWithNoCredential(blobURLParts.String(), nil)
		if err != nil {
			fmt.Println("Failed to create blob service client")
			os.Exit(1)
		}
		return bsc
	}

	// Get name and key variables from environment.
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in the environment, and there is no SAS token present
	if (name == "" && key == "") && blobURLParts.SAS.Encode() == "" {
		fmt.Println("ACCOUNT_NAME and ACCOUNT_KEY should be set, or a SAS token should be supplied before cleaning the file system")
		os.Exit(1)
	}
	c, err := blob.NewSharedKeyCredential(name, key)
	if err != nil {
		fmt.Println("Failed to create shared key credential!")
		os.Exit(1)
	}
	bsc, err := blobservice.NewClientWithSharedKeyCredential(blobURLParts.String(), c, nil)
	if err != nil {
		fmt.Println("Failed to create blob service client")
		os.Exit(1)
	}
	return bsc
}

func createShareFileClient(resourceURL string) *sharefile.Client {
	fileURLParts, err := sharefile.ParseURL(resourceURL)
	if err != nil {
		fmt.Println("Failed to parse url")
		os.Exit(1)
	}
	shareClient := createShareClient(resourceURL)
	fileClient := shareClient.NewRootDirectoryClient().NewFileClient(fileURLParts.DirectoryOrFilePath)
	return fileClient
}

//func createShareDirectoryClient(resourceURL string) *sharedirectory.Client {
//	fileURLParts, err := sharefile.ParseURL(resourceURL)
//	if err != nil {
//		fmt.Println("Failed to parse url")
//		os.Exit(1)
//	}
//	shareClient := createShareClient(resourceURL)
//	if fileURLParts.DirectoryOrFilePath == "" {
//		return shareClient.NewRootDirectoryClient()
//	} else {
//		return shareClient.NewDirectoryClient(fileURLParts.DirectoryOrFilePath)
//	}
//}

func createShareClient(resourceURL string) *share.Client {
	fileURLParts, err := sharefile.ParseURL(resourceURL)
	if err != nil {
		fmt.Println("Failed to parse url")
		os.Exit(1)
	}
	sc := createFileServiceClient(resourceURL).NewShareClient(fileURLParts.ShareName)
	if fileURLParts.ShareSnapshot != "" {
		sc, err = sc.WithSnapshot(fileURLParts.ShareSnapshot)
		if err != nil {
			fmt.Println("Failed to parse snapshot")
			os.Exit(1)
		}
	}
	return sc
}

func createFileServiceClient(resourceURL string) *fileservice.Client {
	fileURLParts, err := sharefile.ParseURL(resourceURL)
	if err != nil {
		fmt.Println("Failed to parse url")
		os.Exit(1)
	}
	fileURLParts.ShareName = ""
	fileURLParts.ShareSnapshot = ""
	fileURLParts.DirectoryOrFilePath = ""

	// create the pipeline, preferring SAS over account name/key
	if fileURLParts.SAS.Encode() != "" {
		fsc, err := fileservice.NewClientWithNoCredential(fileURLParts.String(), nil)
		if err != nil {
			fmt.Println("Failed to create blob service client")
			os.Exit(1)
		}
		return fsc
	}

	// Get name and key variables from environment.
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in the environment, and there is no SAS token present
	if (name == "" && key == "") && fileURLParts.SAS.Encode() == "" {
		fmt.Println("ACCOUNT_NAME and ACCOUNT_KEY should be set, or a SAS token should be supplied before cleaning the file system")
		os.Exit(1)
	}
	c, err := sharefile.NewSharedKeyCredential(name, key)
	if err != nil {
		fmt.Println("Failed to create shared key credential!")
		os.Exit(1)
	}
	fsc, err := fileservice.NewClientWithSharedKeyCredential(fileURLParts.String(), c, nil)
	if err != nil {
		fmt.Println("Failed to create blob service client")
		os.Exit(1)
	}
	return fsc
}

func createFileSystemClient(resourceURL string) *filesystem.Client {
	datalakeURLParts, err := azdatalake.ParseURL(resourceURL)
	if err != nil {
		fmt.Println("Failed to parse url")
		os.Exit(1)
	}
	datalakeURLParts.FileSystemName = ""
	datalakeURLParts.PathName = ""

	// create the pipeline, preferring SAS over account name/key
	if datalakeURLParts.SAS.Encode() != "" {
		fsc, err := filesystem.NewClientWithNoCredential(datalakeURLParts.String(), nil)
		if err != nil {
			fmt.Println("Failed to create filesystem client")
			os.Exit(1)
		}
		return fsc
	}

	// Get name and key variables from environment.
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in the environment, and there is no SAS token present
	if (name == "" && key == "") && datalakeURLParts.SAS.Encode() == "" {
		fmt.Println("ACCOUNT_NAME and ACCOUNT_KEY should be set, or a SAS token should be supplied before cleaning the file system")
		os.Exit(1)
	}
	c, err := azdatalake.NewSharedKeyCredential(name, key)
	if err != nil {
		fmt.Println("Failed to create shared key credential!")
		os.Exit(1)
	}

	fsc, err := filesystem.NewClientWithSharedKeyCredential(resourceURL, c, nil)
	if err != nil {
		fmt.Println("Failed to create filesystem client")
		os.Exit(1)
	}
	return fsc
}

func createDatalakeFileClient(resourceURL string) *datalakefile.Client {
	datalakeURLParts, err := azdatalake.ParseURL(resourceURL)
	if err != nil {
		fmt.Println("Failed to parse url")
		os.Exit(1)
	}
	fileSystemClient := createFileSystemClient(resourceURL)
	fileClient := fileSystemClient.NewFileClient(datalakeURLParts.PathName)
	return fileClient
}

func cleanFileSystem(resourceURL string) {
	fsc := createFileSystemClient(resourceURL)
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// Instead of error checking the delete, error check the create.
	// If the filesystem is deleted somehow, this recovers us from CI hell.
	_, err := fsc.Delete(ctx, nil)
	if err != nil {
		fmt.Println(fmt.Fprintf(os.Stdout, "error deleting the file system for cleaning, %v", err))
		// don't fail just log
	}

	// Sleep seconds to wait the share deletion got succeeded
	time.Sleep(45 * time.Second)

	_, err = fsc.Create(ctx, nil)
	if err != nil {
		fmt.Println(fmt.Fprintf(os.Stdout, "error creating the file system for cleaning, %v", err))
		os.Exit(1)
	}
}

func cleanBfsFile(resourceURL string) {
	fc := createDatalakeFileClient(resourceURL)
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	_, err := fc.Delete(ctx, nil)
	if err != nil {
		fmt.Printf("error deleting the blob FS file, %v\n", err)
		os.Exit(1)
	}
}

func cleanBlobAccount(resourceURL string) {
	serviceClient := createBlobServiceClient(resourceURL)
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// perform a list account
	pager := serviceClient.NewListContainersPager(nil)

	for pager.More() {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		lResp, err := pager.NextPage(ctx)
		if err != nil {
			fmt.Println("error listing containers, please check the container sas, ", err)
			os.Exit(1)
		}

		for _, containerItem := range lResp.ContainerItems {
			_, err := serviceClient.NewContainerClient(*containerItem.Name).Delete(ctx, nil)
			if err != nil {
				fmt.Println("error deleting the container from account, ", err)
				os.Exit(1)
			}
		}
	}
}

func cleanFileAccount(resourceURL string) {
	serviceClient := createFileServiceClient(resourceURL)
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// perform a list account
	pager := serviceClient.NewListSharesPager(nil)
	for pager.More() {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		lResp, err := pager.NextPage(ctx)
		if err != nil {
			fmt.Println("error listing shares, please check the share sas, ", err)
			os.Exit(1)
		}

		for _, shareItem := range lResp.Shares {
			_, err := serviceClient.NewShareClient(*shareItem.Name).Delete(ctx, &share.DeleteOptions{DeleteSnapshots: to.Ptr(share.DeleteSnapshotsOptionTypeInclude)})
			if err != nil {
				fmt.Println("error deleting the share from account, ", err)
				os.Exit(1)
			}
		}
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
	ctx := context.Background()
	buckets, err := s3Client.ListBuckets(ctx)
	if err != nil {
		fmt.Println("error listing S3 service, ", err)
		os.Exit(1)
	}
	for _, bucket := range buckets {
		// Remove all the things in bucket with prefix
		if !strings.HasPrefix(bucket.Name, "s2scopybucket") {
			continue // skip buckets not created by s2s copy testings.
		}

		objectsCh := make(chan minio.ObjectInfo)

		go func() {
			defer close(objectsCh)

			// List all objects from a bucket-name with a matching prefix.
			for object := range s3Client.ListObjects(ctx, bucket.Name, minio.ListObjectsOptions{Prefix: "", Recursive: true}) {
				if object.Err != nil {
					fmt.Printf("error listing the objects from bucket %q, %v\n", bucket.Name, err)
					return
				}
				objectsCh <- object
			}
		}()

		// List bucket, and delete all the objects in the bucket
		_ = s3Client.RemoveObjects(ctx, bucket.Name, objectsCh, minio.RemoveObjectsOptions{})

		// Remove the bucket.
		if err := s3Client.RemoveBucket(ctx, bucket.Name); err != nil {
			fmt.Printf("error deleting the bucket %q from account, %v\n", bucket.Name, err)
		}
	}
}

func cleanGCPAccount(resourceURL string) {
	u, err := url.Parse(resourceURL)

	if err != nil {
		fmt.Println("fail to parse the GCP service URL, ", err)
		os.Exit(1)
	}

	_, err = common.NewGCPURLParts(*u)
	if err != nil {
		fmt.Println("new GCP URL parts, ", err)
		os.Exit(1)
	}

	gcpClient, _ := createGCPClientWithGCSSDK()
	it := gcpClient.Buckets(context.Background(), os.Getenv("GOOGLE_CLOUD_PROJECT"))
	for {
		battrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err == nil {
			if !strings.HasPrefix(battrs.Name, "s2scopybucket") {
				continue // skip buckets not created by s2s copy testings.
			}

			objectsCh := make(chan string)

			go func() {
				defer close(objectsCh)

				// List all objects from a bucket-name with a matching prefix.
				itObj := gcpClient.Bucket(battrs.Name).Objects(context.Background(), nil)
				for {
					attrs, err := itObj.Next()
					if err == iterator.Done {
						break
					}
					if err == nil {
						objectsCh <- attrs.Name
					} else {
						fmt.Printf("error listing the objects from bucket %q, %v\n", battrs.Name, err)
						return

					}
				}
			}()

			deleteGCPBucket(gcpClient, battrs.Name)
		}
	}
}

func deleteGCPBucket(client *gcpUtils.Client, bucketName string) {
	bucket := client.Bucket(bucketName)
	ctx := context.Background()
	it := bucket.Objects(ctx, &gcpUtils.Query{Prefix: ""})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err == nil {
			err = bucket.Object(attrs.Name).Delete(context.TODO())
			if err != nil {
				fmt.Println("Could not clear GCS Buckets.")
				return
			}
		}
	}
	err := bucket.Delete(context.Background())
	if err != nil {
		fmt.Printf("Failed to Delete GCS Bucket %v", bucketName)
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
