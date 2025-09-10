package common

import (
	"fmt"
	"net/url"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
)

// GenericResourceURLParts is intended to be a generic solution to code duplication when using *URLParts
// TODO: Use this to reduce code dupe in the cca.Source and jobPartOrder.Source setups
// Currently this just contains generic functions for what we *need*. This isn't an overarching, perfect implementation.
// The above suggestion would be preferable to continuing to expand this (due to 4x code dupe for every function)-- it's just a bridge over a LARGE gap for now.
type GenericResourceURLParts struct {
	location     Location // underlying location selects which URLParts we're using
	blobURLParts azblob.URLParts
	fileURLParts file.URLParts
	bfsURLParts  azdatalake.URLParts
	s3URLParts   S3URLParts
	gcpURLParts  GCPURLParts
}

func NewGenericResourceURLParts(resourceURL url.URL, location Location) GenericResourceURLParts {
	g := GenericResourceURLParts{location: location}
	var err error

	switch location {
	case ELocation.Blob():
		g.blobURLParts, err = azblob.ParseURL(resourceURL.String())
	case ELocation.FileSMB(), ELocation.FileNFS():
		g.fileURLParts, err = file.ParseURL(resourceURL.String())
	case ELocation.BlobFS():
		g.bfsURLParts, err = azdatalake.ParseURL(resourceURL.String())
	case ELocation.S3():
		g.s3URLParts, err = NewS3URLParts(resourceURL)
	case ELocation.GCP():
		g.gcpURLParts, err = NewGCPURLParts(resourceURL)
	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}
	PanicIfErr(err)
	return g
}

func (g *GenericResourceURLParts) GetContainerName() string {
	switch g.location {
	case ELocation.Blob():
		return g.blobURLParts.ContainerName
	case ELocation.FileSMB(), ELocation.FileNFS():
		return g.fileURLParts.ShareName
	case ELocation.BlobFS():
		return g.bfsURLParts.FileSystemName
	case ELocation.S3():
		return g.s3URLParts.BucketName
	case ELocation.GCP():
		return g.gcpURLParts.BucketName
	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}
}

func (g *GenericResourceURLParts) GetObjectName() string {
	switch g.location {
	case ELocation.Blob():
		return g.blobURLParts.BlobName
	case ELocation.FileSMB(), ELocation.FileNFS():
		return g.fileURLParts.DirectoryOrFilePath
	case ELocation.BlobFS():
		return g.bfsURLParts.PathName
	case ELocation.S3():
		return g.s3URLParts.ObjectKey
	case ELocation.GCP():
		return g.gcpURLParts.ObjectKey
	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}
}

func (g *GenericResourceURLParts) SetObjectName(objectName string) {
	switch g.location {
	case ELocation.Blob():
		g.blobURLParts.BlobName = objectName
	case ELocation.FileSMB(), ELocation.FileNFS():
		g.fileURLParts.DirectoryOrFilePath = objectName
	case ELocation.BlobFS():
		g.bfsURLParts.PathName = objectName
	case ELocation.S3():
		g.s3URLParts.ObjectKey = objectName
	case ELocation.GCP():
		g.gcpURLParts.ObjectKey = objectName
	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}
}

func (g *GenericResourceURLParts) String() string {
	switch g.location {
	case ELocation.S3():
		return g.s3URLParts.String()
	case ELocation.GCP():
		return g.gcpURLParts.String()
	case ELocation.Blob():
		return g.blobURLParts.String()
	case ELocation.FileSMB(), ELocation.FileNFS():
		return g.fileURLParts.String()
	case ELocation.BlobFS():
		return g.bfsURLParts.String()

	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}
}

func (g *GenericResourceURLParts) URL() url.URL {
	switch g.location {
	case ELocation.Blob():
		u := g.blobURLParts.String()
		parsedURL, err := url.Parse(u)
		PanicIfErr(err)
		return *parsedURL
	case ELocation.FileSMB(), ELocation.FileNFS():
		u := g.fileURLParts.String()
		parsedURL, err := url.Parse(u)
		PanicIfErr(err)
		return *parsedURL
	case ELocation.BlobFS():
		u := g.bfsURLParts.String()
		parsedURL, err := url.Parse(u)
		PanicIfErr(err)
		return *parsedURL
	case ELocation.S3():
		return g.s3URLParts.URL()
	case ELocation.GCP():
		return g.gcpURLParts.URL()
	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}
}
