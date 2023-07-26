package common

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	sharefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"net/url"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
)

// GenericResourceURLParts is intended to be a generic solution to code duplication when using *URLParts
// TODO: Use this to reduce code dupe in the cca.Source and jobPartOrder.Source setups
// Currently this just contains generic functions for what we *need*. This isn't an overarching, perfect implementation.
// The above suggestion would be preferable to continuing to expand this (due to 4x code dupe for every function)-- it's just a bridge over a LARGE gap for now.
type GenericResourceURLParts struct {
	location     Location // underlying location selects which URLParts we're using
	blobURLParts blob.URLParts
	fileURLParts sharefile.URLParts
	bfsURLParts  azbfs.BfsURLParts
	s3URLParts   S3URLParts
	gcpURLParts  GCPURLParts
}

func NewGenericResourceURLParts(resourceURL url.URL, location Location) GenericResourceURLParts {
	g := GenericResourceURLParts{location: location}
	var err error

	switch location {
	case ELocation.Blob():
		g.blobURLParts, err = blob.ParseURL(resourceURL.String())
		PanicIfErr(err)
	case ELocation.File():
		g.fileURLParts, err = sharefile.ParseURL(resourceURL.String())
		PanicIfErr(err)
	case ELocation.BlobFS():
		g.bfsURLParts = azbfs.NewBfsURLParts(resourceURL)
	case ELocation.S3():
		g.s3URLParts, err = NewS3URLParts(resourceURL)
		PanicIfErr(err)
	case ELocation.GCP():
		g.gcpURLParts, err = NewGCPURLParts(resourceURL)
		PanicIfErr(err)
	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}

	return g
}

func (g *GenericResourceURLParts) GetContainerName() string {
	switch g.location {
	case ELocation.Blob():
		return g.blobURLParts.ContainerName
	case ELocation.File():
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
	case ELocation.File():
		return g.fileURLParts.DirectoryOrFilePath
	case ELocation.BlobFS():
		return g.bfsURLParts.DirectoryOrFilePath
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
	case ELocation.File():
		g.fileURLParts.DirectoryOrFilePath = objectName
	case ELocation.BlobFS():
		g.bfsURLParts.DirectoryOrFilePath = objectName
	case ELocation.S3():
		g.s3URLParts.ObjectKey = objectName
	case ELocation.GCP():
		g.gcpURLParts.ObjectKey = objectName
	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}
}

func (g *GenericResourceURLParts) String() string {
	var URLOut url.URL

	switch g.location {
	case ELocation.S3():
		return g.s3URLParts.String()
	case ELocation.GCP():
		return g.gcpURLParts.String()
	case ELocation.Blob():
		return g.blobURLParts.String()
	case ELocation.File():
		return g.fileURLParts.String()
	case ELocation.BlobFS():
		URLOut = g.bfsURLParts.URL()

	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}

	return URLOut.String()
}

func (g *GenericResourceURLParts) URL() url.URL {
	switch g.location {
	case ELocation.Blob():
		u := g.blobURLParts.String()
		parsedURL, err := url.Parse(u)
		PanicIfErr(err)
		return *parsedURL
	case ELocation.File():
		u := g.fileURLParts.String()
		parsedURL, err := url.Parse(u)
		PanicIfErr(err)
		return *parsedURL
	case ELocation.BlobFS():
		return g.bfsURLParts.URL()
	case ELocation.S3():
		return g.s3URLParts.URL()
	case ELocation.GCP():
		return g.gcpURLParts.URL()
	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}
}
