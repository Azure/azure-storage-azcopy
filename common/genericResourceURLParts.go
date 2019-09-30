package common

import (
	"fmt"
	"net/url"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/azbfs"
)

// GenericResourceURLParts is intended to be a generic solution to code duplication when using *URLParts
// TODO: Use this to reduce code dupe in the cca.Source and jobPartOrder.Source setups
// Currently this just contains generic functions for what we *need*. This isn't an overarching, perfect implementation.
// The above suggestion would be preferable to continuing to expand this (due to 4x code dupe for every function)-- it's just a bridge over a LARGE gap for now.
type GenericResourceURLParts struct {
	location     Location // underlying location selects which URLParts we're using
	blobURLParts azblob.BlobURLParts
	fileURLParts azfile.FileURLParts
	bfsURLParts  azbfs.BfsURLParts
	s3URLParts   S3URLParts
}

func NewGenericResourceURLParts(resourceURL url.URL, location Location) GenericResourceURLParts {
	g := GenericResourceURLParts{location: location}

	switch location {
	case ELocation.Blob():
		g.blobURLParts = azblob.NewBlobURLParts(resourceURL)
	case ELocation.File():
		g.fileURLParts = azfile.NewFileURLParts(resourceURL)
	case ELocation.BlobFS():
		g.bfsURLParts = azbfs.NewBfsURLParts(resourceURL)
	case ELocation.S3():
		var err error
		g.s3URLParts, err = NewS3URLParts(resourceURL)
		PanicIfErr(err)

	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}

	return g
}

func (g GenericResourceURLParts) GetContainerName() string {
	switch g.location {
	case ELocation.Blob():
		return g.blobURLParts.ContainerName
	case ELocation.File():
		return g.fileURLParts.ShareName
	case ELocation.BlobFS():
		return g.bfsURLParts.FileSystemName
	case ELocation.S3():
		return g.s3URLParts.BucketName

	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}
}

func (g GenericResourceURLParts) GetObjectName() string {
	switch g.location {
	case ELocation.Blob():
		return g.blobURLParts.BlobName
	case ELocation.File():
		return g.fileURLParts.DirectoryOrFilePath
	case ELocation.BlobFS():
		return g.bfsURLParts.DirectoryOrFilePath
	case ELocation.S3():
		return g.s3URLParts.ObjectKey

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

	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}
}

func (g GenericResourceURLParts) String() string {
	var URLOut url.URL

	switch g.location {
	case ELocation.S3():
		return g.s3URLParts.String()

	case ELocation.Blob():
		URLOut = g.blobURLParts.URL()
	case ELocation.File():
		URLOut = g.fileURLParts.URL()
	case ELocation.BlobFS():
		URLOut = g.bfsURLParts.URL()

	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}

	return URLOut.String()
}

func (g GenericResourceURLParts) URL() url.URL {
	switch g.location {
	case ELocation.Blob():
		return g.blobURLParts.URL()
	case ELocation.File():
		return g.fileURLParts.URL()
	case ELocation.BlobFS():
		return g.bfsURLParts.URL()
	case ELocation.S3():
		return g.s3URLParts.URL()
	default:
		panic(fmt.Sprintf("%s is an invalid location for GenericResourceURLParts", g.location))
	}
}
