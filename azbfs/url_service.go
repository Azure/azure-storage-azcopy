package azbfs

import (
	"context"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

// A ServiceURL represents a URL to the Azure Storage File service allowing you to manipulate file shares.
type ServiceURL struct {
	client filesystemClient
}

// NewServiceURL creates a ServiceURL object using the specified URL and request policy pipeline.
func NewServiceURL(url url.URL, p pipeline.Pipeline) ServiceURL {
	if p == nil {
		panic("p can't be nil")
	}
	client := filesystemClient{newManagementClient(url, p)}
	return ServiceURL{client: client}
}

func (s ServiceURL) ListFilesystemsSegment(ctx context.Context, marker *string) (*FilesystemList, error) {
	return s.client.List(ctx, nil, marker, nil, nil, nil, nil)
}

// URL returns the URL endpoint used by the ServiceURL object.
func (s ServiceURL) URL() url.URL {
	return s.client.URL()
}

// String returns the URL as a string.
func (s ServiceURL) String() string {
	u := s.URL()
	return u.String()
}

// WithPipeline creates a new ServiceURL object identical to the source but with the specified request policy pipeline.
func (s ServiceURL) WithPipeline(p pipeline.Pipeline) ServiceURL {
	return NewServiceURL(s.URL(), p)
}

// NewFileSystemURL creates a new ShareURL object by concatenating shareName to the end of
// ServiceURL's URL. The new ShareURL uses the same request policy pipeline as the ServiceURL.
// To change the pipeline, create the ShareURL and then call its WithPipeline method passing in the
// desired pipeline object. Or, call this package's NewFileSystemURL instead of calling this object's
// NewFileSystemURL method.
func (s ServiceURL) NewFileSystemURL(fileSystemName string) FileSystemURL {
	fileSystemURL := appendToURLPath(s.URL(), fileSystemName)
	return NewFileSystemURL(fileSystemURL, s.client.Pipeline())
}

// appendToURLPath appends a string to the end of a URL's path (prefixing the string with a '/' if required)
func appendToURLPath(u url.URL, name string) url.URL {
	// e.g. "https://ms.com/a/b/?k1=v1&k2=v2#f"
	// When you call url.Parse() this is what you'll get:
	//     Scheme: "https"
	//     Opaque: ""
	//       User: nil
	//       Host: "ms.com"
	//       Path: "/a/b/"	This should start with a / and it might or might not have a trailing slash
	//    RawPath: ""
	// ForceQuery: false
	//   RawQuery: "k1=v1&k2=v2"
	//   Fragment: "f"
	if len(u.Path) == 0 || u.Path[len(u.Path)-1] != '/' {
		u.Path += "/" // Append "/" to end before appending name
	}
	u.Path += name
	return u
}
