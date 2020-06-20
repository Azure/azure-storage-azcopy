package common

import (
	"errors"
	"fmt"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"net/url"
	"regexp"
	"strings"
)

type DropboxURLParts struct {
	Scheme     string
	Host       string
	ObjectKey  string
	IsDir      bool
	BucketName string //Workaround for absence of Buckets in Dropbox, this name is derived from destination name
}

const dropboxHostPattern = "^(?P<bucketName>.+).dropbox.com$"
const invalidDropboxURLErrorMessage = "Invalid Dropbox URL. Eg URLs: https://www.dropbox.com/path/to/file or https://www.dropbox.com/path/to/dir/"
const dropboxEssentialHostPart = "dropbox.com"

var dropboxHostRegex = regexp.MustCompile(dropboxHostPattern)

func IsDropboxURL(u url.URL) bool {
	if _, isDropboxURL := findDropboxURLMatches(strings.ToLower(u.Host)); isDropboxURL {
		return true
	}
	return false
}

func findDropboxURLMatches(host string) ([]string, bool) {
	matchSlices := dropboxHostRegex.FindStringSubmatch(host)
	if matchSlices == nil || !strings.Contains(host, dropboxEssentialHostPart) {
		return nil, false
	}
	return matchSlices, true
}

func NewDropboxURLParts(u url.URL) (DropboxURLParts, error) {
	host := strings.ToLower(u.Host)
	matchSlices, isDropboxURL := findDropboxURLMatches(host)
	if !isDropboxURL {
		return DropboxURLParts{}, errors.New(invalidDropboxURLErrorMessage)
	}

	path := u.Path
	up := DropboxURLParts{
		Scheme:     u.Scheme,
		Host:       host,
		BucketName: matchSlices[1],
	}

	if path != "" && path[0] == '/' {
		path = path[1:]
	}
	up.ObjectKey = path

	dbx, err := CreateDropboxClient()
	if err != nil {
		return DropboxURLParts{}, err
	}
	if u.Path != "/" {
		metadata, err := dbx.GetMetadata(files.NewGetMetadataArg("/" + up.ObjectKey))
		if err != nil {
			return DropboxURLParts{}, err
		}
		switch metadata.(type) {
		case *files.FileMetadata:
			up.IsDir = false
		case *files.FolderMetadata:
			up.IsDir = true
		default:
			return DropboxURLParts{}, fmt.Errorf("Invalid file/folder type for specified Dropbox URL")
		}
	} else {
		up.IsDir = true
	}

	return up, nil
}

func (p *DropboxURLParts) URL() url.URL {
	u := url.URL{
		Scheme: p.Scheme,
		Host:   p.Host,
		Path:   p.ObjectKey,
	}
	return u
}

func (p *DropboxURLParts) String() string {
	u := p.URL()
	return u.String()
}
