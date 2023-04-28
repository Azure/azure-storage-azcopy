package main

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/google/uuid"
	"net/url"
	"os"
)

func main() {
	acctKey, acctName := os.Getenv("ACCOUNT_KEY"), os.Getenv("ACCOUNT_NAME")
	key := azbfs.NewSharedKeyCredential(acctName, acctKey)
	p := azbfs.NewPipeline(key, azbfs.PipelineOptions{})
	serviceURL := azbfs.NewServiceURL(url.URL{
		Scheme: "https",
		Host: fmt.Sprintf("%s.dfs.core.windows.net", acctName),
	}, p)

	fsURL := serviceURL.NewFileSystemURL(uuid.NewString())
	defer fsURL.Delete(context.Background())

	_, err := fsURL.Create(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}

	fURL := fsURL.NewRootDirectoryURL().NewFileURL("asdf.txt")

	_, err = fURL.Create(context.Background(), azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = fURL.SetAccessControl(context.Background(), azbfs.BlobFSAccessControl{
		Owner:       "1234",
		Group:       "5213456",
		Permissions: "r-xrw-r--",
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	ctl, err := fURL.GetAccessControl(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(ctl.Owner, ctl.Group, ctl.Permissions)
}
