// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package traverser

import (
	"context"
	"fmt"

	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"

	"strings"
)

// Enumerates an entire blob account, looking into each matching container as it goes
type blobAccountTraverser struct {
	opts InitResourceTraverserOptions

	serviceClient    *blobservice.Client
	ctx              context.Context
	containerPattern string
	cachedContainers []string

	blobOpts []BlobTraverserOptions

	excludeContainerName []ObjectFilter
}

func (t *blobAccountTraverser) IsDirectory(isSource bool) (bool, error) {
	return true, nil // Returns true as account traversal is inherently folder-oriented and recursive.
}

func (t *blobAccountTraverser) ListContainers() ([]string, error) {
	cachedContainers, _, err := t.getListContainers()
	return cachedContainers, err
}

func (t *blobAccountTraverser) getListContainers() ([]string, []string, error) {
	var skippedContainers []string
	// a nil list also returns 0
	if len(t.cachedContainers) == 0 || len(t.excludeContainerName) > 0 {
		cList := make([]string, 0)
		pager := t.serviceClient.NewListContainersPager(nil)
		for pager.More() {
			resp, err := pager.NextPage(t.ctx)
			if err != nil {
				return nil, nil, err
			}
			for _, v := range resp.ContainerItems {
				// a nil list also returns 0
				if len(t.cachedContainers) == 0 {
					// Match a pattern for the container name and the container name only.
					if t.containerPattern != "" {
						if ok, err := containerNameMatchesPattern(*v.Name, t.containerPattern); err != nil {
							// Break if the pattern is invalid
							return nil, nil, err
						} else if !ok {
							// Ignore the container if it doesn't match the pattern.
							continue
						}
					}
				}

				// get a list of containers that are not excluded
				if len(t.excludeContainerName) > 0 {
					so := StoredObject{ContainerName: *v.Name}
					for _, f := range t.excludeContainerName {
						if !f.DoesPass(so) {
							// Ignore the container if the container name should be excluded
							skippedContainers = append(skippedContainers, *v.Name)
							continue
						} else {
							cList = append(cList, *v.Name)
						}
					}
				} else {
					cList = append(cList, *v.Name)
				}
			}
		}
		t.cachedContainers = cList
	}

	return t.cachedContainers, skippedContainers, nil
}

func (t *blobAccountTraverser) Traverse(preprocessor objectMorpher, processor ObjectProcessor, filters []ObjectFilter) error {
	// ListContainers will return the cached container list if containers have already been listed by this traverser.
	cList, skippedContainers, err := t.getListContainers()
	if len(skippedContainers) > 0 {
		common.GetLifecycleMgr().Info("Skipped container(s): " + strings.Join(skippedContainers, ", "))
	}

	if err != nil {
		return err
	}

	for _, v := range cList {
		containerURL := t.serviceClient.NewContainerClient(v).URL()
		containerTraverser := NewBlobTraverser(containerURL, t.serviceClient, t.ctx, InitResourceTraverserOptions{
			IncrementEnumeration: t.opts.IncrementEnumeration,

			CpkOptions: t.opts.CpkOptions,

			Recursive:             true,
			IncludeDirectoryStubs: t.opts.IncludeDirectoryStubs,
			PreserveBlobTags:      t.opts.PreserveBlobTags,
			PreservePermissions:   t.opts.PreservePermissions,
		}, t.blobOpts...)

		preprocessorForThisChild := preprocessor.FollowedBy(newContainerDecorator(v))

		err = containerTraverser.Traverse(preprocessorForThisChild, processor, filters)

		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("failed to list blobs in container %s: %s", v, err))
			continue
		}
	}

	return nil
}

func NewBlobAccountTraverser(serviceClient *blobservice.Client, container string, ctx context.Context, opts InitResourceTraverserOptions, blobOpts ...BlobTraverserOptions) (t *blobAccountTraverser) {
	t = &blobAccountTraverser{
		opts: opts,

		ctx:                  ctx,
		serviceClient:        serviceClient,
		containerPattern:     container,
		excludeContainerName: buildExcludeContainerFilter(opts.ExcludeContainers),

		blobOpts: blobOpts,
	}

	return
}
