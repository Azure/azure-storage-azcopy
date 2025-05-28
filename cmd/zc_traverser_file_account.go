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

package cmd

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
)

// Enumerates an entire files account, looking into each matching share as it goes
type fileAccountTraverser struct {
	opts InitResourceTraverserOptions

	serviceClient *service.Client
	ctx           context.Context
	sharePattern  string
	cachedShares  []string
}

func (t *fileAccountTraverser) IsDirectory(isSource bool) (bool, error) {
	return true, nil // Returns true as account traversal is inherently folder-oriented and recursive.
}

func (t *fileAccountTraverser) listContainers() ([]string, error) {
	if len(t.cachedShares) == 0 {
		shareList := make([]string, 0)

		pager := t.serviceClient.NewListSharesPager(nil)

		for pager.More() {
			resp, err := pager.NextPage(t.ctx)
			if err != nil {
				return nil, err
			}

			for _, v := range resp.Shares {
				// Match a pattern for the share name and the share name only
				if t.sharePattern != "" {
					if ok, err := containerNameMatchesPattern(*v.Name, t.sharePattern); err != nil {
						// Break if the pattern is invalid
						return nil, err
					} else if !ok {
						// Ignore the share if it doesn't match the pattern.
						continue
					}
				}

				shareList = append(shareList, *v.Name)
			}
		}

		t.cachedShares = shareList
		return shareList, nil
	} else {
		return t.cachedShares, nil
	}
}

func (t *fileAccountTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {
	// listContainers will return the cached share list if shares have already been listed by this traverser.
	shareList, err := t.listContainers()

	if err != nil {
		return err
	}

	for _, v := range shareList {
		shareURL := t.serviceClient.NewShareClient(v).URL()
		shareTraverser := newFileTraverser(shareURL, t.serviceClient, t.ctx, InitResourceTraverserOptions{
			DestResourceType:        t.opts.DestResourceType,
			Recursive:               true,
			GetPropertiesInFrontend: t.opts.GetPropertiesInFrontend,
			IncrementEnumeration:    t.opts.IncrementEnumeration,
			TrailingDotOption:       t.opts.TrailingDotOption,
		})

		preprocessorForThisChild := preprocessor.FollowedBy(newContainerDecorator(v))

		err = shareTraverser.Traverse(preprocessorForThisChild, processor, filters)

		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("failed to list files in share %s: %s", v, err))
			continue
		}
	}

	return nil
}

func newFileAccountTraverser(serviceClient *service.Client, shareName string, ctx context.Context, opts InitResourceTraverserOptions) (t *fileAccountTraverser) {
	t = &fileAccountTraverser{
		opts: opts,

		ctx:           ctx,
		serviceClient: serviceClient,
		sharePattern:  shareName,
	}
	return
}
