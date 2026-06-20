//go:build smslidingwindow
// +build smslidingwindow

// Copyright © 2017 Microsoft <wastore@microsoft.com>
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
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// ─── rawPage interface ───────────────────────────────────────────────────────
// rawPage is a source-agnostic abstraction over a page of listing results.
// Implementations wrap the native SDK response and present a merged, sorted
// view of items (files + virtual directory prefixes).
type rawPage interface {
	// len returns the number of items in this page (files + prefixes merged).
	len() int
	// name returns the relative path of the i-th item, with the search prefix stripped.
	name(i int) string
	// lmt returns the last modified time of the i-th item.
	lmt(i int) time.Time
	// size returns the content length of the i-th item.
	size(i int) int64
	// entityType returns File or Folder for the i-th item.
	entityType(i int) common.EntityType
	// isVirtualPrefix returns true if the i-th item is a virtual directory prefix.
	isVirtualPrefix(i int) bool
	// toStoredObject creates a full StoredObject from the i-th item.
	// This is only called for the <1% of items that actually need a transfer.
	toStoredObject(i int, containerName string) StoredObject
}

// ─── blobPageEntry ───────────────────────────────────────────────────────────
// A single entry in the merged (items + prefixes) sorted view of a blob listing page.
//
// Memory optimization: we extract ONLY the scalar fields the merge-join needs
// (relPath, name, lmt, size, entityType, isPrefix) at page-build time and DELIBERATELY
// do NOT retain the heavy SDK *container.BlobItem (which pins Properties — including
// parsed time.Time/fixedZone — and the per-blob Metadata map). At 100M+ items this
// retention was the dominant heap consumer and drove the GC into a death-spiral.
type blobPageEntry struct {
	relPath    string            // relative path (search prefix stripped)
	name       string            // object name only (final path segment)
	lmt        time.Time         // last modified, UTC-normalized; zero for prefixes
	size       int64             // content length; 0 for prefixes
	entityType common.EntityType // File/Folder/Symlink, computed once at page build
	isPrefix   bool              // true for virtual directory prefix entries
}

// newBlobFileEntry extracts only the scalar fields needed by the merge-join from a
// listing BlobItem. The heavy SDK BlobItem (Properties + Metadata + parsed time.Time)
// is intentionally not retained, so per-item heap stays tiny during large enumerations.
func newBlobFileEntry(item *container.BlobItem, searchPrefix string) blobPageEntry {
	itemName := common.IffNotNil(item.Name, "")
	var lmt time.Time
	var size int64
	if item.Properties != nil {
		if item.Properties.LastModified != nil {
			// UTC() drops the per-blob fixedZone retained by the RFC1123 parse.
			lmt = item.Properties.LastModified.UTC()
		}
		if item.Properties.ContentLength != nil {
			size = *item.Properties.ContentLength
		}
	}
	return blobPageEntry{
		relPath:    strings.TrimPrefix(itemName, searchPrefix),
		name:       getObjectNameOnly(itemName),
		lmt:        lmt,
		size:       size,
		entityType: getEntityType(item.Metadata), // metadata consumed here, not retained
		isPrefix:   false,
	}
}

// newBlobPrefixEntry builds an entry for a virtual directory prefix.
func newBlobPrefixEntry(pfxName, searchPrefix string) blobPageEntry {
	trimmedPfx := strings.TrimSuffix(pfxName, common.AZCOPY_PATH_SEPARATOR_STRING)
	return blobPageEntry{
		relPath:    strings.TrimPrefix(trimmedPfx, searchPrefix),
		name:       getObjectNameOnly(trimmedPfx),
		entityType: common.EEntityType.Folder(),
		isPrefix:   true,
	}
}

// ─── blobRawPage ─────────────────────────────────────────────────────────────
// blobRawPage merges BlobItems and BlobPrefixes from a hierarchy listing response
// into a single sorted slice, enabling correct merge-join ordering.
type blobRawPage struct {
	entries      []blobPageEntry
	searchPrefix string
}

// newBlobRawPage merges BlobItems and BlobPrefixes into a single sorted slice
// using a two-pointer merge (both sub-lists are already sorted by the storage service).
func newBlobRawPage(resp container.ListBlobsHierarchyResponse, searchPrefix string) *blobRawPage {
	items := resp.Segment.BlobItems
	prefixes := resp.Segment.BlobPrefixes

	merged := make([]blobPageEntry, 0, len(items)+len(prefixes))
	i, j := 0, 0

	for i < len(items) && j < len(prefixes) {
		itemName := common.IffNotNil(items[i].Name, "")
		pfxName := common.IffNotNil(prefixes[j].Name, "")

		// Compare full names for sorted merge. Prefix names have trailing "/".
		if itemName <= pfxName {
			merged = append(merged, newBlobFileEntry(items[i], searchPrefix))
			i++
		} else {
			merged = append(merged, newBlobPrefixEntry(pfxName, searchPrefix))
			j++
		}
	}

	// Drain remaining items
	for ; i < len(items); i++ {
		merged = append(merged, newBlobFileEntry(items[i], searchPrefix))
	}

	// Drain remaining prefixes
	for ; j < len(prefixes); j++ {
		pfxName := common.IffNotNil(prefixes[j].Name, "")
		merged = append(merged, newBlobPrefixEntry(pfxName, searchPrefix))
	}

	return &blobRawPage{entries: merged, searchPrefix: searchPrefix}
}

func (p *blobRawPage) len() int { return len(p.entries) }

func (p *blobRawPage) name(i int) string { return p.entries[i].relPath }

func (p *blobRawPage) lmt(i int) time.Time { return p.entries[i].lmt }

func (p *blobRawPage) size(i int) int64 { return p.entries[i].size }

func (p *blobRawPage) entityType(i int) common.EntityType { return p.entries[i].entityType }

func (p *blobRawPage) isVirtualPrefix(i int) bool { return p.entries[i].isPrefix }

func (p *blobRawPage) toStoredObject(i int, containerName string) StoredObject {
	e := p.entries[i]
	// Built from extracted scalars only — the heavy SDK BlobItem (Properties/Metadata)
	// is intentionally not retained. Valid for S2S copy with PRESERVE_PROPERTIES=false.
	return newStoredObject(
		nil,
		e.name,
		e.relPath,
		e.entityType,
		e.lmt,
		e.size,
		noBlobProps,
		noBlobProps,
		nil,
		containerName,
	)
}

// ─── pageCursor ──────────────────────────────────────────────────────────────
// pageCursor provides pull-based iteration over pages from a prefetch channel.
// It reads fields directly from the raw SDK page items — zero allocation per item
// for the match case.
type pageCursor struct {
	pageCh <-chan rawPage // prefetch channel (buffer=1)
	errCh  <-chan error   // error from prefetch goroutine
	page   rawPage        // current page being iterated
	idx    int            // position within current page
	done   bool           // true when all pages have been consumed
}

// newPageCursor creates a cursor from a prefetch channel.
// Blocks until the first page arrives.
func newPageCursor(pageCh <-chan rawPage, errCh <-chan error) *pageCursor {
	c := &pageCursor{
		pageCh: pageCh,
		errCh:  errCh,
	}
	// Pull first page
	page, ok := <-pageCh
	if !ok {
		c.done = true
		return c
	}
	c.page = page
	c.idx = 0
	// Skip empty first page
	if page.len() == 0 {
		c.advancePage()
	}
	return c
}

// newEmptyCursor creates a cursor that is immediately done (for empty destination).
func newEmptyCursor() *pageCursor {
	return &pageCursor{done: true}
}

func (c *pageCursor) name() string              { return c.page.name(c.idx) }
func (c *pageCursor) lmt() time.Time            { return c.page.lmt(c.idx) }
func (c *pageCursor) itemSize() int64            { return c.page.size(c.idx) }
func (c *pageCursor) itemEntityType() common.EntityType { return c.page.entityType(c.idx) }
func (c *pageCursor) itemIsVirtualPrefix() bool  { return c.page.isVirtualPrefix(c.idx) }

func (c *pageCursor) toStoredObject(containerName string) StoredObject {
	return c.page.toStoredObject(c.idx, containerName)
}

// advance moves to the next item within the current page or pulls the next page.
func (c *pageCursor) advance() {
	if c.done {
		return
	}
	c.idx++
	if c.idx < c.page.len() {
		return
	}
	c.advancePage()
}

// advancePage pulls the next page from the prefetch channel.
func (c *pageCursor) advancePage() {
	for {
		page, ok := <-c.pageCh
		if !ok {
			c.done = true
			return
		}
		if page.len() > 0 {
			c.page = page
			c.idx = 0
			return
		}
		// Empty page — keep pulling
	}
}

// checkError returns any error from the prefetch goroutine.
// Should be called after the cursor is done.
func (c *pageCursor) checkError() error {
	if c.errCh == nil {
		return nil
	}
	select {
	case err := <-c.errCh:
		return err
	default:
		return nil
	}
}

// ─── Blob prefetch goroutine ─────────────────────────────────────────────────

// startBlobHierarchyPrefetch starts a goroutine that fetches pages from the
// blob hierarchy pager and sends them on a buffered channel (buffer=1).
// Both BlobItems and BlobPrefixes are merged into sorted rawPage entries.
func startBlobHierarchyPrefetch(
	ctx context.Context,
	containerClient *container.Client,
	searchPrefix string,
	includeTags bool,
) (<-chan rawPage, <-chan error) {
	pageCh := make(chan rawPage, 1) // buffer=1 to reduce memory (no deep prefetch)
	errCh := make(chan error, 1)

	go func() {
		defer close(pageCh)
		defer close(errCh)

		pager := containerClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
			Prefix:  &searchPrefix,
			Include: container.ListBlobsInclude{Metadata: true, Tags: includeTags},
		})

		for pager.More() {
			resp, err := pager.NextPage(ctx)
			if err != nil {
				errCh <- fmt.Errorf("blob hierarchy listing failed for prefix '%s': %w", searchPrefix, err)
				return
			}

			page := newBlobRawPage(resp, searchPrefix)
			select {
			case pageCh <- page:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return pageCh, errCh
}

// ─── Service client helpers ──────────────────────────────────────────────────

// parseBlobURL extracts service URL parts from a full blob URL string.
// Returns the container name, blob name (prefix), and the service-level URL parts.
func parseBlobURL(rawURL string) (containerName string, blobName string, serviceURL string, err error) {
	blobURLParts, err := blob.ParseURL(rawURL)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to parse blob URL '%s': %w", rawURL, err)
	}

	containerName = blobURLParts.ContainerName
	blobName = blobURLParts.BlobName

	// Strip container/blob to get service-level URL
	blobURLParts.ContainerName = ""
	blobURLParts.BlobName = ""
	blobURLParts.Snapshot = ""
	blobURLParts.VersionID = ""

	serviceURL = blobURLParts.String()
	return containerName, blobName, serviceURL, nil
}

// createBlobServiceClient creates a blob service client from a raw resource URL and traverser options.
func createBlobServiceClient(resourceURL string, opts *InitResourceTraverserOptions) (*service.Client, error) {
	_, _, serviceURL, err := parseBlobURL(resourceURL)
	if err != nil {
		return nil, err
	}

	res, err := SplitResourceString(serviceURL, common.ELocation.Blob())
	if err != nil {
		return nil, fmt.Errorf("failed to split resource string: %w", err)
	}

	var reauthTok *common.ScopedAuthenticator
	if opts.Credential != nil {
		if at, ok := opts.Credential.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok {
			reauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
		}
	}
	clientOptions := createClientOptions(azcopyScanningLogger, nil, reauthTok)

	c, err := common.GetServiceClientForLocation(
		common.ELocation.Blob(),
		res,
		opts.Credential.CredentialType,
		opts.Credential.OAuthTokenInfo.TokenCredential,
		&clientOptions,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create service client: %w", err)
	}

	bsc, err := c.BlobServiceClient()
	if err != nil {
		return nil, fmt.Errorf("failed to get blob service client: %w", err)
	}

	return bsc, nil
}

// getContainerClientAndPrefix resolves a full blob URL into a container client
// and the search prefix for listing. The prefix includes a trailing "/" if non-empty.
func getContainerClientAndPrefix(resourceURL string, opts *InitResourceTraverserOptions) (*container.Client, string, string, error) {
	containerName, blobName, _, err := parseBlobURL(resourceURL)
	if err != nil {
		return nil, "", "", err
	}

	bsc, err := createBlobServiceClient(resourceURL, opts)
	if err != nil {
		return nil, "", "", err
	}

	containerClient := bsc.NewContainerClient(containerName)

	// Ensure prefix has trailing slash for directory listing (matching traverser behavior)
	searchPrefix := blobName
	if searchPrefix != "" && !strings.HasSuffix(searchPrefix, common.AZCOPY_PATH_SEPARATOR_STRING) {
		searchPrefix += common.AZCOPY_PATH_SEPARATOR_STRING
	}

	return containerClient, containerName, searchPrefix, nil
}
