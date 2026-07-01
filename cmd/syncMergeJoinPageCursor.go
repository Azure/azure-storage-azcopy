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
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/minio/minio-go/v7"

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
	// lwt returns the SMB last-write-time of the i-th item (Azure Files); zero when unavailable.
	lwt(i int) time.Time
	// ct returns the SMB change-time of the i-th item (Azure Files); zero when unavailable.
	ct(i int) time.Time
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
	lwt        time.Time         // SMB last-write-time, UTC-normalized; set only for Azure Files entries, else zero
	ct         time.Time         // SMB change-time, UTC-normalized; set only for Azure Files entries, else zero
	size       int64             // content length; 0 for prefixes
	entityType common.EntityType // File/Folder/Symlink, computed once at page build
	isPrefix   bool              // true for virtual directory prefix entries
	// blobType is the single scalar from Properties we DO retain. The full SDK
	// Properties struct is still dropped; only this small enum is kept so that a
	// re-transferred page/append blob preserves its type (S2S with
	// GET_PROPERTIES_IN_BACKEND=false relies on the StoredObject's blobType).
	// Empty for prefixes/folders.
	blobType blob.BlobType
}

// newBlobFileEntry extracts only the scalar fields needed by the merge-join from a
// listing BlobItem. The heavy SDK BlobItem (Properties + Metadata + parsed time.Time)
// is intentionally not retained, so per-item heap stays tiny during large enumerations.
func newBlobFileEntry(item *container.BlobItem, searchPrefix string) blobPageEntry {
	itemName := common.IffNotNil(item.Name, "")
	var lmt time.Time
	var size int64
	var blobType blob.BlobType
	if item.Properties != nil {
		if item.Properties.LastModified != nil {
			// UTC() drops the per-blob fixedZone retained by the RFC1123 parse.
			lmt = item.Properties.LastModified.UTC()
		}
		if item.Properties.ContentLength != nil {
			size = *item.Properties.ContentLength
		}
		if item.Properties.BlobType != nil {
			blobType = *item.Properties.BlobType
		}
	}
	return blobPageEntry{
		relPath:    strings.TrimPrefix(itemName, searchPrefix),
		name:       getObjectNameOnly(itemName),
		lmt:        lmt,
		size:       size,
		entityType: getEntityType(item.Metadata), // metadata consumed here, not retained
		isPrefix:   false,
		blobType:   blobType,
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
	// isHNS marks the listing as coming from a hierarchical-namespace (BlobFS)
	// account. On HNS every BlobPrefix corresponds to a REAL directory entity
	// (not a virtual prefix), so it must be counted and transferred as a folder.
	isHNS bool
}

// newBlobRawPage merges BlobItems and BlobPrefixes into a single sorted slice
// using a two-pointer merge (both sub-lists are already sorted by the storage service).
//
// HNS dedup: on a hierarchical-namespace (BlobFS) account a non-empty directory
// "dir/foo" appears BOTH as a folder-marker BlobItem ("dir/foo", hdi_isfolder
// metadata) AND as a BlobPrefix ("dir/foo/"). Emitting both would produce two
// merge-join entries with the same relative path, double-counting and double-
// recursing the directory. We therefore drop the BlobPrefix whenever a folder-
// marker BlobItem with the matching name exists in the same page, keeping the
// folder-marker item (isPrefix=false) which both schedules the folder (for ACLs/
// metadata) AND drives recursion. Flat-namespace (FNS) accounts have no folder-
// marker blobs, so this map stays empty and behavior is unchanged.
func newBlobRawPage(resp container.ListBlobsHierarchyResponse, searchPrefix string, isHNS bool) *blobRawPage {
	items := resp.Segment.BlobItems
	prefixes := resp.Segment.BlobPrefixes

	// Pre-scan items for folder-marker blobs (HNS). Only build the dedup set when
	// at least one folder marker is present, so FNS pages pay no allocation cost.
	var folderMarkerNames map[string]struct{}
	for _, it := range items {
		if getEntityType(it.Metadata) == common.EEntityType.Folder() {
			if folderMarkerNames == nil {
				folderMarkerNames = make(map[string]struct{})
			}
			folderMarkerNames[common.IffNotNil(it.Name, "")] = struct{}{}
		}
	}

	// isShadowedPrefix returns true when a BlobPrefix is shadowed by a matching
	// folder-marker BlobItem and should be dropped from the merged page.
	isShadowedPrefix := func(pfxName string) bool {
		if folderMarkerNames == nil {
			return false
		}
		_, ok := folderMarkerNames[strings.TrimSuffix(pfxName, common.AZCOPY_PATH_SEPARATOR_STRING)]
		return ok
	}

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
			if !isShadowedPrefix(pfxName) {
				merged = append(merged, newBlobPrefixEntry(pfxName, searchPrefix))
			}
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
		if !isShadowedPrefix(pfxName) {
			merged = append(merged, newBlobPrefixEntry(pfxName, searchPrefix))
		}
	}

	return &blobRawPage{entries: merged, searchPrefix: searchPrefix, isHNS: isHNS}
}

func (p *blobRawPage) len() int { return len(p.entries) }

func (p *blobRawPage) name(i int) string { return p.entries[i].relPath }

func (p *blobRawPage) lmt(i int) time.Time { return p.entries[i].lmt }

func (p *blobRawPage) lwt(i int) time.Time { return p.entries[i].lwt }

func (p *blobRawPage) ct(i int) time.Time { return p.entries[i].ct }

func (p *blobRawPage) size(i int) int64 { return p.entries[i].size }

func (p *blobRawPage) entityType(i int) common.EntityType { return p.entries[i].entityType }

func (p *blobRawPage) isVirtualPrefix(i int) bool {
	// On HNS, a BlobPrefix is a real directory entity, not a virtual prefix.
	if p.isHNS {
		return false
	}
	return p.entries[i].isPrefix
}

func (p *blobRawPage) toStoredObject(i int, containerName string) StoredObject {
	e := p.entries[i]
	// Built from extracted scalars only — the heavy SDK BlobItem (Properties/Metadata)
	// is intentionally not retained. Valid for S2S copy with PRESERVE_PROPERTIES=false.
	// We DO surface the retained blob type so a re-transferred page/append blob keeps
	// its type (otherwise S2S defaults it to block blob and data integrity fails).
	var blobProps blobPropsProvider = noBlobProps
	if e.blobType != "" {
		blobProps = blobTypeOnlyAdapter{blobType: e.blobType}
	}
	so := newStoredObject(
		nil,
		e.name,
		e.relPath,
		e.entityType,
		e.lmt,
		e.size,
		noBlobProps,
		blobProps,
		nil,
		containerName,
	)
	// Surface the SMB last-write-time and change-time (Azure Files only; zero for
	// blob/S3) so the scheduled transfer carries the right SMB timestamps and the
	// merge-join comparison matches the indexMap orchestrator comparator.
	if !e.lwt.IsZero() || !e.ct.IsZero() {
		so.updateTimestamps(e.lwt, e.ct)
	}
	return so
}

// blobTypeOnlyAdapter is a minimal blobPropsProvider that returns a retained blob
// type and zero/empty values for everything else (via the embedded
// emptyPropertiesAdapter). Used by the page-level merge-join so a re-transferred
// blob preserves its type without retaining the full SDK Properties struct.
type blobTypeOnlyAdapter struct {
	emptyPropertiesAdapter
	blobType blob.BlobType
}

func (a blobTypeOnlyAdapter) BlobType() blob.BlobType { return a.blobType }

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

func (c *pageCursor) name() string                      { return c.page.name(c.idx) }
func (c *pageCursor) lmt() time.Time                    { return c.page.lmt(c.idx) }
func (c *pageCursor) lwt() time.Time                    { return c.page.lwt(c.idx) }
func (c *pageCursor) ct() time.Time                     { return c.page.ct(c.idx) }
func (c *pageCursor) itemSize() int64                   { return c.page.size(c.idx) }
func (c *pageCursor) itemEntityType() common.EntityType { return c.page.entityType(c.idx) }
func (c *pageCursor) itemIsVirtualPrefix() bool         { return c.page.isVirtualPrefix(c.idx) }

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
	isHNS bool,
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

			page := newBlobRawPage(resp, searchPrefix, isHNS)
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

// ─── S3 page producer ────────────────────────────────────────────────────────
// The S3 producer mirrors the blob producer: it lists a single directory level
// using delimiter "/" via minio's low-level Core.ListObjectsV2, which returns the
// object keys (Contents) and the virtual directory prefixes (CommonPrefixes) as
// two separate, individually-sorted lists. We two-pointer merge them into a single
// sorted rawPage so the merge-join's lexicographic ordering invariant holds.
//
// S3 has no folder entities (only objects + virtual prefixes), so every directory
// entry is an isVirtualPrefix Folder (recurse only, no transfer scheduled) — there
// are no HNS-style folder-marker objects to dedup. Zero-byte placeholder objects
// (key == searchPrefix, or key ending in "/") are skipped, matching the s3Traverser.

// newS3FileEntry extracts the scalar fields from an S3 object. Returns ok=false for
// placeholder/self objects that must be skipped (empty relPath or folder-style key).
func newS3FileEntry(o minio.ObjectInfo, searchPrefix string) (blobPageEntry, bool) {
	key := o.Key
	if key == searchPrefix || strings.HasSuffix(key, common.AZCOPY_PATH_SEPARATOR_STRING) {
		return blobPageEntry{}, false
	}
	return blobPageEntry{
		relPath:    strings.TrimPrefix(key, searchPrefix),
		name:       getObjectNameOnly(key),
		lmt:        o.LastModified.UTC(),
		size:       o.Size,
		entityType: common.EEntityType.File(),
		isPrefix:   false,
	}, true
}

// newS3PrefixEntry builds an entry for an S3 CommonPrefix (virtual directory).
func newS3PrefixEntry(pfx, searchPrefix string) blobPageEntry {
	trimmedPfx := strings.TrimSuffix(pfx, common.AZCOPY_PATH_SEPARATOR_STRING)
	return blobPageEntry{
		relPath:    strings.TrimPrefix(trimmedPfx, searchPrefix),
		name:       getObjectNameOnly(trimmedPfx),
		entityType: common.EEntityType.Folder(),
		isPrefix:   true,
	}
}

// newS3RawPage merges S3 Contents (objects) and CommonPrefixes (directories) into a
// single sorted slice using a two-pointer merge. Both sub-lists are already sorted
// by key by the storage service. Reuses blobRawPage as a source-agnostic container.
func newS3RawPage(result minio.ListBucketV2Result, searchPrefix string) *blobRawPage {
	contents := result.Contents
	prefixes := result.CommonPrefixes

	merged := make([]blobPageEntry, 0, len(contents)+len(prefixes))
	i, j := 0, 0

	for i < len(contents) && j < len(prefixes) {
		ck := contents[i].Key
		pk := prefixes[j].Prefix

		// Compare full names for sorted merge. Prefix keys have trailing "/".
		if ck <= pk {
			if e, ok := newS3FileEntry(contents[i], searchPrefix); ok {
				merged = append(merged, e)
			}
			i++
		} else {
			merged = append(merged, newS3PrefixEntry(pk, searchPrefix))
			j++
		}
	}

	for ; i < len(contents); i++ {
		if e, ok := newS3FileEntry(contents[i], searchPrefix); ok {
			merged = append(merged, e)
		}
	}

	for ; j < len(prefixes); j++ {
		merged = append(merged, newS3PrefixEntry(prefixes[j].Prefix, searchPrefix))
	}

	return &blobRawPage{entries: merged, searchPrefix: searchPrefix}
}

// startS3Prefetch starts a goroutine that fetches pages from S3 via Core.ListObjectsV2
// (delimiter "/") and sends them on a buffered channel (buffer=1). Objects and virtual
// directory prefixes are merged into sorted rawPage entries.
func startS3Prefetch(
	ctx context.Context,
	core *minio.Core,
	bucketName string,
	searchPrefix string,
) (<-chan rawPage, <-chan error) {
	pageCh := make(chan rawPage, 1) // buffer=1 to reduce memory (no deep prefetch)
	errCh := make(chan error, 1)

	go func() {
		defer close(pageCh)
		defer close(errCh)

		const s3MaxKeys = 1000
		continuationToken := ""

		for {
			result, err := core.ListObjectsV2(bucketName, searchPrefix, "", continuationToken, common.AZCOPY_PATH_SEPARATOR_STRING, s3MaxKeys)
			if err != nil {
				errCh <- fmt.Errorf("s3 listing failed for prefix '%s': %w", searchPrefix, err)
				return
			}

			page := newS3RawPage(result, searchPrefix)
			select {
			case pageCh <- page:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}

			if !result.IsTruncated {
				return
			}
			continuationToken = result.NextContinuationToken
		}
	}()

	return pageCh, errCh
}

// createS3CoreAndBucket resolves an S3 resource URL into a low-level minio Core
// client and the bucket name. Reuses the shared S3 client manager so credentials
// and connection pools are shared with the traversers.
func createS3CoreAndBucket(ctx context.Context, resourceURL string, opts *InitResourceTraverserOptions) (*minio.Core, string, error) {
	u, err := url.Parse(resourceURL)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse S3 URL '%s': %w", resourceURL, err)
	}

	s3URLParts, err := common.NewS3URLParts(*u)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse S3 URL parts: %w", err)
	}

	if opts.Credential == nil {
		return nil, "", fmt.Errorf("missing credential for S3 source")
	}

	client, err := GetS3TraverserGlobalClientManager().GetS3Client(ctx, s3URLParts, *opts.Credential)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get S3 client: %w", err)
	}

	return &minio.Core{Client: client}, s3URLParts.BucketName, nil
}

// s3SearchPrefix extracts the object-key prefix (with trailing slash) from an S3 URL.
func s3SearchPrefix(resourceURL string) (string, error) {
	u, err := url.Parse(resourceURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse S3 URL '%s': %w", resourceURL, err)
	}
	s3URLParts, err := common.NewS3URLParts(*u)
	if err != nil {
		return "", fmt.Errorf("failed to parse S3 URL parts: %w", err)
	}
	prefix := s3URLParts.ObjectKey
	if prefix != "" && !strings.HasSuffix(prefix, common.AZCOPY_PATH_SEPARATOR_STRING) {
		prefix += common.AZCOPY_PATH_SEPARATOR_STRING
	}
	return prefix, nil
}

// ─── Azure Files page producer ───────────────────────────────────────────────
// The File producer mirrors the S3 producer: Azure Files' ListFilesAndDirectories
// returns Files and Directories as two separate, individually-sorted lists, which
// we two-pointer merge into a single sorted rawPage so the merge-join's
// lexicographic ordering invariant holds.
//
// Unlike a blob FNS virtual prefix, an Azure Files directory is a REAL entity that
// must be created (with its SMB timestamps) at the destination AND recursed into.
// We therefore build the page with isHNS=true so every directory entry is treated
// as a real folder (scheduled + recursed), exactly like an HNS BlobPrefix.
//
// Listing is per-directory-client (not prefix-based like blob), so each level's
// directory client is derived from the cached share client + the search prefix.
// The listed names are already leaf names relative to that directory, so relPath
// equals the leaf name directly (no prefix stripping needed).

// newFileEntry extracts the scalar fields needed by the merge-join from a listed
// Azure Files file. LMT comes from the listing (Include.Timestamps=true), so no
// per-file GetProperties round-trip is needed for the match case.
func newFileEntry(f *directory.File) blobPageEntry {
	name := common.IffNotNil(f.Name, "")
	var lmt time.Time
	var lwt time.Time
	var ct time.Time
	var size int64
	if f.Properties != nil {
		if f.Properties.LastModified != nil {
			lmt = f.Properties.LastModified.UTC()
		}
		if f.Properties.LastWriteTime != nil {
			lwt = f.Properties.LastWriteTime.UTC()
		}
		if f.Properties.ChangeTime != nil {
			ct = f.Properties.ChangeTime.UTC()
		}
		if f.Properties.ContentLength != nil {
			size = *f.Properties.ContentLength
		}
	}
	return blobPageEntry{
		relPath:    name,
		name:       name,
		lmt:        lmt,
		lwt:        lwt,
		ct:         ct,
		size:       size,
		entityType: common.EEntityType.File(),
		isPrefix:   false,
	}
}

// newFileDirEntry builds an entry for a listed Azure Files directory. The directory
// is a real entity (folder), and its LMT is taken from the listing so the merge-join
// can decide transfer vs NoTransferNeeded without a GetProperties round-trip.
func newFileDirEntry(d *directory.Directory) blobPageEntry {
	name := common.IffNotNil(d.Name, "")
	var lmt time.Time
	var lwt time.Time
	var ct time.Time
	if d.Properties != nil {
		if d.Properties.LastModified != nil {
			lmt = d.Properties.LastModified.UTC()
		}
		if d.Properties.LastWriteTime != nil {
			lwt = d.Properties.LastWriteTime.UTC()
		}
		if d.Properties.ChangeTime != nil {
			ct = d.Properties.ChangeTime.UTC()
		}
	}
	return blobPageEntry{
		relPath:    name,
		name:       name,
		lmt:        lmt,
		lwt:        lwt,
		ct:         ct,
		entityType: common.EEntityType.Folder(),
		isPrefix:   false,
	}
}

// newFileRawPage merges Azure Files Files (files) and Directories (folders) into a
// single sorted slice using a two-pointer merge. Both sub-lists are already sorted
// by name by the storage service. A directory is compared as "name/" so the inner
// ordering matches the merge-join's buildChildPath (folders carry a trailing slash).
// Reuses blobRawPage as a source-agnostic container with isHNS=true so directories
// are treated as real folder entities (scheduled + recursed), not virtual prefixes.
func newFileRawPage(resp directory.ListFilesAndDirectoriesResponse, searchPrefix string) *blobRawPage {
	if resp.Segment == nil {
		return &blobRawPage{entries: nil, searchPrefix: searchPrefix, isHNS: true}
	}
	files := resp.Segment.Files
	dirs := resp.Segment.Directories

	merged := make([]blobPageEntry, 0, len(files)+len(dirs))
	i, j := 0, 0

	for i < len(files) && j < len(dirs) {
		fk := common.IffNotNil(files[i].Name, "")
		// Compare a directory as "name/" so the inner ordering matches the
		// merge-join's buildChildPath (folders carry a trailing slash).
		dk := common.IffNotNil(dirs[j].Name, "") + common.AZCOPY_PATH_SEPARATOR_STRING
		if fk <= dk {
			merged = append(merged, newFileEntry(files[i]))
			i++
		} else {
			merged = append(merged, newFileDirEntry(dirs[j]))
			j++
		}
	}

	for ; i < len(files); i++ {
		merged = append(merged, newFileEntry(files[i]))
	}

	for ; j < len(dirs); j++ {
		merged = append(merged, newFileDirEntry(dirs[j]))
	}

	return &blobRawPage{entries: merged, searchPrefix: searchPrefix, isHNS: true}
}

// startFileSharePrefetch starts a goroutine that lists a single Azure Files directory
// level via NewListFilesAndDirectoriesPager and sends merged, sorted pages on a
// buffered channel (buffer=1). The directory client for this level is derived from
// the cached share client and the search prefix. Include.Timestamps=true surfaces the
// per-item LMT directly in the listing so the match case needs no GetProperties call.
func startFileSharePrefetch(
	ctx context.Context,
	shareClient *share.Client,
	searchPrefix string,
) (<-chan rawPage, <-chan error) {
	pageCh := make(chan rawPage, 1) // buffer=1 to reduce memory (no deep prefetch)
	errCh := make(chan error, 1)

	go func() {
		defer close(pageCh)
		defer close(errCh)

		// Derive the directory client for this level from the cached share client.
		dirPath := strings.TrimSuffix(searchPrefix, common.AZCOPY_PATH_SEPARATOR_STRING)
		var dirClient *directory.Client
		if dirPath == "" {
			dirClient = shareClient.NewRootDirectoryClient()
		} else {
			dirClient = shareClient.NewDirectoryClient(dirPath)
		}

		includeExtendedInfo := true
		pager := dirClient.NewListFilesAndDirectoriesPager(&directory.ListFilesAndDirectoriesOptions{
			Include:             directory.ListFilesInclude{Timestamps: true},
			IncludeExtendedInfo: &includeExtendedInfo,
		})

		for pager.More() {
			resp, err := pager.NextPage(ctx)
			if err != nil {
				errCh <- fmt.Errorf("azure files listing failed for prefix '%s': %w", searchPrefix, err)
				return
			}

			page := newFileRawPage(resp, searchPrefix)
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

// parseFileURL extracts the share name and directory/file path from a full Azure
// Files URL string.
func parseFileURL(rawURL string) (shareName string, dirOrFilePath string, err error) {
	fileURLParts, err := file.ParseURL(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse file URL '%s': %w", rawURL, err)
	}
	return fileURLParts.ShareName, fileURLParts.DirectoryOrFilePath, nil
}

// createFileServiceClient creates an Azure Files service client from a raw resource
// URL and traverser options. Mirrors createBlobServiceClient.
func createFileServiceClient(resourceURL string, opts *InitResourceTraverserOptions) (*fileservice.Client, error) {
	fileURLParts, err := file.ParseURL(resourceURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse file URL '%s': %w", resourceURL, err)
	}

	// Strip share / path to obtain the service-level URL.
	fileURLParts.ShareName = ""
	fileURLParts.ShareSnapshot = ""
	fileURLParts.DirectoryOrFilePath = ""

	res, err := SplitResourceString(fileURLParts.String(), common.ELocation.File())
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
	fileOptions := &common.FileClientOptions{
		AllowTrailingDot: opts.TrailingDotOption.IsEnabled(),
	}

	c, err := common.GetServiceClientForLocation(
		common.ELocation.File(),
		res,
		opts.Credential.CredentialType,
		opts.Credential.OAuthTokenInfo.TokenCredential,
		&clientOptions,
		fileOptions,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create file service client: %w", err)
	}

	fsc, err := c.FileServiceClient()
	if err != nil {
		return nil, fmt.Errorf("failed to get file service client: %w", err)
	}

	return fsc, nil
}

// getShareClientAndPrefix resolves a full Azure Files URL into a share client and the
// directory search prefix for listing. The prefix includes a trailing "/" if non-empty.
func getShareClientAndPrefix(resourceURL string, opts *InitResourceTraverserOptions) (*share.Client, string, string, error) {
	shareName, dirOrFilePath, err := parseFileURL(resourceURL)
	if err != nil {
		return nil, "", "", err
	}

	fsc, err := createFileServiceClient(resourceURL, opts)
	if err != nil {
		return nil, "", "", err
	}

	shareClient := fsc.NewShareClient(shareName)

	// Ensure prefix has trailing slash for directory listing (matching traverser behavior)
	searchPrefix := dirOrFilePath
	if searchPrefix != "" && !strings.HasSuffix(searchPrefix, common.AZCOPY_PATH_SEPARATOR_STRING) {
		searchPrefix += common.AZCOPY_PATH_SEPARATOR_STRING
	}

	return shareClient, shareName, searchPrefix, nil
}
