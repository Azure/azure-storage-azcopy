// Copyright © 2026 Microsoft <azcopydev@microsoft.com>
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
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func hardlinkNamespace(root string, local, destination bool) string {
	kind := "remote"
	if local {
		kind = "local"
		if absolute, err := filepath.Abs(root); err == nil {
			root = filepath.Clean(absolute)
		}
	} else if parsed, err := url.Parse(root); err == nil {
		snapshot := parsed.Query().Get("sharesnapshot")
		parsed.User, parsed.RawQuery, parsed.Fragment = nil, "", ""
		parsed.Host = strings.ToLower(parsed.Host)
		parsed.Path = strings.TrimSuffix(parsed.Path, "/")
		root = parsed.String() + "\x00" + snapshot
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%t\x00%s\x00%s", destination, kind, root))))
}

func registerHardlink(store *common.InodeStore, namespace, fileID, relativePath string) (NFSMetadataContext, error) {
	if store == nil {
		return NFSMetadataContext{}, fmt.Errorf("inode store is not initialized; cannot preserve hardlinks")
	}
	if fileID == "" {
		return NFSMetadataContext{}, fmt.Errorf("file identity is unavailable; cannot preserve hardlinks")
	}
	key := namespace + ":" + fileID
	return NFSMetadataContext{Inode: key, FileID: fileID, inodePath: &relativePath}, nil
}

// Register only objects accepted by the filters, so an excluded path cannot
// become the anchor of a selected hardlink group.
func withHardlinkRegistration(store *common.InodeStore, inner ObjectProcessor) ObjectProcessor {
	return func(object StoredObject) error {
		if object.inodePath != nil {
			if store == nil {
				return fmt.Errorf("inode store is not initialized; cannot preserve hardlinks")
			}
			target, exists, err := store.GetOrAdd(object.Inode, *object.inodePath)
			if err != nil {
				return err
			}
			if exists && target != *object.inodePath {
				object.TargetHardlinkFile = target
			}
			if object.hardlinkedSymlink && object.TargetHardlinkFile != "" {
				object.EntityType = common.EEntityType.Hardlink()
			}
		}
		return inner(object)
	}
}

func hardlinkAwareProcessor(store *common.InodeStore, filters []ObjectFilter, inner ObjectProcessor,
	counter enumerationCounterFunc, symlinks common.SymlinkHandlingType, hardlinks common.HardlinkHandlingType) ObjectProcessor {
	count := func(object StoredObject) {
		if counter != nil {
			counter(object.EntityType, symlinks, hardlinks)
		}
	}
	accepted := withHardlinkRegistration(store, func(object StoredObject) error {
		count(object)
		return inner(object)
	})
	return func(object StoredObject) error {
		passed, err := passedFilters(filters, object)
		if err != nil || !passed {
			count(object)
			if err != nil {
				return err
			}
			return ErrIgnored
		}
		return accepted(object)
	}
}

func (t *localTraverser) hardlinkMetadata(fullPath string, info os.FileInfo) (NFSMetadataContext, error) {
	relative, err := filepath.Rel(t.basePath, fullPath)
	if err != nil {
		return NFSMetadataContext{}, err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return NFSMetadataContext{}, fmt.Errorf("hardlink path is outside the traversal root")
	}
	if relative == "." {
		relative = ""
	}
	metadata, err := registerHardlink(t.inodeStore, t.inodeNamespace, getInodeString(info), filepath.ToSlash(relative))
	metadata.hardlinkedSymlink = IsSymbolicLink(info)
	return metadata, err
}

func (t *fileTraverser) hardlinkMetadata(rawURL, fileID string) (NFSMetadataContext, error) {
	root, err := url.Parse(t.basePath)
	if err != nil {
		return NFSMetadataContext{}, err
	}
	target, err := url.Parse(rawURL)
	if err != nil {
		return NFSMetadataContext{}, err
	}
	if !strings.EqualFold(root.Host, target.Host) || root.Scheme != target.Scheme {
		return NFSMetadataContext{}, fmt.Errorf("hardlink path is outside the traversal root")
	}
	rootPath := strings.TrimSuffix(root.Path, "/")
	relative := ""
	if target.Path != rootPath {
		prefix := rootPath + "/"
		if !strings.HasPrefix(target.Path, prefix) {
			return NFSMetadataContext{}, fmt.Errorf("hardlink path is outside the traversal root")
		}
		relative = strings.TrimPrefix(target.Path, prefix)
	}
	return registerHardlink(t.inodeStore, t.inodeNamespace, fileID, relative)
}
