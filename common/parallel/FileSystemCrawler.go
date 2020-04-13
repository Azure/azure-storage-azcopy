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

package parallel

import (
	"context"
	"io"
	"os"
	"path/filepath"
)

// FileOpener is a pluggable implementation of file open
type FileOpener interface {
	Open(name string) (*os.File, error)
}

type DefaultFileOpener struct{}

func (DefaultFileOpener) Open(name string) (*os.File, error) {
	return os.Open(name)
}

type FileSystemEntry struct {
	fullPath string
	info     os.FileInfo
}

// CrawlLocalDirectory specializes parallel.Crawl to work specifically on a local directory.
// It does not follow symlinks.
// The items in the CrawResult output channel are FileSystemEntry s.
// For a wrapper that makes this look more like filepath.Walk, see parallel.Walk.
func CrawlLocalDirectory(ctx context.Context, root string, opener FileOpener, parallelism int) <-chan CrawlResult {
	return Crawl(ctx,
		root,
		func(dir Directory, enqueueDir func(Directory), enqueueOutput func(DirectoryEntry)) error {
			return enumerateOneFileSystemDirectory(opener, dir, enqueueDir, enqueueOutput)
		},
		parallelism,
	)
}

// Walk is similar to filepath.Walk.
// But note the following difference is how WalkFunc is used:
// 1. If fileError passed to walkFunc is not nil, then here the filePath passed to that function will be ""
//    (whereas with filepath.Walk it will/may have a value)
// 2. If the return value of walkFunc function is not nil, enumeration will always stop, not matter what the type of the error.
//    (Unlike filepath.WalkFunc, where returning filePath.SkipDir is handled as a special case)
func Walk(root string, opener FileOpener, parallelism int, walkFn filepath.WalkFunc) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}

	// Call walkfunc on the root.  This is necessary for compatibility with filePath.Walk
	// TODO: add at a test that CrawlLocalDirectory does NOT include the root (i.e. add test to define that behaviour)
	r, err := opener.Open(root)
	if err != nil {
		return err
	}
	rs, err := r.Stat()
	if err != nil {
		return err
	}
	err = walkFn(root, rs, nil)
	if err != nil {
		return err
	}

	// walk the stuff inside the root
	ctx, cancel := context.WithCancel(context.Background())
	ch := CrawlLocalDirectory(ctx, root, opener, parallelism)
	for crawlResult := range ch {
		entry, err := crawlResult.Item()
		if err == nil {
			fsEntry := entry.(FileSystemEntry)
			err = walkFn(fsEntry.fullPath, fsEntry.info, nil)
		} else {
			err = walkFn("", nil, err)
		}
		if err != nil {
			cancel()
			return err
		}
	}

	return nil
}

// enumerateOneFileSystemDirectory is an implementation of EnumerateOneDirFunc specifically for the local file system
func enumerateOneFileSystemDirectory(opener FileOpener, dir Directory, enqueueDir func(Directory), enqueueOutput func(DirectoryEntry)) error {
	dirString := dir.(string)

	d, err := opener.Open(dirString)
	if err != nil {
		return err
	}
	defer d.Close()

	// enumerate immediate children
	for {
		list, err := d.Readdir(1024) // list it in chunks, so that if we get child dirs early, parallel workers can start working on them
		if err == io.EOF {
			if len(list) > 0 {
				panic("unexpected non-empty list")
			}
			return nil
		} else if err != nil {
			return err
		}
		for _, childInfo := range list {
			childEntry := FileSystemEntry{
				fullPath: filepath.Join(dirString, childInfo.Name()),
				info:     childInfo,
			}
			if childInfo.IsDir() {
				enqueueDir(childEntry.fullPath)
			}
			enqueueOutput(childEntry)
		}
	}
}
