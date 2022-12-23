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
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
)

type FileSystemEntry struct {
	fullPath string
	info     os.FileInfo
}

// represents a file info that we may have failed to obtain
type failableFileInfo interface {
	os.FileInfo
	Error() error
}

type DirReader interface {
	Readdir(dir *os.File, n int) ([]os.FileInfo, error)
	Close()
}

// CrawlLocalDirectory specializes parallel.Crawl to work specifically on a local directory.
// It does not follow symlinks.
// The items in the CrawResult output channel are FileSystemEntry s.
// For a wrapper that makes this look more like filepath.Walk, see parallel.Walk.
func CrawlLocalDirectory(ctx context.Context, root Directory, relBase Directory, parallelism int, reader DirReader, getObjectIndexerMapSize func() int64,
	orderedTqueue OrderedTqueueInterface, isSource bool, isSync bool, maxObjectIndexerSizeInGB uint32) []chan CrawlResult {
	sourceTraverser := isSync && isSource
	return Crawl(ctx,
		root, relBase,
		func(dir Directory, enqueueDir func(Directory), enqueueOutput func(DirectoryEntry, error)) error {
			return enumerateOneFileSystemDirectory(dir, enqueueDir, enqueueOutput, reader, sourceTraverser)
		},
		parallelism, getObjectIndexerMapSize, orderedTqueue, isSource, isSync, maxObjectIndexerSizeInGB)
}

// Walk is similar to filepath.Walk.
// But note the following difference is how WalkFunc is used:
// 1. If fileError passed to walkFunc is not nil, then here the filePath passed to that function will usually be ""
//    (whereas with filepath.Walk it will usually (always?) have a value).
// 2. If the return value of walkFunc function is not nil, enumeration will always stop, not matter what the type of the error.
//    (Unlike filepath.WalkFunc, where returning filePath.SkipDir is handled as a special case).
func Walk(appCtx context.Context, root string, relBase string, parallelism int, parallelStat bool, walkFn filepath.WalkFunc,
	getObjectIndexerMapSize func() int64, orderedTqueue OrderedTqueueInterface, isSource bool, isSync bool, maxObjectIndexerSizeInGB uint32) {
	var ctx context.Context
	var cancel context.CancelFunc
	signalRootError := func(e error) {
		_ = walkFn(root, nil, e)
	}

	// relBase is not empty only in case of follow-symlinks. Where symlink pointing to directory traversed separately and
	// this following code cause wrong entries created in ObjectIndexer Map.
	// Main intention of following code to check if accessing root folder having any issue or not.
	if relBase == "" {
		root, err := filepath.Abs(root)
		if err != nil {
			signalRootError(err)
			return
		}

		// Call walkfunc on the root.  This is necessary for compatibility with filePath.Walk
		// TODO: add at a test that CrawlLocalDirectory does NOT include the root (i.e. add test to define that behaviour)
		r, err := os.Open(root) // for directories, we don't need a special open with FILE_FLAG_BACKUP_SEMANTICS, because directory opening uses FindFirst which doesn't need that flag. https://blog.differentpla.net/blog/2007/05/25/findfirstfile-and-se_backup_name
		if err != nil {
			signalRootError(err)
			return
		}
		rs, err := r.Stat()
		if err != nil {
			signalRootError(err)
			return
		}

		err = walkFn(root, rs, nil)
		if err != nil {
			signalRootError(err)
			return
		}

		_ = r.Close()
	}
	// walk the stuff inside the root
	reader, remainingParallelism := NewDirReader(parallelism, parallelStat)
	defer reader.Close()

	ctx, cancel = context.WithCancel(appCtx)
	//
	// CrawlLocalDirectory() returns an array of channels to allow multiple goroutines to process the queued entries so
	// that the source traverser is not limited by the single goroutine processing all the enumerated entries.
	// We need to use many goroutines as the number of channels returned, each goroutine processing entries added on
	// that channel.
	// Note that all entries for a directory and the special EnqueueToTqueue entry for that directory are added to the
	// *same* channel so a goroutine is guaranteed to see EnqueueToTqueue entry for the directory after seeing all the
	// chidlren of the directory.
	//
	channels := CrawlLocalDirectory(ctx, root, relBase, remainingParallelism, reader, getObjectIndexerMapSize, orderedTqueue, isSource, isSync, maxObjectIndexerSizeInGB)

	errChan := make(chan struct{}, len(channels))
	var wg sync.WaitGroup

	processFunc := func(index int) {
		defer wg.Done()
		for {
			select {
			case crawlResult, ok := <-channels[index]:
				if !ok {
					return
				}

				if crawlResult.EnqueueToTqueue() {
					// Do the sanity check, EnqueueToTqueue should be true in case of sync operation and traverser is source.
					if !isSync || !isSource {
						panic(fmt.Sprintf("Entry set for enqueue to tqueue for invalid operation, isSync[%v], isSource[%v]", isSync, isSource))
					}

					//
					// This is a special CrawlResult which signifies that we need to enqueue the given directory to tqueue for
					// target traverser to process. Tell orderedTqueue so that it can add it in a proper child-after-parent
					// order.
					//
					entry, _ := crawlResult.Item()
					orderedTqueue.MarkProcessed(crawlResult.Idx(), entry)

					continue
				}

				entry, err := crawlResult.Item()
				if err == nil {
					fsEntry := entry.(FileSystemEntry)
					err = walkFn(fsEntry.fullPath, fsEntry.info, nil)
				} else {
					// Our directory scanners can enqueue FileSystemEntry items with potentially full path and fileInfo for failures encountered during enumeration.
					// If the entry is valid we pass those to caller.
					// TODO: Right now we are adding entry to error channel, but not adding storedObject to indexerMap. This is not correct, as it will cause target
					//       traverser to treat this as a deleted file and it will delete from the target. Since we don't know the correct status of the file, the right
					//       thing to do is to just skip the file and added to error copy log.
					if fsEntry, ok := entry.(FileSystemEntry); ok {
						err = walkFn(fsEntry.fullPath, fsEntry.info, err)
					} else {
						err = walkFn("", nil, err) // cannot supply path here, because crawlResult probably doesn't have one, due to the error
					}
				}
				if err != nil {
					fmt.Printf("Traverser failed with error: %v", err)
					errChan <- struct{}{}
					cancel()
				}
			case <-errChan:
				fmt.Printf("Some other thread received error, so coming out.")
				errChan <- struct{}{}
				return
			}
		}
	}

	for i := 0; i < len(channels); i++ {
		wg.Add(1)
		go processFunc(i)
	}
	wg.Wait()

	fmt.Printf("Done processing of local traverser channels, root(%s), relBase(%s)\n", root, relBase)
}

// This dummy GUID used to represent ".", Why need to use this guid ?
// Reason for that "." entry will be cleaned up by various filepath api's.
// And we need this "." entry to store the entry in IndexerMap.
const DotSpecialGUID = "474da3ff-8b02-48fc-843e-58b26f11fdd4"

// enumerateOneFileSystemDirectory is an implementation of EnumerateOneDirFunc specifically for the local file system
func enumerateOneFileSystemDirectory(dir Directory, enqueueDir func(Directory), enqueueOutput func(DirectoryEntry, error), r DirReader, sourceTraverser bool) error {
	dirString := dir.(string)

	d, err := os.Open(dirString) // for directories, we don't need a special open with FILE_FLAG_BACKUP_SEMANTICS, because directory opening uses FindFirst which doesn't need that flag. https://blog.differentpla.net/blog/2007/05/25/findfirstfile-and-se_backup_name
	if err != nil {
		// FileInfo value being nil should mean that the FileSystemEntry refers to a directory.
		enqueueOutput(FileSystemEntry{
			fullPath: dirString,
			info:     nil,
		}, err)

		// Since we have already enqueued the failed enumeration entry, return nil error to avoid duplicate queueing by workerLoop().
		return err
	}
	defer d.Close()

	// enumerate immediate children
	for {
		//
		// TODO :- If directory permission changes midway after part of directory has been enumerated, we will end up in a situation
		//         where some of the directory entries are added to indexerMap and rest are not. The directory itself will not be added
		//         to tqueue. The partially added entries will never be processed (hence removed from indexerMap) sincethere is no tqueue
		//         covering them, this will cause panic as we assert indexerMap to be empty at the end of enumeration.
		//
		// Note :- Directories with fewer elements than what we read in a single Readdir() call will not be affected by above. To reduce the chances
		//         of this issue, read more in one Readdir() call.
		//
		list, err := r.Readdir(d, 10240) // list it in chunks, so that if we get child dirs early, parallel workers can start working on them
		if err == io.EOF {
			if len(list) > 0 {
				panic("unexpected non-empty list")
			}
			break
		} else if err != nil {
			// FileInfo value being nil should mean that the FileSystemEntry refers to a directory.
			enqueueOutput(FileSystemEntry{dirString, nil}, err)

			// Since we have already enqueued the failed enumeration entry, return nil error to avoid duplicate queueing by workerLoop().
			return err
		}
		for _, childInfo := range list {
			childEntry := FileSystemEntry{
				fullPath: filepath.Join(dirString, childInfo.Name()),
				info:     childInfo,
			}

			if failable, ok := childInfo.(failableFileInfo); ok && failable.Error() != nil {
				// while Readdir as a whole did not fail, this particular file info did
				enqueueOutput(childEntry, failable.Error())
				continue
			}
			isSymlink := childInfo.Mode()&os.ModeSymlink != 0 // for compatibility with filepath.Walk, we do not follow symlinks, but we do enqueue them as output
			if childInfo.IsDir() && !isSymlink {
				enqueueDir(childEntry.fullPath)
			}
			enqueueOutput(childEntry, nil)
		}
	}

	//
	// Enqueue the directory itself.
	// We do it for the source traverser case so that it can add the directory to tqueue as soon as it's done
	// enumerating a directory. It helps target traverser get all the objects that it needs - which is all objects
	// in the directory, and the directory itself.
	//
	if sourceTraverser {
		// TODO: Need to handle error from lstat.
		fileInfo, _ := os.Lstat(dirString)
		childEntry := FileSystemEntry{
			fullPath: path.Join(dirString, DotSpecialGUID),
			info:     fileInfo,
		}
		if failable, ok := fileInfo.(failableFileInfo); ok && failable.Error() != nil {
			// while Readdir as a whole did not fail, this particular file info did
			enqueueOutput(childEntry, failable.Error())
		} else {
			enqueueOutput(childEntry, nil)
		}
	}
	return nil
}
