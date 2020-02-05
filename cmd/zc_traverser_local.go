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
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
)

type localTraverser struct {
	fullPath       string
	recursive      bool
	followSymlinks bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc
}

func (t *localTraverser) isDirectory(bool) bool {
	if strings.HasSuffix(t.fullPath, "/") {
		return true
	}

	props, err := os.Stat(t.fullPath)

	if err != nil {
		return false
	}

	return props.IsDir()
}

func (t *localTraverser) getInfoIfSingleFile() (os.FileInfo, bool, error) {
	fileInfo, err := os.Stat(t.fullPath)

	if err != nil {
		return nil, false, err
	}

	if fileInfo.IsDir() {
		return nil, false, nil
	}

	return fileInfo, true, nil
}

// WalkWithSymlinks is a symlinks-aware version of filePath.Walk.
// Separate this from the traverser for two purposes:
// 1) Cleaner code
// 2) Easier to test individually than to test the entire traverser.
func WalkWithSymlinks(fullPath string, walkFunc filepath.WalkFunc, followSymlinks bool) (err error) {
	// We want to re-queue symlinks up in their evaluated form because filepath.Walk doesn't evaluate them for us.
	// So, what is the plan of attack?
	// Because we can't create endless channels, we create an array instead and use it as a queue.
	// Furthermore, we use a map as a hashset to avoid re-walking any paths we already know.
	type walkItem struct {
		fullPath     string // We need the full, symlink-resolved path to walk against.
		relativeBase string // We also need the relative base path we found the symlink at.
	}

	fullPath, err = filepath.Abs(fullPath)

	if err != nil {
		return err
	}

	walkQueue := []walkItem{{fullPath: fullPath, relativeBase: ""}}
	seenPaths := map[string]bool{} // do NOT put fullPath: true into the map at this time, because we want to match the semantics of filepath.Walk, where the walkfunc is called for the root

	for len(walkQueue) > 0 {
		queueItem := walkQueue[0]
		walkQueue = walkQueue[1:]

		err = filepath.Walk(queueItem.fullPath, func(filePath string, fileInfo os.FileInfo, fileError error) error {
			if fileError != nil {
				glcm.Info(fmt.Sprintf("Accessing %s failed with error: %s", filePath, fileError))
				return nil
			}

			computedRelativePath := strings.TrimPrefix(cleanLocalPath(filePath), cleanLocalPath(queueItem.fullPath))
			computedRelativePath = cleanLocalPath(common.GenerateFullPath(queueItem.relativeBase, computedRelativePath))
			computedRelativePath = strings.TrimPrefix(computedRelativePath, common.AZCOPY_PATH_SEPARATOR_STRING)

			if followSymlinks && fileInfo.Mode()&os.ModeSymlink != 0 {
				result, err := filepath.EvalSymlinks(filePath)

				if err != nil {
					glcm.Info(fmt.Sprintf("Failed to resolve symlink %s: %s", filePath, err))
					return nil
				}

				result, err = filepath.Abs(result)
				if err != nil {
					glcm.Info(fmt.Sprintf("Failed to get absolute path of symlink result %s: %s", filePath, err))
					return nil
				}

				slPath, err := filepath.Abs(filePath)
				if err != nil {
					glcm.Info(fmt.Sprintf("Failed to get absolute path of %s: %s", filePath, err))
				}

				if _, ok := seenPaths[result]; !ok {
					seenPaths[result] = true
					seenPaths[slPath] = true // Note we've seen the symlink as well. We shouldn't ever have issues if we _don't_ do this because we'll just catch it by symlink result
					walkQueue = append(walkQueue, walkItem{
						fullPath:     result,
						relativeBase: computedRelativePath,
					})
				} else {
					glcm.Info(fmt.Sprintf("Ignored already linked directory pointed at %s (link at %s)", result, common.GenerateFullPath(fullPath, computedRelativePath)))
				}
				return nil
			} else {
				result, err := filepath.Abs(filePath)

				if err != nil {
					glcm.Info(fmt.Sprintf("Failed to get absolute path of %s: %s", filePath, err))
					return nil
				}

				if _, ok := seenPaths[result]; !ok {
					seenPaths[result] = true
					return walkFunc(common.GenerateFullPath(fullPath, computedRelativePath), fileInfo, fileError)
				} else {
					// Output resulting path of symlink and symlink source
					glcm.Info(fmt.Sprintf("Ignored already seen file located at %s (found at %s)", filePath, common.GenerateFullPath(fullPath, computedRelativePath)))
					return nil
				}
			}
		})
	}
	return
}

func (t *localTraverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) (err error) {
	singleFileInfo, isSingleFile, err := t.getInfoIfSingleFile()

	if err != nil {
		return fmt.Errorf("cannot scan the path %s, please verify that it is a valid", t.fullPath)
	}

	// if the path is a single file, then pass it through the filters and send to processor
	if isSingleFile {
		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.File())
		}

		return processIfPassedFilters(filters,
			newStoredObject(
				preprocessor,
				singleFileInfo.Name(),
				"",
				common.EEntityType.File(),
				singleFileInfo.ModTime(),
				singleFileInfo.Size(),
				noContentProps, // Local MD5s are computed in the STE, and other props don't apply to local files
				noBlobProps,
				noMetdata,
				"", // Local has no such thing as containers
			),
			processor,
		)
	} else {
		if t.recursive {
			processFile := func(filePath string, fileInfo os.FileInfo, fileError error) error {
				if fileError != nil {
					glcm.Info(fmt.Sprintf("Accessing %s failed with error: %s", filePath, fileError))
					return nil
				}

				var entityType common.EntityType
				if fileInfo.IsDir() {
					entityType = common.EEntityType.Folder()
				} else {
					entityType = common.EEntityType.File()
				}

				relPath := strings.TrimPrefix(strings.TrimPrefix(cleanLocalPath(filePath), cleanLocalPath(t.fullPath)), common.DeterminePathSeparator(t.fullPath))
				if !t.followSymlinks && fileInfo.Mode()&os.ModeSymlink != 0 {
					glcm.Info(fmt.Sprintf("Skipping over symlink at %s because --follow-symlinks is false", common.GenerateFullPath(t.fullPath, relPath)))
					return nil
				}

				if t.incrementEnumerationCounter != nil {
					t.incrementEnumerationCounter(entityType)
				}

				return processIfPassedFilters(filters,
					newStoredObject(
						preprocessor,
						fileInfo.Name(),
						strings.ReplaceAll(relPath, common.DeterminePathSeparator(t.fullPath), common.AZCOPY_PATH_SEPARATOR_STRING), // Consolidate relative paths to the azcopy path separator for sync
						entityType,
						fileInfo.ModTime(), // get this for both files and folders, since sync needs it for both.
						fileInfo.Size(),
						noContentProps, // Local MD5s are computed in the STE, and other props don't apply to local files
						noBlobProps,
						noMetdata,
						"", // Local has no such thing as containers
					),
					processor)
			}

			// note: Walk includes root, so no need here to separately create storedObject for root (as we do for other folder-aware sources)
			return WalkWithSymlinks(t.fullPath, processFile, t.followSymlinks)
		} else {
			// if recursive is off, we only need to scan the files immediately under the fullPath
			// We don't transfer any directory properties here, not even the root. (Because the root's
			// properties won't be transferred, because the only way to do a non-recursive directory transfer
			// is with /* (aka stripTopDir).
			files, err := ioutil.ReadDir(t.fullPath)
			if err != nil {
				return err
			}

			// go through the files and return if any of them fail to process
			for _, singleFile := range files {
				// This won't change. It's purely to hand info off to STE about where the symlink lives.
				relativePath := singleFile.Name()
				if singleFile.Mode()&os.ModeSymlink != 0 {
					if !t.followSymlinks {
						continue
					} else {
						// Because this only goes one layer deep, we can just append the filename to fullPath and resolve with it.
						symlinkPath := common.GenerateFullPath(t.fullPath, singleFile.Name())
						// Evaluate the symlink
						result, err := filepath.EvalSymlinks(symlinkPath)

						if err != nil {
							return err
						}

						// Resolve the absolute file path of the symlink
						result, err = filepath.Abs(result)

						if err != nil {
							return err
						}

						// Replace the current FileInfo with
						singleFile, err = os.Stat(result)

						if err != nil {
							return err
						}
					}
				}

				if singleFile.IsDir() {
					continue
					// it does't make sense to transfer directory properties when not recursing
				}

				if t.incrementEnumerationCounter != nil {
					t.incrementEnumerationCounter(common.EEntityType.File())
				}

				err := processIfPassedFilters(filters,
					newStoredObject(
						preprocessor,
						singleFile.Name(),
						strings.ReplaceAll(relativePath, common.DeterminePathSeparator(t.fullPath), common.AZCOPY_PATH_SEPARATOR_STRING), // Consolidate relative paths to the azcopy path separator for sync
						common.EEntityType.File(), // TODO: add code path for folders
						singleFile.ModTime(),
						singleFile.Size(),
						noContentProps, // Local MD5s are computed in the STE, and other props don't apply to local files
						noBlobProps,
						noMetdata,
						"", // Local has no such thing as containers
					),
					processor)

				if err != nil {
					return err
				}
			}
		}
	}

	return
}

// Replace azcopy path separators (/) with the OS path separator
func consolidatePathSeparators(path string) string {
	pathSep := common.DeterminePathSeparator(path)

	return strings.ReplaceAll(path, common.AZCOPY_PATH_SEPARATOR_STRING, pathSep)
}

func newLocalTraverser(fullPath string, recursive bool, followSymlinks bool, incrementEnumerationCounter enumerationCounterFunc) *localTraverser {
	traverser := localTraverser{
		fullPath:                    cleanLocalPath(fullPath),
		recursive:                   recursive,
		followSymlinks:              followSymlinks,
		incrementEnumerationCounter: incrementEnumerationCounter}
	return &traverser
}

func cleanLocalPath(localPath string) string {
	localPathSeparator := common.DeterminePathSeparator(localPath)
	// path.Clean only likes /, and will only handle /. So, we consolidate it to /.
	// it will do absolutely nothing with \.
	normalizedPath := path.Clean(strings.ReplaceAll(localPath, localPathSeparator, common.AZCOPY_PATH_SEPARATOR_STRING))
	// return normalizedPath path separator.
	normalizedPath = strings.ReplaceAll(normalizedPath, common.AZCOPY_PATH_SEPARATOR_STRING, localPathSeparator)

	// path.Clean steals the first / from the // or \\ prefix.
	if strings.HasPrefix(localPath, `\\`) || strings.HasPrefix(localPath, `//`) {
		// return the \ we stole from the UNC/extended path.
		normalizedPath = localPathSeparator + normalizedPath
	}

	// path.Clean steals the last / from C:\, C:/, and does not add one for C:
	if common.RootDriveRegex.MatchString(strings.ReplaceAll(common.ToShortPath(normalizedPath), common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING)) {
		normalizedPath += common.OS_PATH_SEPARATOR
	}

	return normalizedPath
}
