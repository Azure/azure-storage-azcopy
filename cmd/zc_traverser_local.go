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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
)

type localTraverser struct {
	fullPath       string
	recursive      bool
	followSymlinks bool
	appCtx         *context.Context
	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc
	errorChannel                chan ErrorFileInfo
}

func (t *localTraverser) IsDirectory(bool) bool {
	if strings.HasSuffix(t.fullPath, "/") {
		return true
	}

	props, err := common.OSStat(t.fullPath)

	if err != nil {
		return false
	}

	return props.IsDir()
}

func (t *localTraverser) getInfoIfSingleFile() (os.FileInfo, bool, error) {
	fileInfo, err := common.OSStat(t.fullPath)

	if err != nil {
		return nil, false, err
	}

	if fileInfo.IsDir() {
		return nil, false, nil
	}

	return fileInfo, true, nil
}

func UnfurlSymlinks(symlinkPath string) (result string, err error) {
	var count uint32
	unfurlingPlan := []string{symlinkPath}

	// We need to do some special UNC path handling for windows.
	if runtime.GOOS != "windows" {
		return filepath.EvalSymlinks(symlinkPath)
	}

	for len(unfurlingPlan) > 0 {
		item := unfurlingPlan[0]

		fi, err := os.Lstat(item)

		if err != nil {
			return item, err
		}

		if fi.Mode()&os.ModeSymlink != 0 {
			result, err := os.Readlink(item)

			if err != nil {
				return result, err
			}

			// Previously, we'd try to detect if the read link was a relative path by appending and starting the item
			// However, it seems to be a fairly unlikely and hard to reproduce scenario upon investigation (Couldn't manage to reproduce the scenario)
			// So it was dropped. However, on the off chance, we'll still do it if syntactically it makes sense.
			if result == "" || result == "." { // A relative path being "" or "." likely (and in the latter case, on our officially supported OSes, always) means that it's just the same folder.
				result = filepath.Dir(item)
			} else if !os.IsPathSeparator(result[0]) { // We can assume that a relative path won't start with a separator
				possiblyResult := filepath.Join(filepath.Dir(item), result)
				if _, err := os.Lstat(possiblyResult); err == nil {
					result = possiblyResult
				}
			}

			result = common.ToExtendedPath(result)

			/*
			 * Either we can store all the symlink seen till now for this path or we count how many iterations to find out cyclic loop.
			 * Choose the count method and restrict the number of links to 40. Which linux kernel adhere.
			 */
			if count >= 40 {
				return "", errors.New("failed to unfurl symlink: too many links")
			}

			unfurlingPlan = append(unfurlingPlan, result)
		} else {
			return item, nil
		}

		unfurlingPlan = unfurlingPlan[1:]
		count++
	}

	return "", errors.New("failed to unfurl symlink: exited loop early")
}

type seenPathsRecorder interface {
	Record(path string)
	HasSeen(path string) bool
}

type nullSeenPathsRecorder struct{}

func (*nullSeenPathsRecorder) Record(_ string) {
	// no-op
}
func (*nullSeenPathsRecorder) HasSeen(_ string) bool {
	return false // in the null case, there are no symlinks in play, so no cycles, so we have never seen the path before
}

type realSeenPathsRecorder struct {
	m map[string]struct{}
}

func (r *realSeenPathsRecorder) Record(path string) {
	r.m[path] = struct{}{}
}
func (r *realSeenPathsRecorder) HasSeen(path string) bool {
	_, ok := r.m[path]
	return ok
}

type symlinkTargetFileInfo struct {
	os.FileInfo
	name string
}

// ErrorFileInfo holds information about files and folders that failed enumeration.
type ErrorFileInfo struct {
	FilePath string
	FileInfo os.FileInfo
	ErrorMsg error
}

func (s symlinkTargetFileInfo) Name() string {
	return s.name // override the name
}

// WalkWithSymlinks is a symlinks-aware, parallelized, version of filePath.Walk.
// Separate this from the traverser for two purposes:
// 1) Cleaner code
// 2) Easier to test individually than to test the entire traverser.
func WalkWithSymlinks(appCtx *context.Context, fullPath string, walkFunc filepath.WalkFunc, followSymlinks bool, errorChannel chan ErrorFileInfo) (err error) {

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

	// do NOT put fullPath: true into the map at this time, because we want to match the semantics of filepath.Walk, where the walkfunc is called for the root
	// When following symlinks, our current implementation tracks folders and files.  Which may consume GB's of RAM when there are 10s of millions of files.
	var seenPaths seenPathsRecorder = &nullSeenPathsRecorder{} // uses no RAM
	if followSymlinks {
		seenPaths = &realSeenPathsRecorder{make(map[string]struct{})} // have to use the RAM if we are dealing with symlinks, to prevent cycles
	}

	for len(walkQueue) > 0 {
		queueItem := walkQueue[0]
		walkQueue = walkQueue[1:]
		// walk contents of this queueItem in parallel
		// (for simplicity of coding, we don't parallelize across multiple queueItems)
		parallel.Walk(appCtx, queueItem.fullPath, EnumerationParallelism, EnumerationParallelStatFiles, func(filePath string, fileInfo os.FileInfo, fileError error) error {
			if fileError != nil {
				WarnStdoutAndScanningLog(fmt.Sprintf("Accessing '%s' failed with error: %s", filePath, fileError))
				if errorChannel != nil {
					errorChannel <- ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: fileError}
				}
				return nil
			}
			computedRelativePath := strings.TrimPrefix(cleanLocalPath(filePath), cleanLocalPath(queueItem.fullPath))
			computedRelativePath = cleanLocalPath(common.GenerateFullPath(queueItem.relativeBase, computedRelativePath))
			computedRelativePath = strings.TrimPrefix(computedRelativePath, common.AZCOPY_PATH_SEPARATOR_STRING)

			if computedRelativePath == "." {
				computedRelativePath = ""
			}

			// TODO: Later we might want to transfer these special files as such.
			unsupportedFileTypes := (os.ModeSocket | os.ModeNamedPipe | os.ModeIrregular | os.ModeDevice)

			if (fileInfo.Mode() & unsupportedFileTypes) != 0 {
				err := fmt.Errorf("Unsupported file type %s: %v", filePath, fileInfo.Mode())
				WarnStdoutAndScanningLog(err.Error())

				if errorChannel != nil {
					errorChannel <- ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: err}
				}
				return nil
			}

			if fileInfo.Mode()&os.ModeSymlink != 0 {
				if !followSymlinks {
					return nil // skip it
				}

				/*
				 * There is one case where symlink can point to outside of sharepoint(symlink is absolute path). In that case
				 * we need to throw error. Its very unlikely same file or folder present on the agent side.
				 * In that case it anywaythrow the error.
				 *
				 * TODO: Need to handle this case.
				 */
				result, err := UnfurlSymlinks(filePath)

				if err != nil {
					err = fmt.Errorf("Failed to resolve symlink %s: %s", filePath, err)
					WarnStdoutAndScanningLog(err.Error())
					if errorChannel != nil {
						errorChannel <- ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: err}
					}
					return nil
				}

				result, err = filepath.Abs(result)
				if err != nil {
					err = fmt.Errorf("Failed to get absolute path of symlink result %s: %s", filePath, err)
					WarnStdoutAndScanningLog(err.Error())
					if errorChannel != nil {
						errorChannel <- ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: err}
					}
					return nil
				}

				slPath, err := filepath.Abs(filePath)
				if err != nil {
					err = fmt.Errorf("Failed to get absolute path of %s: %s", filePath, err)
					WarnStdoutAndScanningLog(err.Error())
					if errorChannel != nil {
						errorChannel <- ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: err}
					}
					return nil
				}

				rStat, err := os.Stat(result)
				if err != nil {
					err = fmt.Errorf("Failed to get properties of symlink target at %s: %s", result, err)
					WarnStdoutAndScanningLog(err.Error())
					if errorChannel != nil {
						errorChannel <- ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: err}
					}
					return nil
				}

				if rStat.IsDir() {
					if !seenPaths.HasSeen(result) {
						err := walkFunc(common.GenerateFullPath(fullPath, computedRelativePath), symlinkTargetFileInfo{rStat, fileInfo.Name()}, fileError)
						// Since this doesn't directly manipulate the error, and only checks for a specific error, it's OK to use in a generic function.
						skipped, err := getProcessingError(err)

						if !skipped { // Don't go any deeper (or record it) if we skipped it.
							seenPaths.Record(common.ToExtendedPath(result))
							seenPaths.Record(common.ToExtendedPath(slPath)) // Note we've seen the symlink as well. We shouldn't ever have issues if we _don't_ do this because we'll just catch it by symlink result
							walkQueue = append(walkQueue, walkItem{
								fullPath:     result,
								relativeBase: computedRelativePath,
							})
						}
						// enumerate the FOLDER now (since its presence in seenDirs will prevent its properties getting enumerated later)
						return err
					} else {
						WarnStdoutAndScanningLog(fmt.Sprintf("Ignored already linked directory pointed at %s (link at %s)", result, common.GenerateFullPath(fullPath, computedRelativePath)))
					}
				} else {
					// It's a symlink to a file and we handle cyclic symlinks.
					// (this does create the inconsistency that if there are two symlinks to the same file we will process it twice,
					// but if there are two symlinks to the same directory we will process it only once. Because only directories are
					// deduped to break cycles.  For now, we are living with the inconsistency. The alternative would be to "burn" more
					// RAM by putting filepaths into seenDirs too, but that could be a non-trivial amount of RAM in big directories trees).
					targetFi := symlinkTargetFileInfo{rStat, fileInfo.Name()}

					err := walkFunc(common.GenerateFullPath(fullPath, computedRelativePath), targetFi, fileError)
					_, err = getProcessingError(err)
					return err
				}
				return nil
			} else {
				// not a symlink
				result, err := filepath.Abs(filePath)

				if err != nil {
					err = fmt.Errorf("Failed to get absolute path of %s: %s", filePath, err)
					WarnStdoutAndScanningLog(err.Error())
					if errorChannel != nil {
						errorChannel <- ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: err}
					}
					return nil
				}

				if !seenPaths.HasSeen(result) {
					err := walkFunc(common.GenerateFullPath(fullPath, computedRelativePath), fileInfo, fileError)
					// Since this doesn't directly manipulate the error, and only checks for a specific error, it's OK to use in a generic function.
					skipped, err := getProcessingError(err)

					// If the file was skipped, don't record it.
					if !skipped {
						seenPaths.Record(common.ToExtendedPath(result))
					}

					return err
				} else {
					if fileInfo.IsDir() {
						// We can't output a warning here (and versions 10.3.x never did)
						// because we'll hit this for the directory that is the direct (root) target of any symlink, so any warning here would be a red herring.
						// In theory there might be cases when a warning here would be correct - but they are rare and too hard to identify in our code
					} else {
						WarnStdoutAndScanningLog(fmt.Sprintf("Ignored already seen file located at %s (found at %s)", filePath, common.GenerateFullPath(fullPath, computedRelativePath)))
					}
					return nil
				}
			}
		})
	}
	return
}

func (t *localTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	singleFileInfo, isSingleFile, err := t.getInfoIfSingleFile()

	if err != nil {
		azcopyScanningLogger.Log(pipeline.LogError, fmt.Sprintf("Failed to scan path %s: %s", t.fullPath, err.Error()))
		return fmt.Errorf("failed to scan path %s due to %s", t.fullPath, err.Error())
	}

	// if the path is a single file, then pass it through the filters and send to processor
	if isSingleFile {
		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.File())
		}

		err := processIfPassedFilters(filters,
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
		_, err = getProcessingError(err)
		return err
	} else {
		if t.recursive {
			processFile := func(filePath string, fileInfo os.FileInfo, fileError error) error {
				if fileError != nil {
					WarnStdoutAndScanningLog(fmt.Sprintf("Accessing %s failed with error: %s", filePath, fileError))
					return nil
				}

				var entityType common.EntityType
				if fileInfo.IsDir() {
					newFileInfo, err := WrapFolder(filePath, fileInfo)
					if err != nil {
						WarnStdoutAndScanningLog(fmt.Sprintf("Failed to get last change of target at %s: %s", filePath, err))
					} else {
						// fileInfo becomes nil in case we fail to wrap folder.
						fileInfo = newFileInfo
					}

					entityType = common.EEntityType.Folder()
				} else {
					entityType = common.EEntityType.File()
				}

				relPath := strings.TrimPrefix(strings.TrimPrefix(cleanLocalPath(filePath), cleanLocalPath(t.fullPath)), common.DeterminePathSeparator(t.fullPath))
				if !t.followSymlinks && fileInfo.Mode()&os.ModeSymlink != 0 {
					WarnStdoutAndScanningLog(fmt.Sprintf("Skipping over symlink at %s because --follow-symlinks is false", common.GenerateFullPath(t.fullPath, relPath)))
					return nil
				}

				if t.incrementEnumerationCounter != nil {
					t.incrementEnumerationCounter(entityType)
				}

				// This is an exception to the rule. We don't strip the error here, because WalkWithSymlinks catches it.
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

			// note: Walk includes root, so no need here to separately create StoredObject for root (as we do for other folder-aware sources)
			return WalkWithSymlinks(t.appCtx, t.fullPath, processFile, t.followSymlinks, t.errorChannel)

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
						result, err := UnfurlSymlinks(symlinkPath)

						if err != nil {
							return err
						}

						// Resolve the absolute file path of the symlink
						result, err = filepath.Abs(result)

						if err != nil {
							return err
						}

						// Replace the current FileInfo with
						singleFile, err = common.OSStat(result)

						if err != nil {
							return err
						}
					}
				}

				if singleFile.IsDir() {
					continue
					// it doesn't make sense to transfer directory properties when not recurring
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
				_, err = getProcessingError(err)
				if err != nil {
					return err
				}
			}
		}
	}

	return
}

func newLocalTraverser(ctx *context.Context, fullPath string, recursive bool, followSymlinks bool, incrementEnumerationCounter enumerationCounterFunc, errorChannel chan ErrorFileInfo) *localTraverser {
	traverser := localTraverser{
		fullPath:                    cleanLocalPath(fullPath),
		recursive:                   recursive,
		followSymlinks:              followSymlinks,
		appCtx:                      ctx,
		incrementEnumerationCounter: incrementEnumerationCounter,
		errorChannel:                errorChannel}
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
