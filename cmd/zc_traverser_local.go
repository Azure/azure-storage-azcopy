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
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
)

type localTraverser struct {
	fullPath        string
	recursive       bool
	stripTopDir     bool
	symlinkHandling common.SymlinkHandlingType
	appCtx          context.Context
	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc
	errorChannel                chan<- ErrorFileInfo

	targetHashType common.SyncHashType
	hashAdapter    common.HashDataAdapter
	// receives fullPath entries and manages hashing of files lacking metadata.
	hashTargetChannel chan string
	hardlinkHandling  common.PreserveHardlinksOption
}

func (t *localTraverser) IsDirectory(bool) (bool, error) {
	if strings.HasSuffix(t.fullPath, "/") {
		return true, nil
	}

	props, err := common.OSStat(t.fullPath)

	if err != nil {
		return false, err
	}

	return props.IsDir(), nil
}

func (t *localTraverser) getInfoIfSingleFile() (os.FileInfo, bool, error) {
	if t.stripTopDir {
		return nil, false, nil // StripTopDir can NEVER be a single file. If a user wants to target a single file, they must escape the *.
	}

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
			if count >= MAX_SYMLINKS_TO_FOLLOW {
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

func writeToErrorChannel(errorChannel chan<- ErrorFileInfo, err ErrorFileInfo) {
	if errorChannel != nil {
		errorChannel <- err
	}
}

// WalkWithSymlinks is a symlinks-aware, parallelized, version of filePath.Walk.
// Separate this from the traverser for two purposes:
// 1) Cleaner code
// 2) Easier to test individually than to test the entire traverser.
func WalkWithSymlinks(appCtx context.Context, fullPath string, walkFunc filepath.WalkFunc, symlinkHandling common.SymlinkHandlingType, errorChannel chan<- ErrorFileInfo, hardlinkHandling common.PreserveHardlinksOption, incrementEnumerationCounter enumerationCounterFunc) (err error) {

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
	if symlinkHandling.Follow() {                              // only if we're following we need to worry about this
		seenPaths = &realSeenPathsRecorder{make(map[string]struct{})} // have to use the RAM if we are dealing with symlinks, to prevent cycles
	}

	for len(walkQueue) > 0 {
		queueItem := walkQueue[0]
		walkQueue = walkQueue[1:]
		// walk contents of this queueItem in parallel
		// (for simplicity of coding, we don't parallelize across multiple queueItems)
		parallel.Walk(appCtx, queueItem.fullPath, EnumerationParallelism, EnumerationParallelStatFiles, func(filePath string, fileInfo os.FileInfo, fileError error) error {
			if fileError != nil {
				WarnStdoutAndScanningLog(fmt.Sprintf("Accessing '%s' failed with error: %s", filePath, fileError.Error()))
				writeToErrorChannel(errorChannel, ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: fileError})
				return nil
			}
			computedRelativePath := strings.TrimPrefix(cleanLocalPath(filePath), cleanLocalPath(queueItem.fullPath))
			computedRelativePath = cleanLocalPath(common.GenerateFullPath(queueItem.relativeBase, computedRelativePath))
			computedRelativePath = strings.TrimPrefix(computedRelativePath, common.AZCOPY_PATH_SEPARATOR_STRING)

			if computedRelativePath == "." {
				computedRelativePath = ""
			}

			if fileInfo == nil {
				err := fmt.Errorf("fileInfo is nil for file %s", filePath)
				WarnStdoutAndScanningLog(err.Error())
				return nil
			}
			if fileInfo.Mode()&os.ModeSymlink != 0 {
				if symlinkHandling.Preserve() {
					// Handle it like it's not a symlink
					result, err := filepath.Abs(filePath)

					if err != nil {
						WarnStdoutAndScanningLog(fmt.Sprintf("Failed to get absolute path of %s: %s", filePath, err))
						return nil
					}

					err = walkFunc(common.GenerateFullPath(fullPath, computedRelativePath), fileInfo, fileError)
					// Since this doesn't directly manipulate the error, and only checks for a specific error, it's OK to use in a generic function.
					skipped, err := getProcessingError(err)

					// If the file was skipped, don't record it.
					if !skipped {
						seenPaths.Record(common.ToExtendedPath(result))
					}

					return err
				}

				if symlinkHandling.None() {
					if isNFSCopy {
						if incrementEnumerationCounter != nil {
							incrementEnumerationCounter(common.EEntityType.Symlink())
						}
						logNFSLinkWarning(fileInfo.Name(), "", true)
					}
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
					err = fmt.Errorf("failed to resolve symlink %s: %w", filePath, err)
					WarnStdoutAndScanningLog(err.Error())
					writeToErrorChannel(errorChannel, ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: err})
					return nil
				}

				result, err = filepath.Abs(result)
				if err != nil {
					err = fmt.Errorf("failed to get absolute path of symlink result %s: %w", filePath, err)
					WarnStdoutAndScanningLog(err.Error())
					writeToErrorChannel(errorChannel, ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: err})
					return nil
				}

				slPath, err := filepath.Abs(filePath)
				if err != nil {
					err = fmt.Errorf("failed to get absolute path of %s: %w", filePath, err)
					WarnStdoutAndScanningLog(err.Error())
					writeToErrorChannel(errorChannel, ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: err})
					return nil
				}

				rStat, err := os.Stat(result)
				if err != nil {
					err = fmt.Errorf("failed to get properties of symlink target at %s: %w", result, err)
					WarnStdoutAndScanningLog(err.Error())
					writeToErrorChannel(errorChannel, ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: err})
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
				if isNFSCopy {
					LogHardLinkIfDefaultPolicy(fileInfo, hardlinkHandling)
					if !IsRegularFile(fileInfo) && !fileInfo.IsDir() {
						// We don't want to process other non-regular files here.
						if incrementEnumerationCounter != nil {
							incrementEnumerationCounter(common.EEntityType.Other())
						}
						logSpecialFileWarning(fileInfo.Name())
						return nil
					}
				}
				// not a symlink
				result, err := filepath.Abs(filePath)

				if err != nil {
					err = fmt.Errorf("failed to get absolute path of %s: %w", filePath, err)
					WarnStdoutAndScanningLog(err.Error())
					writeToErrorChannel(errorChannel, ErrorFileInfo{FilePath: filePath, FileInfo: fileInfo, ErrorMsg: err})
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

func (t *localTraverser) GetHashData(relPath string) (*common.SyncHashData, error) {
	if t.targetHashType == common.ESyncHashType.None() {
		return nil, nil // no-op
	}

	fullPath := filepath.Join(t.fullPath, relPath)
	fi, err := os.Stat(fullPath) // grab the stat so we can tell if the hash is valid
	if err != nil {
		return nil, err
	}

	if fi.IsDir() {
		return nil, nil // there is no hash data on directories
	}

	// If a hash is considered unusable by some metric, attempt to set it up for generation, if the user allows it.
	handleHashingError := func(err error) (*common.SyncHashData, error) {
		switch err {
		case ErrorNoHashPresent,
			ErrorHashNoLongerValid,
			ErrorHashNotCompatible:
			break
		default:
			return nil, err
		}

		// defer hashing to the goroutine
		t.hashTargetChannel <- relPath
		return nil, ErrorHashAsyncCalculation
	}

	// attempt to grab existing hash data, and ensure it's validity.
	data, err := t.hashAdapter.GetHashData(relPath)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			common.LogHashStorageFailure()
			if azcopyScanningLogger != nil {
				azcopyScanningLogger.Log(common.LogError, fmt.Sprintf("failed to read hash data for %s: %s", relPath, err.Error()))
			}
		}

		// Treat failure to read/parse/etc like a missing hash.
		return handleHashingError(ErrorNoHashPresent)
	} else {
		if data.Mode != t.targetHashType {
			return handleHashingError(ErrorHashNotCompatible)
		}

		if !data.LMT.Equal(fi.ModTime()) {
			return handleHashingError(ErrorHashNoLongerValid)
		}

		return data, nil
	}
}

// prepareHashingThreads creates background threads to perform hashing on local files that are missing hashes.
// It returns a finalizer and a wrapped processor-- Use the wrapped processor in place of the original processor (even if synchashtype is none)
// and wrap the error getting returned in the finalizer function to kill the background threads.
func (t *localTraverser) prepareHashingThreads(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (finalizer func(existingErr error) error, hashingProcessor func(obj StoredObject) error) {
	if t.targetHashType == common.ESyncHashType.None() { // if no hashing is needed, do nothing.
		return func(existingErr error) error {
			return existingErr // nothing to overwrite with, no-op
		}, processor
	}

	// set up for threaded hashing
	t.hashTargetChannel = make(chan string, 1_000) // "reasonable" backlog
	// Use half of the available CPU cores for hashing to prevent throttling the STE too hard if hashing is still occurring when the first job part gets sent out
	hashingThreadCount := runtime.NumCPU() / 2
	hashError := make(chan error, hashingThreadCount)
	wg := &sync.WaitGroup{}
	immediateStopHashing := int32(0)

	// create return wrapper to handle hashing errors
	finalizer = func(existingErr error) error {
		if existingErr != nil {
			close(t.hashTargetChannel)                  // stop sending hashes
			atomic.StoreInt32(&immediateStopHashing, 1) // force the end of hashing
			wg.Wait()                                   // Await the finalization of all hashing

			return existingErr // discard all hashing errors
		} else {
			close(t.hashTargetChannel) // stop sending hashes

			wg.Wait()                    // Await the finalization of all hashing
			close(hashError)             // close out the error channel
			for err := range hashError { // inspect all hashing errors
				if err != nil {
					return err
				}
			}

			return nil
		}
	}

	// wrap the processor, preventing a data race
	commitMutex := &sync.Mutex{}
	mutexProcessor := func(proc objectProcessor) objectProcessor {
		return func(object StoredObject) error {
			commitMutex.Lock() // prevent committing two objects at once to prevent a data race
			defer commitMutex.Unlock()
			err := proc(object)

			return err
		}
	}
	processor = mutexProcessor(processor)

	// spin up hashing threads
	for i := 0; i < hashingThreadCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done() // mark the hashing thread as completed

			for relPath := range t.hashTargetChannel {
				if atomic.LoadInt32(&immediateStopHashing) == 1 { // should we stop hashing?
					return
				}

				fullPath := filepath.Join(t.fullPath, relPath)
				fi, err := os.Stat(fullPath) // query LMT & if it's a directory
				if err != nil {
					err = fmt.Errorf("failed to get properties of file result %s: %w", relPath, err)
					hashError <- err
					return
				}

				if fi.IsDir() { // this should never happen
					panic(relPath)
				}

				f, err := os.OpenFile(fullPath, os.O_RDONLY, 0644) // perm is not used here since it's RO
				if err != nil {
					err = fmt.Errorf("failed to open file for reading result %s: %w", relPath, err)
					hashError <- err
					return
				}

				var hasher hash.Hash // set up hasher
				switch t.targetHashType {
				case common.ESyncHashType.MD5():
					hasher = md5.New()
				}

				// hash.Hash provides a writer type, allowing us to do a (small, 32MB to be precise) buffered write into the hasher and avoid memory concerns
				_, err = io.Copy(hasher, f)
				if err != nil {
					err = fmt.Errorf("failed to read file into hasher result %s: %w", relPath, err)
					hashError <- err
					return
				}

				sum := hasher.Sum([]byte{})

				hashData := common.SyncHashData{
					Mode: t.targetHashType,
					Data: base64.StdEncoding.EncodeToString(sum),
					LMT:  fi.ModTime(),
				}

				// failing to store hash data doesn't mean we can't transfer (e.g. RO directory)
				err = t.hashAdapter.SetHashData(relPath, &hashData)
				if err != nil {
					common.LogHashStorageFailure()
					if azcopyScanningLogger != nil {
						azcopyScanningLogger.Log(common.LogError, fmt.Sprintf("failed to write hash data for %s: %s", relPath, err.Error()))
					}
				}

				err = processIfPassedFilters(filters,
					newStoredObject(
						func(storedObject *StoredObject) {
							// apply the hash data
							// storedObject.hashData = hashData
							switch hashData.Mode {
							case common.ESyncHashType.MD5():
								storedObject.md5 = sum
							default: // no-op
							}

							if preprocessor != nil {
								// apply the original preprocessor
								preprocessor(storedObject)
							}
						},
						fi.Name(),
						strings.ReplaceAll(relPath, common.DeterminePathSeparator(t.fullPath), common.AZCOPY_PATH_SEPARATOR_STRING),

						common.EEntityType.File(),
						fi.ModTime(),
						fi.Size(),
						noContentProps, // Local MD5s are computed in the STE, and other props don't apply to local files
						noBlobProps,
						noMetadata,
						"", // Local has no such thing as containers
					),
					processor, // the original processor is wrapped in the mutex processor.
				)
				_, err = getProcessingError(err)
				if err != nil {
					hashError <- err
					return
				}
			}
		}()
	}

	// wrap the processor, try to grab hashes, or defer processing to the goroutines
	hashingProcessor = func(storedObject StoredObject) error {
		if storedObject.entityType != common.EEntityType.File() {
			// the original processor is wrapped in the mutex processor.
			return processor(storedObject) // no process folders
		}

		if strings.HasSuffix(path.Base(storedObject.relativePath), common.AzCopyHashDataStream) {
			return nil // do not process hash data files.
		}

		hashData, err := t.GetHashData(storedObject.relativePath)

		if err != nil {
			switch err {
			case ErrorNoHashPresent, ErrorHashNoLongerValid, ErrorHashNotCompatible:
				// the original processor is wrapped in the mutex processor.
				return processor(storedObject) // There is no hash data, so this file will be overwritten (in theory).
			case ErrorHashAsyncCalculation:
				return nil // File will be processed later
			default:
				return err // Cannot get or create hash data for some reason
			}
		}

		// storedObject.hashData = hashData
		switch hashData.Mode {
		case common.ESyncHashType.MD5():
			md5data, _ := base64.StdEncoding.DecodeString(hashData.Data) // If decode fails, treat it like no hash is present.
			storedObject.md5 = md5data
		default: // do nothing, no hash is present.
		}

		// delay the mutex until after potentially long-running operations
		// the original processor is wrapped in the mutex processor.
		return processor(storedObject)
	}

	return finalizer, hashingProcessor
}

func (t *localTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	singleFileInfo, isSingleFile, err := t.getInfoIfSingleFile()
	// it fails here if file does not exist
	if err != nil {
		azcopyScanningLogger.Log(common.LogError, fmt.Sprintf("Failed to scan path %s: %s", t.fullPath, err.Error()))
		return fmt.Errorf("failed to scan path %s due to %w", t.fullPath, err)
	}

	finalizer, hashingProcessor := t.prepareHashingThreads(preprocessor, processor, filters)

	// if the path is a single file, then pass it through the filters and send to processor
	if isSingleFile {

		entityType := common.EEntityType.File()
		if isNFSCopy {
			if singleFileInfo.Mode()&os.ModeSymlink != 0 {
				entityType = common.EEntityType.Symlink()
				if t.incrementEnumerationCounter != nil {
					t.incrementEnumerationCounter(entityType)
				}
				return nil
			} else if IsHardlink(singleFileInfo) {
				LogHardLinkIfDefaultPolicy(singleFileInfo, t.hardlinkHandling)
				entityType = common.EEntityType.Hardlink()
			} else if IsRegularFile(singleFileInfo) {
				entityType = common.EEntityType.File()
			} else {
				entityType = common.EEntityType.Other()
				logSpecialFileWarning(singleFileInfo.Name())
				if t.incrementEnumerationCounter != nil {
					t.incrementEnumerationCounter(entityType)
				}
				return nil
			}
		}

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(entityType)
		}

		err := processIfPassedFilters(filters,
			newStoredObject(
				preprocessor,
				singleFileInfo.Name(),
				"",
				entityType,
				singleFileInfo.ModTime(),
				singleFileInfo.Size(),
				noContentProps, // Local MD5s are computed in the STE, and other props don't apply to local files
				noBlobProps,
				noMetadata,
				"", // Local has no such thing as containers
			),
			hashingProcessor, // hashingProcessor handles the mutex wrapper
		)
		_, err = getProcessingError(err)

		return finalizer(err)
	} else {
		if t.recursive {
			processFile := func(filePath string, fileInfo os.FileInfo, fileError error) error {
				if fileError != nil {
					WarnStdoutAndScanningLog(fmt.Sprintf("Accessing %s failed with error: %s", filePath, fileError.Error()))
					return nil
				}

				var entityType common.EntityType
				if fileInfo.Mode()&os.ModeSymlink == os.ModeSymlink {
					entityType = common.EEntityType.Symlink()
				} else if fileInfo.IsDir() {
					newFileInfo, err := WrapFolder(filePath, fileInfo)
					if err != nil {
						WarnStdoutAndScanningLog(fmt.Sprintf("Failed to get last change of target at %s: %s", filePath, err.Error()))
					} else {
						// fileInfo becomes nil in case we fail to wrap folder.
						fileInfo = newFileInfo
					}

					entityType = common.EEntityType.Folder()
				} else {
					entityType = common.EEntityType.File()
				}

				// NFS Handling
				if isNFSCopy {
					if IsHardlink(fileInfo) {
						entityType = common.EEntityType.Hardlink()
					}
				}

				relPath := strings.TrimPrefix(strings.TrimPrefix(cleanLocalPath(filePath), cleanLocalPath(t.fullPath)), common.DeterminePathSeparator(t.fullPath))
				if t.symlinkHandling.None() && fileInfo.Mode()&os.ModeSymlink != 0 {
					WarnStdoutAndScanningLog(fmt.Sprintf("Skipping over symlink at %s because symlinks are not handled (--follow-symlinks or --preserve-symlinks)", common.GenerateFullPath(t.fullPath, relPath)))
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
						noMetadata,
						"", // Local has no such thing as containers
					),
					hashingProcessor, // hashingProcessor handles the mutex wrapper
				)
			}

			// note: Walk includes root, so no need here to separately create StoredObject for root (as we do for other folder-aware sources)
			return finalizer(WalkWithSymlinks(t.appCtx, t.fullPath, processFile, t.symlinkHandling, t.errorChannel, t.hardlinkHandling, t.incrementEnumerationCounter))
		} else {
			// if recursive is off, we only need to scan the files immediately under the fullPath
			// We don't transfer any directory properties here, not even the root. (Because the root's
			// properties won't be transferred, because the only way to do a non-recursive directory transfer
			// is with /* (aka stripTopDir).
			entries, err := os.ReadDir(t.fullPath)
			if err != nil {
				return err
			}

			entityType := common.EEntityType.File()

			// go through the files and return if any of them fail to process
			for _, entry := range entries {
				// This won't change. It's purely to hand info off to STE about where the symlink lives.
				relativePath := entry.Name()
				fileInfo, _ := entry.Info()
				if fileInfo.Mode()&os.ModeSymlink != 0 {
					if t.symlinkHandling.None() {
						if isNFSCopy && t.incrementEnumerationCounter != nil {
							t.incrementEnumerationCounter(common.EEntityType.Symlink())
						}
						continue
					} else if t.symlinkHandling.Preserve() { // Mark the entity type as a symlink.
						entityType = common.EEntityType.Symlink()
					} else if t.symlinkHandling.Follow() {
						// Because this only goes one layer deep, we can just append the filename to fullPath and resolve with it.
						symlinkPath := common.GenerateFullPath(t.fullPath, entry.Name())
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
						fileInfo, err = common.OSStat(result)

						if err != nil {
							return err
						}
					}
				}
				// NFS handling
				if isNFSCopy {
					if IsHardlink(fileInfo) {
						entityType = common.EEntityType.Hardlink()
					} else if !IsRegularFile(fileInfo) {
						entityType = common.EEntityType.Other()
						if t.incrementEnumerationCounter != nil {
							t.incrementEnumerationCounter(entityType)
						}
						continue
					}
				}

				if entry.IsDir() {
					continue
					// it doesn't make sense to transfer directory properties when not recurring
				}

				if t.incrementEnumerationCounter != nil {
					t.incrementEnumerationCounter(common.EEntityType.File())
				}

				err := processIfPassedFilters(filters,
					newStoredObject(
						preprocessor,
						entry.Name(),
						strings.ReplaceAll(relativePath, common.DeterminePathSeparator(t.fullPath), common.AZCOPY_PATH_SEPARATOR_STRING), // Consolidate relative paths to the azcopy path separator for sync
						entityType, // TODO: add code path for folders
						fileInfo.ModTime(),
						fileInfo.Size(),
						noContentProps, // Local MD5s are computed in the STE, and other props don't apply to local files
						noBlobProps,
						noMetadata,
						"", // Local has no such thing as containers
					),
					hashingProcessor, // hashingProcessor handles the mutex wrapper
				)
				_, err = getProcessingError(err)
				if err != nil {
					return finalizer(err)
				}
			}
		}
	}

	return finalizer(err)
}

func newLocalTraverser(fullPath string, ctx context.Context, opts InitResourceTraverserOptions) (*localTraverser, error) {
	var hashAdapter common.HashDataAdapter
	if opts.SyncHashType != common.ESyncHashType.None() { // Only initialize the hash adapter should we need it.
		var err error
		hashAdapter, err = common.NewHashDataAdapter(common.LocalHashDir, fullPath, common.LocalHashStorageMode)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize hash adapter: %w", err)
		}
	}

	traverser := localTraverser{
		fullPath:                    cleanLocalPath(fullPath),
		recursive:                   opts.Recursive,
		symlinkHandling:             opts.SymlinkHandling,
		appCtx:                      ctx,
		incrementEnumerationCounter: opts.IncrementEnumeration,
		errorChannel:                opts.ErrorChannel,
		targetHashType:              opts.SyncHashType,
		hashAdapter:                 hashAdapter,
		stripTopDir:                 opts.StripTopDir,
		hardlinkHandling:            opts.HardlinkHandling,
	}
	return &traverser, nil
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

func logSpecialFileWarning(fileName string) {
	if common.AzcopyCurrentJobLogger == nil {
		return
	}

	message := fmt.Sprintf("File '%s' at the source is a special file and will be skipped and not copied", fileName)
	common.AzcopyCurrentJobLogger.Log(common.LogWarning, message)
}

// logNFSLinkWarning logs a warning for either a symbolic link or a hard link in an NFS share.
// - For symlinks: inodeNo should be empty.
// - For hard links: inodeNo should be the file's inode number.
func logNFSLinkWarning(fileName, inodeNo string, isSymlink bool) {
	if common.AzcopyCurrentJobLogger == nil {
		return
	}

	var message string
	if isSymlink {
		message = fmt.Sprintf("File '%s' at the source is a symbolic link and will be skipped and not copied", fileName)
	} else {
		message = fmt.Sprintf("File '%s' with inode '%s' at the source is a hard link, but is copied as a full file", fileName, inodeNo)
	}

	common.AzcopyCurrentJobLogger.Log(common.LogWarning, message)
}
