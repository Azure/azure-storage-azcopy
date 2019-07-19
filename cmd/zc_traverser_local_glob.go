package cmd

import (
	"os"
	"path/filepath"
	"strings"
)

type localGlobTraverser struct {
	fullPath       string
	recursive      bool
	followSymlinks bool

	incrementEnumerationCounter func()
}

// TODO: Replace me with Ze's list traverser.
func (t *localGlobTraverser) traverse(processor objectProcessor, filters []objectFilter) error {
	// This should only be chosen if there's a glob to do, so let's get globbing.
	// We don't have to worry about following symlinks here-- filepath.Glob follows them for the globbing.
	// Furthermore, all symlink functionality should live in localTraverser
	matches, err := filepath.Glob(t.fullPath)

	/*
		This is a little janky what we're actually doing.
		In essence, we're globbing and then spawning an objectTraverser for each globbed object.
	*/

	if err != nil {
		return err
	}

	for _, v := range matches {
		// Detect if it's a single file or not so we can avoid recursion if needbe.
		// Realistically, we could probably just process here but that's extra code that's already in the local traverser.
		_, isFile, err := t.getInfoIfSingleFile(v)
		if err != nil {
			return err
		}

		if !isFile && !t.recursive {
			continue
		}

		// Clean our origin path so the traversers can make use of them
		v = cleanLocalPath(v)
		// Initialize a new local traverser. We won't need to worry about other traversers because globbing is local only.
		nt := newLocalTraverser(v, t.recursive, t.followSymlinks, t.incrementEnumerationCounter)

		// Inject the current relative path under the relative path found by the traverser.
		// This allows us to submit a complete relative path to the requester.
		injectedProcessor := func(object storedObject) error {
			// Craft our relative path based off the matched object vs our base path
			relativePath := strings.TrimPrefix(v, t.getBasePath())
			// Craft the object relative path based off our actual base relative path and the found relative path
			object.relativePath = filepath.Join(relativePath, object.relativePath)

			// Continue by running the original processor.
			return processor(object)
		}

		// Run the local traverser.
		err = nt.traverse(injectedProcessor, filters)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *localGlobTraverser) getInfoIfSingleFile(path string) (os.FileInfo, bool, error) {
	fileInfo, err := os.Stat(path)

	if err != nil {
		return nil, false, err
	}

	if fileInfo.IsDir() {
		return nil, false, nil
	}

	return fileInfo, true, nil
}

func (t *localGlobTraverser) getBasePath() string {
	return trimWildcards(t.fullPath)
}

func trimWildcards(path string) string {
	if strings.Index(path, "*") == -1 {
		return path
	}

	return path[:strings.LastIndex(replacePathSeparators(path[:strings.Index(path, "*")]), "/")+1]
}

func newGlobTraverser(fullGlobPath string, recursive bool, followSymlinks bool, incrementEnumerationCounter func()) *localGlobTraverser {
	traverser := localGlobTraverser{
		fullPath:                    cleanLocalPath(fullGlobPath),
		recursive:                   recursive,
		followSymlinks:              followSymlinks,
		incrementEnumerationCounter: incrementEnumerationCounter,
	}

	return &traverser
}
