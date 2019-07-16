package cmd

import (
	"os"
	"path/filepath"
	"strings"
)

type localGlobTraverser struct {
	fullPath string
	recursive bool

	incrementEnumerationCounter func()
}

func (t *localGlobTraverser) traverse(processor objectProcessor, filters []objectFilter) error {
	// This should only be chosen if there's a glob to do, so let's get globbing.
	matches, err := filepath.Glob(t.fullPath)

	/*
	This is a little janky what we're actually doing.
	In essence, we're globbing and then spawning an objectTraverser for each globbed object.
	*/

	if err != nil {
		return err
	}

	for _,v := range matches {
		_, isFile, err := t.getInfoIfSingleFile(v)
		if err != nil {
			return err
		}

		if !isFile && !t.recursive {
			continue
		}

		v = cleanLocalPath(v)
		nt := newLocalTraverser(v, t.recursive, t.incrementEnumerationCounter)
		injectedProcessor := func(object storedObject) error {
			relativePath := strings.TrimPrefix(v, t.getBasePath())
			object.relativePath = relativePath

			return processor(object)
		}

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

func newGlobTraverser(fullGlobPath string, recursive bool, incrementEnumerationCounter func()) *localGlobTraverser {
	traverser := localGlobTraverser{
		fullPath: cleanLocalPath(fullGlobPath),
		recursive: recursive,
		incrementEnumerationCounter: incrementEnumerationCounter,
	}

	return &traverser
}

