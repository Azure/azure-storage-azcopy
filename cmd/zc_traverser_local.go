package cmd

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type localTraverser struct {
	fullPath  string
	recursive bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func (t *localTraverser) traverse(processor objectProcessor, filters []objectFilter) (err error) {
	singleFileInfo, isSingleFile, err := t.getInfoIfSingleFile()

	if err != nil {
		return fmt.Errorf("cannot scan the path %s, please verify that it is a valid", t.fullPath)
	}

	// if the path is a single file, then pass it through the filters and send to processor
	if isSingleFile {
		t.incrementEnumerationCounter()
		err = processIfPassedFilters(filters, newStoredObject(singleFileInfo.Name(),
			"", // relative path makes no sense when the full path already points to the file
			singleFileInfo.ModTime(), singleFileInfo.Size(), nil), processor)
		return

	} else {
		if t.recursive {
			err = filepath.Walk(t.fullPath, func(filePath string, fileInfo os.FileInfo, fileError error) error {
				if fileError != nil {
					return fileError
				}

				// skip the subdirectories
				if fileInfo.IsDir() {
					return nil
				}

				t.incrementEnumerationCounter()
				return processIfPassedFilters(filters, newStoredObject(fileInfo.Name(),
					strings.Replace(filePath, t.fullPath+common.AZCOPY_PATH_SEPARATOR_STRING, "", 1),
					fileInfo.ModTime(),
					fileInfo.Size(), nil), processor)
			})

			return
		} else {
			// if recursive is off, we only need to scan the files immediately under the fullPath
			files, err := ioutil.ReadDir(t.fullPath)
			if err != nil {
				return err
			}

			// go through the files and return if any of them fail to process
			for _, singleFile := range files {
				if singleFile.IsDir() {
					continue
				}

				t.incrementEnumerationCounter()
				err = processIfPassedFilters(filters, newStoredObject(singleFile.Name(), singleFile.Name(), singleFile.ModTime(), singleFile.Size(), nil), processor)

				if err != nil {
					return err
				}
			}
		}
	}

	return
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

func newLocalTraverser(fullPath string, recursive bool, incrementEnumerationCounter func()) *localTraverser {
	traverser := localTraverser{
		fullPath:                    fullPath,
		recursive:                   recursive,
		incrementEnumerationCounter: incrementEnumerationCounter}
	return &traverser
}
