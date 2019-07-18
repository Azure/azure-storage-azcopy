package cmd

import (
	"bufio"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"net/url"
	"os"
)

// a meta traverser that goes through a list of paths (potentially directory entities) and scans them one by one
// behaves like a single traverser (basically a "traverser of traverser")
type listTraverser struct {
	fileLocation            string
	childTraverserGenerator childTraverserGenerator
}

type childTraverserGenerator func(childPath string) (resourceTraverser, error)

func (l *listTraverser) traverse(processor objectProcessor, filters []objectFilter) (err error) {
	f, err := os.Open(l.fileLocation)
	if err != nil {
		return fmt.Errorf("unable to open %s to retrieve the required list of entities to transfer", l.fileLocation)
	}
	defer f.Close()

	// spawn a scanner to read the list of entities one line at a time
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		childPath := scanner.Text()

		// fetch an appropriate traverser, and go through the child path, which could be
		//   1. a single entity
		//   2. a directory entity that needs to be scanned
		childTraverser, err := l.childTraverserGenerator(childPath)
		if err != nil {
			glcm.Info(fmt.Sprintf("Skipping %s due to error %s", childPath, err))
			continue
		}

		err = childTraverser.traverse(processor, filters)
		if err != nil {
			glcm.Info(fmt.Sprintf("Skipping %s as it cannot be scanned due to error: %s", childPath, err))
		}
	}

	// in case the list of entities was not read properly, we can no longer continue enumeration
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("unable to scan %s to retrieve the required list of entities to transfer", l.fileLocation)
	}

	return nil
}

func (l *listTraverser) setPreProcessingCallback(callback preProcessingCallback) {
	// TODO cannot fulfill this request since listTraverser is a meta-traverser and does not
}

func newListTraverser(parent string, parentSAS string, credential common.CredentialInfo, recursive bool,
	fileLocation string, parentType common.Location) resourceTraverser {
	var traverserGenerator childTraverserGenerator

	// when scanning a child path under the parent, we need to make sure that the relative paths of
	// the results are indeed starting right under the parent
	// ex: parent = /usr/foo
	// case 1: child1 is a file
	//         the relative path returned by the child traverser would be ""
	//         it should be "child1" instead
	// case 2: child2 is a directory, and it has items under it such as child2/grandchild1
	//         the relative path returned by the child traverser would be "grandchild1"
	//         it should be "child2/grandchild1" instead
	getPreProcessingCallback := func(relativeChildPath string) preProcessingCallback {
		return func(object storedObject) storedObject {
			if object.relativePath == "" {
				// if the child is a single file
				object.relativePath = relativeChildPath
			} else {
				object.relativePath = common.GenerateFullPath(relativeChildPath, object.relativePath)
			}

			return object
		}
	}

	// TODO implement for Local, ADLS Gen 2
	switch parentType {
	case common.ELocation.Blob():
		traverserGenerator = func(relativeChildPath string) (resourceTraverser, error) {
			// assume child path is not URL-encoded yet, this is consistent with the behavior of previous implementation
			childURL, _ := url.Parse(parent)
			childURL.Path = common.GenerateFullPath(childURL.Path, relativeChildPath)

			// construct traverser that goes through child
			traverser, err := newBlobTraverserForCopy(childURL.String(), parentSAS, credential, recursive)
			if err != nil {
				return nil, err
			}

			// make sure the returned storedObjects have the right relative path starting at under parent
			traverser.setPreProcessingCallback(getPreProcessingCallback(relativeChildPath))
			return traverser, nil
		}
	case common.ELocation.File():
		traverserGenerator = func(relativeChildPath string) (resourceTraverser, error) {
			// assume child path is not URL-encoded yet, this is consistent with the behavior of previous implementation
			childURL, _ := url.Parse(parent)
			childURL.Path = common.GenerateFullPath(childURL.Path, relativeChildPath)

			// construct traverser that goes through child
			traverser, err := newFileTraverserForCopy(childURL.String(), parentSAS, credential, recursive)
			if err != nil {
				return nil, err
			}

			// make sure the returned storedObjects have the right relative path starting at under parent
			traverser.setPreProcessingCallback(getPreProcessingCallback(relativeChildPath))
			return traverser, nil
		}
	default:
		glcm.Error("List traverser is not implemented for " + parentType.String())
	}

	return &listTraverser{
		fileLocation:            fileLocation,
		childTraverserGenerator: traverserGenerator,
	}
}
