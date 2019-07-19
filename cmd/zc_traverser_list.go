package cmd

import (
	"bufio"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"io"
	"net/url"
)

// a meta traverser that goes through a list of paths (potentially directory entities) and scans them one by one
// behaves like a single traverser (basically a "traverser of traverser")
type listTraverser struct {
	listReader              io.ReadCloser
	childTraverserGenerator childTraverserGenerator
}

type childTraverserGenerator func(childPath string) (resourceTraverser, error)

func (l *listTraverser) traverse(processor objectProcessor, filters []objectFilter) (err error) {
	// spawn a scanner to read the list of entities one line at a time
	scanner := bufio.NewScanner(l.listReader)
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

		// when scanning a child path under the parent, we need to make sure that the relative paths of
		// the results are indeed starting right under the parent
		// ex: parent = /usr/foo
		// case 1: child1 is a file under the parent
		//         the relative path returned by the child traverser would be ""
		//         it should be "child1" instead
		// case 2: child2 is a directory, and it has items under it such as child2/grandchild1
		//         the relative path returned by the child traverser would be "grandchild1"
		//         it should be "child2/grandchild1" instead
		preProcessor := func(object storedObject) error {
			if object.relativePath == "" {
				// if the child is a single file
				object.relativePath = childPath
			} else {
				object.relativePath = common.GenerateFullPath(childPath, object.relativePath)
			}

			return processor(object)
		}

		err = childTraverser.traverse(preProcessor, filters)
		if err != nil {
			glcm.Info(fmt.Sprintf("Skipping %s as it cannot be scanned due to error: %s", childPath, err))
		}
	}

	// in case the list of entities was not read properly, we can no longer continue enumeration
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("unable to scan the required list of entities due to error: %s", err)
	}

	// close the reader before returning
	return l.listReader.Close()
}

func newListTraverser(parent string, parentSAS string, parentType common.Location, credential common.CredentialInfo,
	recursive bool, listReader io.ReadCloser) resourceTraverser {
	var traverserGenerator childTraverserGenerator

	traverserGenerator = func(relativeChildPath string) (resourceTraverser, error) {
		// assume child path is not URL-encoded yet, this is consistent with the behavior of previous implementation
		childURL, _ := url.Parse(parent)
		childURL.Path = common.GenerateFullPath(childURL.Path, relativeChildPath)

		// construct traverser that goes through child
		traverser, err := newTraverserForCopy(childURL.String(), parentSAS, parentType, credential, recursive)
		if err != nil {
			return nil, err
		}
		return traverser, nil
	}

	return &listTraverser{
		listReader:              listReader,
		childTraverserGenerator: traverserGenerator,
	}
}
