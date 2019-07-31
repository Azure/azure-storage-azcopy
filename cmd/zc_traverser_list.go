package cmd

import (
	"context"
	"fmt"
	"net/url"

	"github.com/Azure/azure-storage-azcopy/common"
)

// a meta traverser that goes through a list of paths (potentially directory entities) and scans them one by one
// behaves like a single traverser (basically a "traverser of traverser")
type listTraverser struct {
	listReader              chan string
	childTraverserGenerator childTraverserGenerator
}

type childTraverserGenerator func(childPath string) (resourceTraverser, error)

func (l *listTraverser) isDirectory(isDest bool) bool {
	return false
}

// To kill the traverser, close() the channel under it.
// Behavior demonstrated: https://play.golang.org/p/OYdvLmNWgwO
func (l *listTraverser) traverse(processor objectProcessor, filters []objectFilter) (err error) {
	// spawn a scanner to read the list of entities one line at a time
	childPath, ok := <-l.listReader
	for ; ok; childPath, ok = <-l.listReader {

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
			object.relativePath = common.GenerateFullPath(childPath, object.relativePath)
			return processor(object)
		}

		err = childTraverser.traverse(preProcessor, filters)
		if err != nil {
			glcm.Info(fmt.Sprintf("Skipping %s as it cannot be scanned due to error: %s", childPath, err))
		}
	}

	// close the reader before returning
	return nil
}

func newListTraverser(parent string, parentSAS string, parentType common.Location, credential *common.CredentialInfo, ctx *context.Context,
	recursive bool, listChan chan string) resourceTraverser {
	var traverserGenerator childTraverserGenerator

	traverserGenerator = func(relativeChildPath string) (resourceTraverser, error) {
		source := ""
		if parentType != common.ELocation.Local() {
			// assume child path is not URL-encoded yet, this is consistent with the behavior of previous implementation
			childURL, _ := url.Parse(parent)
			childURL.Path = common.GenerateFullPath(childURL.Path, relativeChildPath)

			// construct traverser that goes through child
			source = copyHandlerUtil{}.appendQueryParamToUrl(childURL, parentSAS).String()
		} else {
			// is local, only generate the full path
			source = common.GenerateFullPath(parent, relativeChildPath)
		}

		traverser, err := initResourceTraverser(source, parentType, ctx, credential, nil, nil, recursive, func() {})
		if err != nil {
			return nil, err
		}
		return traverser, nil
	}

	return &listTraverser{
		listReader:              listChan,
		childTraverserGenerator: traverserGenerator,
	}
}
