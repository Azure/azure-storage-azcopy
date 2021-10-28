// Copyright © 2019 Microsoft <wastore@microsoft.com>
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
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"net/url"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// a meta traverser that goes through a list of paths (potentially directory entities) and scans them one by one
// behaves like a single traverser (basically a "traverser of traverser")
type listTraverser struct {
	listReader              chan string
	recursive               bool
	childTraverserGenerator childTraverserGenerator
}

type childTraverserGenerator func(childPath string) (ResourceTraverser, error)

// There is no impact to a list traverser returning false because a list traverser points directly to relative paths.
func (l *listTraverser) IsDirectory(bool) bool {
	return false
}

// To kill the traverser, close() the channel under it.
// Behavior demonstrated: https://play.golang.org/p/OYdvLmNWgwO
func (l *listTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	// read a channel until it closes to get a list of objects
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

		// listTraverser will only ever execute on the source
		if !l.recursive && childTraverser.IsDirectory(true) {
			continue // skip over directories
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
		childPreProcessor := func(object *StoredObject) {
			object.relativePath = common.GenerateFullPath(childPath, object.relativePath)
		}
		preProcessorForThisChild := preprocessor.FollowedBy(childPreProcessor)

		err = childTraverser.Traverse(preProcessorForThisChild, processor, filters)
		if err != nil {
			glcm.Info(fmt.Sprintf("Skipping %s as it cannot be scanned due to error: %s", childPath, err))
		}
	}

	return nil
}

func newListTraverser(parent common.ResourceString, parentType common.Location, credential *common.CredentialInfo,
	ctx *context.Context, recursive, followSymlinks, getProperties bool, listChan chan string,
	includeDirectoryStubs bool, incrementEnumerationCounter enumerationCounterFunc, s2sPreserveBlobTags bool,
	logLevel pipeline.LogLevel, cpkOptions common.CpkOptions) ResourceTraverser {
	var traverserGenerator childTraverserGenerator

	traverserGenerator = func(relativeChildPath string) (ResourceTraverser, error) {
		source := parent.Clone()
		if parentType != common.ELocation.Local() {
			// assume child path is not URL-encoded yet, this is consistent with the behavior of previous implementation
			childURL, _ := url.Parse(parent.Value)
			childURL.Path = common.GenerateFullPath(childURL.Path, relativeChildPath)
			source.Value = childURL.String()
		} else {
			// is local, only generate the full path
			source.Value = common.GenerateFullPath(parent.ValueLocal(), relativeChildPath)
		}

		// Construct a traverser that goes through the child
		traverser, err := InitResourceTraverser(source, parentType, ctx, credential, &followSymlinks,
			nil, recursive, getProperties, includeDirectoryStubs, incrementEnumerationCounter,
			nil, s2sPreserveBlobTags, logLevel, cpkOptions)
		if err != nil {
			return nil, err
		}
		return traverser, nil
	}

	return &listTraverser{
		listReader:              listChan,
		recursive:               recursive,
		childTraverserGenerator: traverserGenerator,
	}
}
