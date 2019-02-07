package cmd

// the objectIndexer is essential for the generic sync enumerator to work
// it can serve as a:
// 		1. objectProcessor: accumulate a lookup map with given storedObjects
//		2. resourceTraverser: go through the entities in the map like a traverser
type objectIndexer struct {
	indexMap map[string]storedObject
}

func newObjectIndexer() *objectIndexer {
	indexer := objectIndexer{}
	indexer.indexMap = make(map[string]storedObject)

	return &indexer
}

// process the given stored object by indexing it using its relative path
func (i *objectIndexer) store(storedObject storedObject) (err error) {
	i.indexMap[storedObject.relativePath] = storedObject
	return
}

// go through the remaining stored objects in the map to process them
func (i *objectIndexer) traverse(processor objectProcessor, filters []objectFilter) (err error) {
	for _, value := range i.indexMap {
		err = processIfPassedFilters(filters, value, processor)
		if err != nil {
			return
		}
	}
	return
}
