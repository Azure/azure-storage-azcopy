package cmd

// with the help of an objectIndexer containing the source objects
// filter out the destination objects that should be transferred
type syncDestinationFilter struct {
	// the rejected objects would be passed to the recyclers
	recyclers objectProcessor

	// storing the source objects
	i *objectIndexer
}

func newSyncDestinationFilter(i *objectIndexer, recyclers objectProcessor) objectFilter {
	return &syncDestinationFilter{i: i, recyclers: recyclers}
}

// it will only pass destination objects that are present in the indexer but stale compared to the entry in the map
// if the destinationObject is not present at all, it will be passed to the recyclers
// ex: we already know what the source contains, now we are looking at objects at the destination
// if file x from the destination exists at the source, then we'd only transfer it if it is considered stale compared to its counterpart at the source
// if file x does not exist at the source, then it is considered extra, and will be deleted
func (f *syncDestinationFilter) doesPass(destinationObject storedObject) bool {
	storedObjectInMap, present := f.i.indexMap[destinationObject.relativePath]

	// if the destinationObject is present and stale, we let it pass
	if present {
		defer delete(f.i.indexMap, destinationObject.relativePath)

		if storedObjectInMap.isMoreRecentThan(destinationObject) {
			return true
		}

		return false
	}

	// purposefully ignore the error from recyclers
	// it's a tolerable error, since it just means some extra destination object might hang around a bit longer
	_ = f.recyclers(destinationObject)
	return false
}

// with the help of an objectIndexer containing the destination objects
// filter out the source objects that should be transferred
type syncSourceFilter struct {

	// storing the destination objects
	i *objectIndexer
}

func newSyncSourceFilter(i *objectIndexer) objectFilter {
	return &syncSourceFilter{i: i}
}

// it will only pass items that are:
//	1. not present in the map
//  2. present but is more recent than the entry in the map
// note: we remove the storedObject if it is present
func (f *syncSourceFilter) doesPass(sourceObject storedObject) bool {
	storedObjectInMap, present := f.i.indexMap[sourceObject.relativePath]

	// if the sourceObject is more recent, we let it pass
	if present {
		defer delete(f.i.indexMap, sourceObject.relativePath)

		if sourceObject.isMoreRecentThan(storedObjectInMap) {
			return true
		}

		return false
	}

	return true
}
