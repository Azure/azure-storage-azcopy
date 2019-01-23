package cmd

import (
	"errors"
	"github.com/Azure/azure-storage-azcopy/common"
	"strings"
	"time"
)

// -------------------------------------- Component Definitions -------------------------------------- \\
// the following interfaces and structs allow the sync enumerator
// to be generic and has as little duplicated code as possible

// represent a local or remote resource entity (ex: local file, blob, etc.)
// we can add more properties if needed, as this is easily extensible
type genericEntity struct {
	name             string
	lastModifiedTime time.Time
	size             int64

	// partial path relative to its directory
	// example: dir=/var/a/b/c fullPath=/var/a/b/c/d/e/f.pdf relativePath=d/e/f.pdf
	relativePath string
}

func (entity *genericEntity) isMoreRecentThan(entity2 genericEntity) bool {
	return entity.lastModifiedTime.After(entity2.lastModifiedTime)
}

// capable of traversing a resource, pass each entity to the given entityProcessor if it passes all the filters
type resourceTraverser interface {
	traverse(processor entityProcessor, filters []entityFilter) error
}

// given a genericEntity, process it accordingly
type entityProcessor interface {
	process(entity genericEntity) error
}

// given a genericEntity, verify if it satisfies the defined conditions
type entityFilter interface {
	pass(entity genericEntity) bool
}

// -------------------------------------- Generic Enumerator -------------------------------------- \\

type syncEnumerator struct {
	// these allow us to go through the source and destination
	sourceTraverser      resourceTraverser
	destinationTraverser resourceTraverser

	// filters apply to both the source and destination
	filters []entityFilter

	// the processor responsible for scheduling copy transfers
	copyTransferScheduler entityProcessor

	// the processor responsible for scheduling delete transfers
	deleteTransferScheduler entityProcessor

	// a finalizer that is always called if the enumeration finishes properly
	finalize func() error
}

func (e *syncEnumerator) enumerate() (err error) {
	destinationIndexer := newDestinationIndexer()

	// enumerate the destination and build lookup map
	err = e.destinationTraverser.traverse(destinationIndexer, e.filters)
	if err != nil {
		return
	}

	// add the destinationIndexer as an extra filter to the list
	e.filters = append(e.filters, destinationIndexer)

	// enumerate the source and schedule transfers
	err = e.sourceTraverser.traverse(e.copyTransferScheduler, e.filters)
	if err != nil {
		return
	}

	// delete extra files at the destination if needed
	err = destinationIndexer.traverse(e.deleteTransferScheduler, nil)
	if err != nil {
		return
	}

	// execute the finalize func which may perform useful clean up steps
	err = e.finalize()
	if err != nil {
		return
	}

	return
}

// the destinationIndexer implements both entityProcessor, entityFilter, and resourceTraverser
// it is essential for the generic enumerator to work
// it can:
// 		1. accumulate a lookup map with given destination entities
// 		2. serve as a filter to check whether a given entity is the lookup map
//		3. go through the entities in the map like a traverser
type destinationIndexer struct {
	indexMap map[string]genericEntity
}

func newDestinationIndexer() *destinationIndexer {
	indexer := destinationIndexer{}
	indexer.indexMap = make(map[string]genericEntity)

	return &indexer
}

func (i *destinationIndexer) process(entity genericEntity) (err error) {
	i.indexMap[entity.relativePath] = entity
	return
}

// it will only pass items that are:
//	1. not present in the map
//  2. present but is more recent than the entry in the map
// note: we remove the entity if it is present
func (i *destinationIndexer) pass(entity genericEntity) bool {
	entityInMap, present := i.indexMap[entity.relativePath]

	// if the given entity is more recent, we let it pass
	if present {
		defer delete(i.indexMap, entity.relativePath)

		if entity.isMoreRecentThan(entityInMap) {
			return true
		}
	} else if !present {
		return true
	}

	return false
}

// go through the entities in the map to process them
func (i *destinationIndexer) traverse(processor entityProcessor, filters []entityFilter) (err error) {
	for _, value := range i.indexMap {
		if !passedFilters(filters, value) {
			continue
		}

		err = processor.process(value)
		if err != nil {
			return
		}
	}
	return
}

// -------------------------------------- Helper Funcs -------------------------------------- \\

func passedFilters(filters []entityFilter, entity genericEntity) bool {
	if filters != nil && len(filters) > 0 {
		// loop through the filters, if any of them fail, then return false
		for _, filter := range filters {
			if !filter.pass(entity) {
				return false
			}
		}
	}

	return true
}

func processIfPassedFilters(filters []entityFilter, entity genericEntity, processor entityProcessor) (err error) {
	if passedFilters(filters, entity) {
		err = processor.process(entity)
	}

	return
}

// entity names are useful for filters
func getEntityNameOnly(fullPath string) (nameOnly string) {
	lastPathSeparator := strings.LastIndex(fullPath, common.AZCOPY_PATH_SEPARATOR_STRING)

	// if there is a path separator and it is not the last character
	if lastPathSeparator > 0 && lastPathSeparator != len(fullPath)-1 {
		// then we separate out the name of the entity
		nameOnly = fullPath[lastPathSeparator+1:]
	} else {
		nameOnly = fullPath
	}

	return
}

// -------------------------------------- Implemented Enumerators -------------------------------------- \\

func newSyncDownloadEnumerator(cca *cookedSyncCmdArgs) (enumerator *syncEnumerator, err error) {
	sourceTraverser, err := newBlobTraverser(cca, true)
	if err != nil {
		// this is unexpected
		// if there is an error here, the URL was probably not valid
		return nil, err
	}

	destinationTraverser := newLocalTraverser(cca, false)
	deleteScheduler := newSyncLocalDeleteProcessor(cca, false)
	transferScheduler := newSyncTransferProcessor(cca, NumOfFilesPerDispatchJobPart)

	_, isSingleBlob := sourceTraverser.getPropertiesIfSingleBlob()
	_, isSingleFile, _ := destinationTraverser.getInfoIfSingleFile()
	if isSingleBlob != isSingleFile {
		return nil, errors.New("sync must happen between source and destination of the same type: either blob <-> file, or container/virtual directory <-> local directory")
	}

	finalize := func() error {
		jobInitiated, err := transferScheduler.dispatchFinalPart()
		if err != nil {
			return err
		}

		if !jobInitiated && !deleteScheduler.wasAnyFileDeleted() {
			return errors.New("the source and destination are already in sync")
		}

		cca.setScanningComplete()
		return nil
	}

	includeFilters := buildIncludeFilters(cca.include)
	excludeFilters := buildExcludeFilters(cca.exclude)

	// trigger the progress reporting
	cca.waitUntilJobCompletion(false)

	return &syncEnumerator{
		sourceTraverser:         sourceTraverser,
		destinationTraverser:    destinationTraverser,
		copyTransferScheduler:   transferScheduler,
		deleteTransferScheduler: deleteScheduler,
		finalize:                finalize,
		filters:                 append(includeFilters, excludeFilters...),
	}, nil
}
