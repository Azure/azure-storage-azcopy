// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package traverser

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestHashIndexer_StoreAndLookup(t *testing.T) {
	a := assert.New(t)
	indexer := NewHashIndexer()

	md5Hash := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	obj := StoredObject{
		Name:         "file1.txt",
		RelativePath: "dir/file1.txt",
		Size:         1024,
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	}

	indexer.Store(obj)

	// Should be able to look up by the same hash
	found, ok := indexer.Lookup(md5Hash)
	a.True(ok)
	a.Equal("dir/file1.txt", found.RelativePath)
	a.Equal(int64(1024), found.Size)

	// Should have 1 indexed entry
	a.Equal(1, indexer.IndexedCount())
	a.Equal(int64(0), indexer.MissingHashCount())
}

func TestHashIndexer_LookupNotFound(t *testing.T) {
	a := assert.New(t)
	indexer := NewHashIndexer()

	md5Hash := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	differentHash := []byte{0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8, 0xf7, 0xf6, 0xf5, 0xf4, 0xf3, 0xf2, 0xf1, 0xf0}

	obj := StoredObject{
		Name:         "file1.txt",
		RelativePath: "dir/file1.txt",
		Size:         1024,
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	}

	indexer.Store(obj)

	// Should not find a different hash
	_, ok := indexer.Lookup(differentHash)
	a.False(ok)
}

func TestHashIndexer_NilHashSkipped(t *testing.T) {
	a := assert.New(t)
	indexer := NewHashIndexer()

	// Object with no MD5 should be skipped and counted as missing
	obj := StoredObject{
		Name:         "file1.txt",
		RelativePath: "dir/file1.txt",
		Size:         1024,
		Md5:          nil,
		EntityType:   common.EEntityType.File(),
	}

	indexer.Store(obj)

	a.Equal(0, indexer.IndexedCount())
	a.Equal(int64(1), indexer.MissingHashCount())
}

func TestHashIndexer_EmptyHashSkipped(t *testing.T) {
	a := assert.New(t)
	indexer := NewHashIndexer()

	// Object with empty MD5 slice should be skipped
	obj := StoredObject{
		Name:         "file1.txt",
		RelativePath: "dir/file1.txt",
		Size:         1024,
		Md5:          []byte{},
		EntityType:   common.EEntityType.File(),
	}

	indexer.Store(obj)

	a.Equal(0, indexer.IndexedCount())
	a.Equal(int64(1), indexer.MissingHashCount())
}

func TestHashIndexer_LookupNilHash(t *testing.T) {
	a := assert.New(t)
	indexer := NewHashIndexer()

	// Lookup with nil hash should return not found
	_, ok := indexer.Lookup(nil)
	a.False(ok)

	// Lookup with empty hash should return not found
	_, ok = indexer.Lookup([]byte{})
	a.False(ok)
}

func TestHashIndexer_FirstObjectWins(t *testing.T) {
	a := assert.New(t)
	indexer := NewHashIndexer()

	md5Hash := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	// Store two objects with the same hash - first should win
	obj1 := StoredObject{
		Name:         "file1.txt",
		RelativePath: "dir/file1.txt",
		Size:         1024,
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	}
	obj2 := StoredObject{
		Name:         "file2.txt",
		RelativePath: "other/file2.txt",
		Size:         1024,
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	}

	indexer.Store(obj1)
	indexer.Store(obj2)

	// Should return the first object stored
	found, ok := indexer.Lookup(md5Hash)
	a.True(ok)
	a.Equal("dir/file1.txt", found.RelativePath)

	// Only one unique hash indexed
	a.Equal(1, indexer.IndexedCount())
}

func TestHashIndexer_MultipleUniqueHashes(t *testing.T) {
	a := assert.New(t)
	indexer := NewHashIndexer()

	hash1 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	hash2 := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}
	hash3 := []byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30}

	indexer.Store(StoredObject{RelativePath: "a.txt", Md5: hash1, EntityType: common.EEntityType.File()})
	indexer.Store(StoredObject{RelativePath: "b.txt", Md5: hash2, EntityType: common.EEntityType.File()})
	indexer.Store(StoredObject{RelativePath: "c.txt", Md5: hash3, EntityType: common.EEntityType.File()})
	indexer.Store(StoredObject{RelativePath: "d.txt", Md5: nil, EntityType: common.EEntityType.File()}) // no hash

	a.Equal(3, indexer.IndexedCount())
	a.Equal(int64(1), indexer.MissingHashCount())

	found1, ok1 := indexer.Lookup(hash1)
	a.True(ok1)
	a.Equal("a.txt", found1.RelativePath)

	found2, ok2 := indexer.Lookup(hash2)
	a.True(ok2)
	a.Equal("b.txt", found2.RelativePath)

	found3, ok3 := indexer.Lookup(hash3)
	a.True(ok3)
	a.Equal("c.txt", found3.RelativePath)
}

func TestObjectIndexer_OnStoreHook(t *testing.T) {
	a := assert.New(t)
	indexer := NewObjectIndexer()

	var hookedObjects []StoredObject
	indexer.OnStore = func(obj StoredObject) {
		hookedObjects = append(hookedObjects, obj)
	}

	obj1 := StoredObject{RelativePath: "file1.txt", EntityType: common.EEntityType.File()}
	obj2 := StoredObject{RelativePath: "file2.txt", EntityType: common.EEntityType.File()}

	_ = indexer.Store(obj1)
	_ = indexer.Store(obj2)

	a.Equal(2, len(hookedObjects))
	a.Equal("file1.txt", hookedObjects[0].RelativePath)
	a.Equal("file2.txt", hookedObjects[1].RelativePath)
}

func TestObjectIndexer_OnStoreHookNil(t *testing.T) {
	a := assert.New(t)
	indexer := NewObjectIndexer()

	// Should not panic when OnStore is nil
	obj := StoredObject{RelativePath: "file1.txt", EntityType: common.EEntityType.File()}
	err := indexer.Store(obj)
	a.NoError(err)
	a.Equal(1, len(indexer.IndexMap))
}
