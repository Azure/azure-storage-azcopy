package common

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func GenerateRandomFolder() string {
	return fmt.Sprintf("folder/subfolder%d", rand.Intn(100000))
}

func setupTest(t *testing.T) (*Trie, *assert.Assertions, string) {
	trie := NewTrie()
	a := assert.New(t)
	folderName := GenerateRandomFolder()
	return trie, a, folderName
}

func TestTrie_InsertAndGet(t *testing.T) {
	trie, a, folderName := setupTest(t)
	trie.Insert(folderName, 1)

	value, exists := trie.Get(folderName)
	a.True(exists)
	a.Equal(1, value)
}

func TestTrie_GetNonExistent(t *testing.T) {
	trie, a, folderName := setupTest(t)

	_, exists := trie.Get(folderName)
	a.False(exists)
}

func TestTrie_Delete(t *testing.T) {
	trie, a, folderName := setupTest(t)

	trie.Insert(folderName, 1)
	trie.Delete(folderName)

	_, exists := trie.Get(folderName)
	a.False(exists)
}

func TestTrie_DeletePartialPath(t *testing.T) {
	trie, a, folderName := setupTest(t)
	trie.Insert(folderName, 1)
	deleted := trie.Delete("folder")
	a.False(deleted)
}
