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
	trie.InsertStatus(folderName, 1)

	value, exists := trie.GetStatus(folderName)
	a.True(exists)
	a.Equal(uint32(1), value)
}

func TestTrie_GetNonExistent(t *testing.T) {
	trie, a, folderName := setupTest(t)

	_, exists := trie.GetStatus(folderName)
	a.False(exists)
}

func TestTrie_InsertAndCheck_UnregisteredButCreatedStatus(t *testing.T) {
	trie, a, folderName := setupTest(t)

	isUnregisteredButCreated, exists := trie.CheckIfUnregisteredButCreated(folderName)
	a.False(exists)

	trie.InsertUnregisteredStatus(folderName, true)

	isUnregisteredButCreated, exists = trie.CheckIfUnregisteredButCreated(folderName)
	a.True(exists)
	a.Equal(true, isUnregisteredButCreated)
}
