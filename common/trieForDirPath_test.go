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

func TestTrie_GetDirDetails(t *testing.T) {
	trie, a, folderName := setupTest(t)

	trie.SetUnregisteredStatus(folderName, true)
	trie.InsertStatus(folderName, 1)

	status, isUnregisteredButCreated, exists := trie.GetDirDetails(folderName)

	a.True(exists)
	a.Equal(true, isUnregisteredButCreated)
	a.Equal(uint32(1), status)
}
