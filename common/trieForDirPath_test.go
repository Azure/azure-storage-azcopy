package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTrie_NewTrie(t *testing.T) {
	a := assert.New(t)
	trie := NewTrie()
	a.NotNil(trie)
	a.NotNil(trie.Root)
	a.Empty(trie.Root.children)
}

func TestTrie_InsertDirNode(t *testing.T) {
	a := assert.New(t)
	// One Level
	trie := NewTrie()

	n1, created := trie.InsertDirNode("mydir")
	a.NotNil(n1)
	a.True(created)
	a.Len(trie.Root.children, 1)
	a.Contains(trie.Root.children, "mydir")
	a.Empty(trie.Root.children["mydir"].children)

	n2, created := trie.InsertDirNode("mydir")
	a.NotNil(n2)
	a.Equal(n1, n2)
	a.False(created)

	// Multiple Levels
	trie = NewTrie()

	n1, created = trie.InsertDirNode("mydir/mysubdir/lastlevel")
	a.NotNil(n1)
	a.True(created)
	a.Len(trie.Root.children, 1)
	a.Contains(trie.Root.children, "mydir")
	a.Len(trie.Root.children["mydir"].children, 1)
	a.Contains(trie.Root.children["mydir"].children, "mysubdir")
	a.Len(trie.Root.children["mydir"].children["mysubdir"].children, 1)
	a.Contains(trie.Root.children["mydir"].children["mysubdir"].children, "lastlevel")
	a.Empty(trie.Root.children["mydir"].children["mysubdir"].children["lastlevel"].children)
	a.Equal(trie.Root.children["mydir"].children["mysubdir"].children["lastlevel"], n1)

	// Insert in middle
	n2, created = trie.InsertDirNode("mydir/mysubdir")
	a.False(created)
	a.Equal(trie.Root.children["mydir"].children["mysubdir"], n2)

	// Insert a different child
	n3, created := trie.InsertDirNode("mydir/mysubdirsibling")
	a.True(created)

	a.Len(trie.Root.children["mydir"].children, 2)
	a.Contains(trie.Root.children["mydir"].children, "mysubdir")
	a.Contains(trie.Root.children["mydir"].children, "mysubdirsibling")
	a.Empty(trie.Root.children["mydir"].children["mysubdirsibling"].children)
	a.Equal(trie.Root.children["mydir"].children["mysubdirsibling"], n3)

}

func TestTrie_GetDirNode(t *testing.T) {
	a := assert.New(t)
	// One Level
	trie := NewTrie()

	n, ok := trie.GetDirNode("mydir/mysubdir/lastlevel")
	a.Nil(n)
	a.False(ok)

	n1, _ := trie.InsertDirNode("mydir")
	n2, ok := trie.GetDirNode("mydir")
	a.True(ok)
	a.Equal(n1, n2)

	n1, _ = trie.InsertDirNode("mydir/mysubdir/lastlevel")
	n2, _ = trie.InsertDirNode("mydir/mysubdirsibling")
	n3, ok := trie.GetDirNode("mydir")
	a.True(ok)
	a.Equal(trie.Root.children["mydir"], n3)

	n4, ok := trie.GetDirNode("mydir/mysubdir/lastlevel/actuallyiwantthisone")
	a.Nil(n4)
	a.False(ok)

	_, ok = trie.GetDirNode("mydir/mysubdir")
	a.True(ok)
	_, ok = trie.GetDirNode("mydir/mysubdir/lastlevel")
	a.True(ok)
	_, ok = trie.GetDirNode("mydir/mysubdirsibling")
	a.True(ok)
}
