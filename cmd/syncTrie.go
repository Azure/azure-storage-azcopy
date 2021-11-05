package cmd

type objectTrie struct {
	// nil on the root.
	parent *objectTrie
	char   rune
	leaves map[rune]*objectTrie
	items  map[*objectTrie]bool // TODO: Should we bother trying to remove this from memory?
	// for sync, this will either be just a raw LMT (*time.Time, for the destination) or a full storedObject for the source.
	data interface{}
	// For intentionally present but nil entries, so we don't delete them in a recursivedelete call.
	presentButNil bool
}

// parent can be nil on the root.
func newObjectTrie(parent *objectTrie) *objectTrie {
	out := &objectTrie{
		parent: parent,
		leaves: make(map[rune]*objectTrie),
	}

	if parent == nil {
		out.items = make(map[*objectTrie]bool)
	}

	return out
}

func (o *objectTrie) PutObject(path string, data interface{}) {
	tidx := o                // trie index
	for _, v := range path { // iterate over each rune, adding it to the trie
		if newTIDX, ok := tidx.leaves[v]; !ok { // do not erase existing paths.
			tidx.leaves[v] = newObjectTrie(tidx)
			tidx = tidx.leaves[v]
			tidx.char = v
		} else {
			tidx = newTIDX
		}
	}

	// set the data
	tidx.data = data
	tidx.presentButNil = data == nil
	// add the index to the root
	o.items[tidx] = true
}

func (o *objectTrie) GetObject(path string) (data interface{}, present bool) {
	tidx := o                // trie index
	for _, v := range path { // iterate over each rune, do not add it if it's not present.
		var ok bool
		if tidx, ok = tidx.leaves[v]; !ok { // Only continue traversing if the path is actually present.
			return nil, false
		}
	}

	// return the data; include present because even nil can be present
	return tidx.data, tidx.data != nil || tidx.presentButNil
}

func (o *objectTrie) DeleteRecursive(path string) {
	tidx := o // trie index
	completedSearch := true
	for _, v := range path { // Find the end of the path.
		var ok bool
		if tidx, ok = tidx.leaves[v]; !ok { // Only continue traversing if the path is actually present.
			completedSearch = false
			break // Start deletion even if we didn't find the exact index-- there _may_ be orphans for some reason.
		}
	}

	if completedSearch {
		// Delete the index from the root
		delete(o.items, tidx)
	}

	// start working backwards until we hit the root or something we can't delete.
	for tidx != o && tidx.parent.Deletable() {
		tidx = tidx.parent
	}

	// snip the entire branch
	tidx.data = nil
	tidx.presentButNil = false
	if tidx.Deletable() {
		toRemove := tidx.char
		tidx = tidx.parent
		delete(tidx.leaves, toRemove)
	}
}

// Deletable denotes when a leaf on the trie is effectively pointless to keep around.
// It has no children, no data, and it is not intentionally nil.
func (o *objectTrie) Deletable() bool {
	return o.data == nil && !o.presentButNil && len(o.leaves) == 0
}

func (o *objectTrie) GetName() string {
	stack := ""
	tidx := o
	for tidx.parent != nil {
		stack = string(tidx.char) + stack // reconstructing backwards.
		tidx = tidx.parent
	}

	return stack
}

type trieIndex struct {
	path string
	data interface{}
}

func (o *objectTrie) GetIndexes() chan trieIndex {
	out := make(chan trieIndex, 30) // Buffered channel, because indexing will take some time in larger workloads, and we don't want to buffer literally every index, bypassing the need for a trie in the first place.

	go func() {
		for k, _ := range o.items {
			out <- trieIndex{
				path: k.GetName(),
				data: k.data,
			}
		}

		close(out)
	}()

	return out
}
