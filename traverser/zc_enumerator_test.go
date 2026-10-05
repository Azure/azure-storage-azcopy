package traverser

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type enumeratorTestTraverser struct {
	err error
}

func (t enumeratorTestTraverser) Traverse(objectMorpher, ObjectProcessor, []ObjectFilter) error {
	return t.err
}

func (enumeratorTestTraverser) IsDirectory(bool) (bool, error) {
	return true, nil
}

func TestSyncEnumeratorAcceptsMissingBlobDestination(t *testing.T) {
	finalized := false
	enumerator := NewSyncEnumerator(
		enumeratorTestTraverser{},
		enumeratorTestTraverser{err: errors.New("blob https://account.blob.core.windows.net/container/prefix not found in destination. Err BlobNotFound")},
		nil,
		nil,
		nil,
		func() error {
			finalized = true
			return nil
		},
	)

	require.NoError(t, enumerator.Enumerate())
	require.True(t, finalized)
}

func TestSyncEnumeratorRejectsUnexpectedDestinationError(t *testing.T) {
	expected := errors.New("destination listing failed")
	enumerator := NewSyncEnumerator(
		enumeratorTestTraverser{},
		enumeratorTestTraverser{err: expected},
		nil,
		nil,
		nil,
		func() error {
			t.Fatal("finalize must not run after an unexpected traversal failure")
			return nil
		},
	)

	require.ErrorIs(t, enumerator.Enumerate(), expected)
}
