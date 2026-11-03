package cmd

import (
	"math/rand"
	"slices"
	"testing"
	"unicode/utf16"

	"github.com/stretchr/testify/require"
)

func referenceBlobNameCompare(a, b string) int {
	return slices.Compare(utf16.Encode([]rune(a)), utf16.Encode([]rune(b)))
}

func TestCompareBlobNames(t *testing.T) {
	names := []string{
		"", "/", "a", "a/", "a-1", "a//b", "a/../b", "\u00e9", "e\u0301",
		"leaf/x\ufeffy.txt", "leaf/x\U0001f600y.txt",
		"chars/#\ufeff/#\ufeff/f.txt", "chars/#\U0001f600/f.txt",
	}
	for _, r := range []rune{0x7f, 0x80, 0x7ff, 0x800, 0xd7ff, 0xe000, 0xfeff, 0xffff, 0x10000, 0x1f600, 0x10ffff} {
		names = append(names, string(r), "prefix/"+string(r)+"/tail")
	}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 100; i++ {
		var runes []rune
		for j := 0; j < 5; j++ {
			r := rune(rng.Intn(0x110000))
			if r >= 0xd800 && r <= 0xdfff {
				r = 'x'
			}
			runes = append(runes, r)
		}
		names = append(names, string(runes))
	}
	for _, a := range names {
		for _, b := range names {
			require.Equal(t, referenceBlobNameCompare(a, b), compareBlobNames(a, b), "%q vs %q", a, b)
		}
	}
}

func TestFlatListRequestBoundsUnicode(t *testing.T) {
	start, end, empty := flatListRequestBounds("", "x\U0001f600", flatListRange{"x\U0001f600", "x\ufeff"})
	require.False(t, empty)
	require.Equal(t, "x\U0001f600", start)
	require.Empty(t, end, "the prefix is tighter than the upper bound")

	_, _, empty = flatListRequestBounds("", "x\ufeff", flatListRange{"x\U00010000", "x\U0001f601"})
	require.True(t, empty, "the entire supplementary range precedes the high-BMP prefix")
}
