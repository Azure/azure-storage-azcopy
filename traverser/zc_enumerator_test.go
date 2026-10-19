package traverser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStripCompressionExtension_ZStdCases(t *testing.T) {
	tests := []struct {
		name            string
		destinationName string
		contentEncoding string
		expected        string
	}{
		{
			name:            "zstd strips lowercase zst",
			destinationName: "file.zst",
			contentEncoding: "zstd",
			expected:        "file",
		},
		{
			name:            "zstd strips lowercase zstd",
			destinationName: "file.zstd",
			contentEncoding: "zstd",
			expected:        "file",
		},
		{
			name:            "zstd strips mixed case extension",
			destinationName: "file.ZsTd",
			contentEncoding: "zstd",
			expected:        "file",
		},
		{
			name:            "zstd strips with mixed case encoding",
			destinationName: "file.zst",
			contentEncoding: "ZsTd",
			expected:        "file",
		},
		{
			name:            "zstd strips multi extension tar zst",
			destinationName: "file.tar.zst",
			contentEncoding: "zstd",
			expected:        "file.tar",
		},
		{
			name:            "zstd does not strip unrelated extension",
			destinationName: "file.tar.gz",
			contentEncoding: "zstd",
			expected:        "file.tar.gz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			a.Equal(tt.expected, stripCompressionExtension(tt.destinationName, tt.contentEncoding))
		})
	}
}
