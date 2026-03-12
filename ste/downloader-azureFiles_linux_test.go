//go:build linux
// +build linux

package ste

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// sourceRootJptm wraps testJobPartTransferManager and overrides GetSourceRoot
// so that computeDownloadHardlinkTarget can be tested without a live job.
type sourceRootJptm struct {
	*testJobPartTransferManager
	sourceRoot string
}

func (s *sourceRootJptm) GetSourceRoot() string {
	return s.sourceRoot
}

func newJptmWithRoot(root string) *sourceRootJptm {
	return &sourceRootJptm{
		testJobPartTransferManager: &testJobPartTransferManager{},
		sourceRoot:                 root,
	}
}

func TestComputeDownloadHardlinkTarget(t *testing.T) {
	tests := []struct {
		name                   string
		sourceRootURL          string
		srcFilePath            string
		destination            string
		targetHardlinkFilePath string
		wantResult             string
		wantErrContains        string
	}{
		{
			// File directly inside the traversal root; anchor is a sibling.
			name:                   "file at root level",
			sourceRootURL:          "https://account.file.core.windows.net/share/srcdir",
			srcFilePath:            "srcdir/link.txt",
			destination:            "/local/dstdir/link.txt",
			targetHardlinkFilePath: "anchor.txt",
			wantResult:             "/local/dstdir/anchor.txt",
		},
		{
			// File nested one directory deep inside the traversal root.
			name:                   "file in subdirectory",
			sourceRootURL:          "https://account.file.core.windows.net/share/srcdir",
			srcFilePath:            "srcdir/subdir/link.txt",
			destination:            "/local/dstdir/subdir/link.txt",
			targetHardlinkFilePath: "subdir/anchor.txt",
			wantResult:             "/local/dstdir/subdir/anchor.txt",
		},
		{
			// Anchor lives in a different subdirectory than the link.
			name:                   "anchor in sibling directory",
			sourceRootURL:          "https://account.file.core.windows.net/share/srcdir",
			srcFilePath:            "srcdir/a/link.txt",
			destination:            "/local/dstdir/a/link.txt",
			targetHardlinkFilePath: "b/anchor.txt",
			wantResult:             "/local/dstdir/b/anchor.txt",
		},
		{
			// StripTopDir: the source root already includes the trailing directory name,
			// so the relative path is just the file name with no extra prefix.
			name:                   "StripTopDir – file is directly rooted",
			sourceRootURL:          "https://account.file.core.windows.net/share/srcdir",
			srcFilePath:            "srcdir/link.txt",
			destination:            "/local/link.txt",
			targetHardlinkFilePath: "anchor.txt",
			wantResult:             "/local/anchor.txt",
		},
		{
			// Source root points to the share root (empty path component).
			name:                   "share-root source, file in subdirectory",
			sourceRootURL:          "https://account.file.core.windows.net/share",
			srcFilePath:            "subdir/link.txt",
			destination:            "/local/subdir/link.txt",
			targetHardlinkFilePath: "subdir/anchor.txt",
			wantResult:             "/local/subdir/anchor.txt",
		},
		{
			// Deeply nested source and anchor paths.
			name:                   "deeply nested paths",
			sourceRootURL:          "https://account.file.core.windows.net/share/root",
			srcFilePath:            "root/a/b/c/link.txt",
			destination:            "/mnt/dst/a/b/c/link.txt",
			targetHardlinkFilePath: "a/b/c/anchor.txt",
			wantResult:             "/mnt/dst/a/b/c/anchor.txt",
		},
		{
			// Malformed URL should return an error.
			name:                   "invalid source root URL",
			sourceRootURL:          "://bad-url",
			srcFilePath:            "srcdir/link.txt",
			destination:            "/local/dstdir/link.txt",
			targetHardlinkFilePath: "anchor.txt",
			wantErrContains:        "parsing source root URL",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info := &TransferInfo{
				SrcFilePath:            tc.srcFilePath,
				Destination:            tc.destination,
				TargetHardlinkFilePath: tc.targetHardlinkFilePath,
			}
			jptm := newJptmWithRoot(tc.sourceRootURL)

			got, err := computeDownloadHardlinkTarget(info, jptm)

			if tc.wantErrContains != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErrContains)
				}
				assert.Contains(t, err.Error(), tc.wantErrContains)
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				assert.Equal(t, tc.wantResult, got)
			}
		})
	}
}
