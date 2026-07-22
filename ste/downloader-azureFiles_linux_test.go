//go:build linux
// +build linux

package ste

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// sourceRootStub is a minimal stub that satisfies the GetSourceRoot() call
// used by computeDownloadHardlinkTarget. All other IJobPartTransferMgr methods
// are unused and will panic if called — which is fine for unit tests that only
// exercise computeDownloadHardlinkTarget.
type sourceRootStub struct {
	IJobPartTransferMgr // embedded; panics on any method not overridden
	root                string
}

func (s *sourceRootStub) GetSourceRoot() string { return s.root }

func TestComputeDownloadHardlinkTarget(t *testing.T) {
	tests := []struct {
		name                   string
		sourceRoot             string // Azure Files URL (source root)
		srcFilePath            string // TransferInfo.SrcFilePath
		destination            string // TransferInfo.Destination (local path)
		targetHardlinkFilePath string // TransferInfo.TargetHardlinkFilePath (traversal-root-relative)
		wantPath               string // expected result
		wantErr                bool
	}{
		// ────────────────── normal directory copy ──────────────────
		{
			name:                   "directory copy — file in subdirectory",
			sourceRoot:             "https://account.file.core.windows.net/share/srcdir",
			srcFilePath:            "srcdir/subdir/link.txt",
			destination:            "/local/dstdir/subdir/link.txt",
			targetHardlinkFilePath: "subdir/anchor.txt",
			wantPath:               "/local/dstdir/subdir/anchor.txt",
		},
		{
			name:                   "directory copy — file at root level",
			sourceRoot:             "https://account.file.core.windows.net/share/srcdir",
			srcFilePath:            "srcdir/link.txt",
			destination:            "/local/dstdir/link.txt",
			targetHardlinkFilePath: "anchor.txt",
			wantPath:               "/local/dstdir/anchor.txt",
		},
		{
			name:                   "directory copy — deeply nested file",
			sourceRoot:             "https://account.file.core.windows.net/share/root",
			srcFilePath:            "root/a/b/c/link.txt",
			destination:            "/mnt/dest/a/b/c/link.txt",
			targetHardlinkFilePath: "a/anchor.txt",
			wantPath:               "/mnt/dest/a/anchor.txt",
		},

		// ───────────── share root as source root ─────────────
		{
			name:                   "share root — source root is share itself",
			sourceRoot:             "https://account.file.core.windows.net/share",
			srcFilePath:            "subdir/link.txt",
			destination:            "/local/dstdir/subdir/link.txt",
			targetHardlinkFilePath: "subdir/anchor.txt",
			wantPath:               "/local/dstdir/subdir/anchor.txt",
		},

		// ───────────── single-file transfer (relPath == "") ─────────────
		{
			name:                   "single file transfer — relative path is empty",
			sourceRoot:             "https://account.file.core.windows.net/share/srcdir/file.txt",
			srcFilePath:            "srcdir/file.txt",
			destination:            "/local/file.txt",
			targetHardlinkFilePath: "anchor.txt",
			wantPath:               "/local/file.txt/anchor.txt", // Join uses full dest as prefix
		},

		// ───────────── StripTopDir scenario ─────────────
		{
			name:                   "strip top dir — source root includes trailing slash",
			sourceRoot:             "https://account.file.core.windows.net/share/srcdir/",
			srcFilePath:            "srcdir/subdir/link.txt",
			destination:            "/local/dstdir/subdir/link.txt",
			targetHardlinkFilePath: "subdir/anchor.txt",
			wantPath:               "/local/dstdir/subdir/anchor.txt",
		},

		// ───────────── target in different subdirectory ─────────────
		{
			name:                   "target anchor is in a different subdirectory",
			sourceRoot:             "https://account.file.core.windows.net/share/root",
			srcFilePath:            "root/dirA/link.txt",
			destination:            "/dest/dirA/link.txt",
			targetHardlinkFilePath: "dirB/anchor.txt",
			wantPath:               "/dest/dirB/anchor.txt",
		},

		// ───────────── destination path separator mismatch → ERROR ─────────────
		{
			name:                   "error — destination does not end with expected relative path",
			sourceRoot:             "https://account.file.core.windows.net/share/srcdir",
			srcFilePath:            "srcdir/subdir/link.txt",
			destination:            "/local/WRONG/link.txt", // "subdir/link.txt" is not a suffix
			targetHardlinkFilePath: "subdir/anchor.txt",
			wantErr:                true,
		},

		// ───────────── URL normalization edge — double slashes ─────────────
		{
			name:                   "error — extra path component in destination breaks suffix match",
			sourceRoot:             "https://account.file.core.windows.net/share/srcdir",
			srcFilePath:            "srcdir/sub/link.txt",
			destination:            "/local/dstdir/extra/sub/link.txt", // extra segment
			targetHardlinkFilePath: "sub/anchor.txt",
			wantPath:               "/local/dstdir/extra/sub/anchor.txt", // prefix becomes /local/dstdir/extra/
		},

		// ───────────── invalid source root URL → ERROR ─────────────
		{
			name:                   "error — unparsable source root URL",
			sourceRoot:             "://not-a-valid-url",
			srcFilePath:            "dir/link.txt",
			destination:            "/local/dir/link.txt",
			targetHardlinkFilePath: "dir/anchor.txt",
			wantErr:                true,
		},

		// ───────────── target at root ─────────────
		{
			name:                   "target hardlink at traversal root",
			sourceRoot:             "https://account.file.core.windows.net/share/srcdir",
			srcFilePath:            "srcdir/sub/link.txt",
			destination:            "/local/dst/sub/link.txt",
			targetHardlinkFilePath: "anchor.txt",
			wantPath:               "/local/dst/anchor.txt",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info := &TransferInfo{
				SrcFilePath:            tc.srcFilePath,
				Destination:            tc.destination,
				TargetHardlinkFilePath: tc.targetHardlinkFilePath,
			}
			stub := &sourceRootStub{root: tc.sourceRoot}

			got, err := computeDownloadHardlinkTarget(info, stub)

			if tc.wantErr {
				assert.Error(t, err, "expected an error but got none; result=%q", got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantPath, got)
			}
		})
	}
}
