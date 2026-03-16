package ste

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// uploadStub satisfies GetSourceRoot and FailActiveSend for computeUploadHardlinkTarget.
type uploadStub struct {
	IJobPartTransferMgr
	root       string
	failCalled bool
	failWhere  string
}

func (s *uploadStub) GetSourceRoot() string { return s.root }
func (s *uploadStub) FailActiveSend(where string, err error) {
	s.failCalled = true
	s.failWhere = where
}

func TestComputeUploadHardlinkTarget(t *testing.T) {
	tests := []struct {
		name                   string
		sourceRoot             string // local source root directory
		source                 string // TransferInfo.Source (local file path)
		destination            string // TransferInfo.Destination (Azure Files URL)
		targetHardlinkFilePath string // TransferInfo.TargetHardlinkFilePath
		wantPath               string // expected result (empty when error expected)
		wantFail               bool   // expect FailActiveSend to be called
	}{
		// ────────────────── normal directory upload ──────────────────
		{
			name:                   "directory upload — file in subdirectory",
			sourceRoot:             "/local/srcdir",
			source:                 "/local/srcdir/subdir/link.txt",
			destination:            "https://account.file.core.windows.net/share/dstdir/subdir/link.txt",
			targetHardlinkFilePath: "subdir/anchor.txt",
			wantPath:               "/dstdir/subdir/anchor.txt",
		},
		{
			name:                   "directory upload — file at root level",
			sourceRoot:             "/local/srcdir",
			source:                 "/local/srcdir/link.txt",
			destination:            "https://account.file.core.windows.net/share/dstdir/link.txt",
			targetHardlinkFilePath: "anchor.txt",
			wantPath:               "/dstdir/anchor.txt",
		},
		{
			name:                   "directory upload — deeply nested file",
			sourceRoot:             "/mnt/data",
			source:                 "/mnt/data/a/b/c/link.txt",
			destination:            "https://account.file.core.windows.net/share/dest/a/b/c/link.txt",
			targetHardlinkFilePath: "a/anchor.txt",
			wantPath:               "/dest/a/anchor.txt",
		},

		// ───────────── source root with trailing slash ─────────────
		{
			name:                   "source root with trailing slash",
			sourceRoot:             "/local/srcdir/",
			source:                 "/local/srcdir/subdir/link.txt",
			destination:            "https://account.file.core.windows.net/share/dstdir/subdir/link.txt",
			targetHardlinkFilePath: "subdir/anchor.txt",
			wantPath:               "/dstdir/subdir/anchor.txt",
		},

		// ───────────── target in different subdirectory ─────────────
		{
			name:                   "target hardlink in different subdirectory",
			sourceRoot:             "/home/user/project",
			source:                 "/home/user/project/dirA/link.txt",
			destination:            "https://account.file.core.windows.net/share/remote/dirA/link.txt",
			targetHardlinkFilePath: "dirB/anchor.txt",
			wantPath:               "/remote/dirB/anchor.txt",
		},

		// ───────────── target at traversal root ─────────────
		{
			name:                   "target hardlink at traversal root",
			sourceRoot:             "/src",
			source:                 "/src/sub/link.txt",
			destination:            "https://account.file.core.windows.net/share/dst/sub/link.txt",
			targetHardlinkFilePath: "anchor.txt",
			wantPath:               "/dst/anchor.txt",
		},

		// ───────────── share root as destination (no extra prefix) ─────────────
		{
			name:                   "destination directly under share root",
			sourceRoot:             "/local/dir",
			source:                 "/local/dir/link.txt",
			destination:            "https://account.file.core.windows.net/share/link.txt",
			targetHardlinkFilePath: "anchor.txt",
			wantPath:               "/anchor.txt",
		},

		// ───────────── single-file transfer (relPath derived from source matches full dest path) ─────────────
		{
			name:                   "single file — relative path matches entire dest directory path",
			sourceRoot:             "/local/srcdir",
			source:                 "/local/srcdir/file.txt",
			destination:            "https://account.file.core.windows.net/share/dstdir/file.txt",
			targetHardlinkFilePath: "anchor.txt",
			wantPath:               "/dstdir/anchor.txt",
		},

		// ───────────── dest path mismatch (TrimSuffix is no-op) ─────────────
		{
			name:                   "dest path mismatch — TrimSuffix has no effect",
			sourceRoot:             "/local/srcdir",
			source:                 "/local/srcdir/subdir/link.txt",
			destination:            "https://account.file.core.windows.net/share/WRONG/link.txt",
			targetHardlinkFilePath: "subdir/anchor.txt",
			// TrimSuffix("WRONG/link.txt", "subdir/link.txt") => "WRONG/link.txt" (no-op)
			// result: "/" + path.Join("WRONG/link.txt", "subdir/anchor.txt")
			wantPath: "/WRONG/link.txt/subdir/anchor.txt",
		},

		// ───────────── source equals source root (empty relPath) ─────────────
		{
			name:                   "source equals source root — empty relative path",
			sourceRoot:             "/local/srcdir",
			source:                 "/local/srcdir",
			destination:            "https://account.file.core.windows.net/share/dstdir",
			targetHardlinkFilePath: "anchor.txt",
			// fileRelPath = TrimPrefix("", "/") = ""
			// TrimSuffix("dstdir", "") = "dstdir"
			wantPath: "/dstdir/anchor.txt",
		},

		// ───────────── destination with SAS token ─────────────
		{
			name:                   "destination URL with SAS query string",
			sourceRoot:             "/data",
			source:                 "/data/sub/link.txt",
			destination:            "https://account.file.core.windows.net/share/out/sub/link.txt?sv=2021-06-08&sig=abc",
			targetHardlinkFilePath: "sub/anchor.txt",
			wantPath:               "/out/sub/anchor.txt",
		},

		// ───────────── multiple nested dirs ─────────────
		{
			name:                   "multiple nested directories in path",
			sourceRoot:             "/a",
			source:                 "/a/b/c/d/e/link.txt",
			destination:            "https://account.file.core.windows.net/share/x/y/b/c/d/e/link.txt",
			targetHardlinkFilePath: "b/c/anchor.txt",
			wantPath:               "/x/y/b/c/anchor.txt",
		},

		// ───────────── invalid destination URL → FailActiveSend ─────────────
		{
			name:                   "error — unparseable destination URL",
			sourceRoot:             "/local",
			source:                 "/local/link.txt",
			destination:            "://not-a-valid-url",
			targetHardlinkFilePath: "anchor.txt",
			wantFail:               true,
		},

		// ───────────── target hardlink path with multiple segments ─────────────
		{
			name:                   "target hardlink path with multiple segments",
			sourceRoot:             "/src",
			source:                 "/src/a/link.txt",
			destination:            "https://account.file.core.windows.net/share/dst/a/link.txt",
			targetHardlinkFilePath: "x/y/z/anchor.txt",
			wantPath:               "/dst/x/y/z/anchor.txt",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info := &TransferInfo{
				Source:                 tc.source,
				Destination:            tc.destination,
				TargetHardlinkFilePath: tc.targetHardlinkFilePath,
			}
			stub := &uploadStub{root: tc.sourceRoot}

			got := computeUploadHardlinkTarget(info, stub)

			if tc.wantFail {
				assert.True(t, stub.failCalled, "expected FailActiveSend to be called")
				assert.Equal(t, "", got)
			} else {
				assert.False(t, stub.failCalled, "unexpected FailActiveSend call: %s", stub.failWhere)
				assert.Equal(t, tc.wantPath, got)
			}
		})
	}
}
