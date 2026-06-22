// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

// newNudgeTestCmd builds a cobra command exposing a representative subset of the
// copy flags, then "sets" the given flags (so cmd.Flags().Visit treats them as
// changed, exactly like real user input).
func newNudgeTestCmd(t *testing.T, setFlags ...string) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "copy"}
	var b bool
	var s string
	// A mix of Storage-Mover-compatible and azcopy-only flags.
	cmd.Flags().BoolVar(&b, "recursive", false, "")
	cmd.Flags().BoolVar(&b, "put-md5", false, "")
	cmd.Flags().BoolVar(&b, "dry-run", false, "")
	cmd.Flags().StringVar(&s, "include-pattern", "", "")
	cmd.Flags().StringVar(&s, "blob-type", "", "")

	for _, name := range setFlags {
		if err := cmd.Flags().Set(name, flagValueFor(name)); err != nil {
			t.Fatalf("failed to set flag %q: %v", name, err)
		}
	}
	return cmd
}

func flagValueFor(name string) string {
	switch name {
	case "include-pattern", "blob-type":
		return "x"
	default:
		return "true"
	}
}

func TestEvaluateStorageMoverOpportunity(t *testing.T) {
	a := assert.New(t)

	cases := []struct {
		name         string
		fromTo       common.FromTo
		setFlags     []string
		wantEligible bool
		wantReason   string
	}{
		{
			name:         "covered pair, no flags",
			fromTo:       common.EFromTo.LocalBlob(),
			wantEligible: true,
		},
		{
			name:         "covered pair, compatible flags only",
			fromTo:       common.EFromTo.LocalBlob(),
			setFlags:     []string{"recursive", "put-md5", "dry-run"},
			wantEligible: true,
		},
		{
			name:         "covered pair S3 to Blob",
			fromTo:       common.EFromTo.S3Blob(),
			wantEligible: true,
		},
		{
			name:         "covered pair, incompatible flag",
			fromTo:       common.EFromTo.LocalBlob(),
			setFlags:     []string{"recursive", "include-pattern"},
			wantEligible: false,
			wantReason:   "incompatible-flags:include-pattern",
		},
		{
			name:         "uncovered pair (download)",
			fromTo:       common.EFromTo.BlobLocal(),
			wantEligible: false,
			wantReason:   "pair-not-covered:BlobLocal",
		},
		{
			name:         "uncovered pair takes precedence over flags",
			fromTo:       common.EFromTo.BlobLocal(),
			setFlags:     []string{"include-pattern"},
			wantEligible: false,
			wantReason:   "pair-not-covered:BlobLocal",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := newNudgeTestCmd(t, tc.setFlags...)
			eligible, reason := evaluateStorageMoverOpportunity(cmd, tc.fromTo)
			a.Equal(tc.wantEligible, eligible)
			a.Equal(tc.wantReason, reason)
		})
	}
}

func TestIncompatibleFlagsUsed(t *testing.T) {
	a := assert.New(t)

	// Only compatible flags changed -> nothing reported.
	cmd := newNudgeTestCmd(t, "recursive", "put-md5")
	a.Empty(incompatibleFlagsUsed(cmd))

	// An azcopy-only flag changed -> reported.
	cmd = newNudgeTestCmd(t, "recursive", "blob-type")
	a.Equal([]string{"blob-type"}, incompatibleFlagsUsed(cmd))
}
