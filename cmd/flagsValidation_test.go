package cmd

import (
	"context"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestValidateProtocolCompatibility(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	// Test cases where validation should NOT be called (no File locations involved)
	testCases := []struct {
		name     string
		fromTo   common.FromTo
		shouldValidate bool
		description string
	}{
		{
			name:     "S3ToBlob",
			fromTo:   common.EFromTo.S3Blob(),
			shouldValidate: false,
			description: "S3 to Blob should not validate (neither side is File)",
		},
		{
			name:     "GCPToBlob", 
			fromTo:   common.EFromTo.GCPBlob(),
			shouldValidate: false,
			description: "GCP to Blob should not validate (neither side is File)",
		},
		{
			name:     "LocalToBlob",
			fromTo:   common.EFromTo.LocalBlob(),
			shouldValidate: false,
			description: "Local to Blob should not validate (neither side is File)",
		},
		{
			name:     "BlobToLocal",
			fromTo:   common.EFromTo.BlobLocal(),
			shouldValidate: false,
			description: "Blob to Local should not validate (neither side is File)",
		},
		{
			name:     "BlobToBlob",
			fromTo:   common.EFromTo.BlobBlob(),
			shouldValidate: false,
			description: "Blob to Blob should not validate (neither side is File)",
		},
		{
			name:     "LocalToBlobFS",
			fromTo:   common.EFromTo.LocalBlobFS(),
			shouldValidate: false,
			description: "Local to BlobFS should not validate (neither side is File)",
		},
		{
			name:     "LocalToFile",
			fromTo:   common.EFromTo.LocalFile(),
			shouldValidate: true,
			description: "Local to File should validate (destination is File)",
		},
		{
			name:     "FileToLocal",
			fromTo:   common.EFromTo.FileLocal(),
			shouldValidate: true,
			description: "File to Local should validate (source is File)",
		},
		{
			name:     "LocalToFileNFS",
			fromTo:   common.EFromTo.LocalFileNFS(),
			shouldValidate: true,
			description: "Local to FileNFS should validate (destination is FileNFS)",
		},
		{
			name:     "FileNFSToLocal",
			fromTo:   common.EFromTo.FileNFSLocal(),
			shouldValidate: true,
			description: "FileNFS to Local should validate (source is FileNFS)",
		},
		{
			name:     "FileToFile",
			fromTo:   common.EFromTo.FileFile(),
			shouldValidate: true,
			description: "File to File should validate (both sides are File)",
		},
		{
			name:     "FileNFSToFileNFS",
			fromTo:   common.EFromTo.FileNFSFileNFS(),
			shouldValidate: true,
			description: "FileNFS to FileNFS should validate (both sides are FileNFS)",
		},
		{
			name:     "FileToBlob",
			fromTo:   common.EFromTo.FileBlob(),
			shouldValidate: true,
			description: "File to Blob should validate (source is File)",
		},
		{
			name:     "BlobToFile",
			fromTo:   common.EFromTo.BlobFile(),
			shouldValidate: true,
			description: "Blob to File should validate (destination is File)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create dummy resource strings
			src := common.ResourceString{Value: "https://source.example.com/path"}
			dst := common.ResourceString{Value: "https://dest.example.com/path"}
			
			// For non-File transfers, we can pass nil service clients since validation should be skipped
			// For File transfers, we would need proper service clients, but we're testing the conditional logic
			var srcClient, dstClient *common.ServiceClient
			
			if !tc.shouldValidate {
				// Test that validation is skipped when no File locations are involved
				// This should not panic even with nil service clients
				err := validateProtocolCompatibility(ctx, tc.fromTo, src, dst, srcClient, dstClient)
				a.NoError(err, "validateProtocolCompatibility should not fail for %s: %s", tc.name, tc.description)
			} else {
				// For File transfers, we expect the function to attempt validation
				// Since we're passing nil service clients, we expect it to fail gracefully
				// This tests that the conditional logic correctly identifies File transfers
				err := validateProtocolCompatibility(ctx, tc.fromTo, src, dst, srcClient, dstClient)
				// We expect an error here because we're passing nil service clients for File transfers
				// The important thing is that it doesn't panic and attempts validation
				if tc.fromTo.From().IsFile() || tc.fromTo.To().IsFile() {
					a.Error(err, "validateProtocolCompatibility should attempt validation for %s and fail with nil clients: %s", tc.name, tc.description)
				}
			}
		})
	}
}

func TestValidateProtocolCompatibility_ConditionalLogic(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	// Test the specific conditional logic
	src := common.ResourceString{Value: "https://source.example.com/path"}
	dst := common.ResourceString{Value: "https://dest.example.com/path"}

	// Test that S3->Blob doesn't call validation (should not panic with nil clients)
	err := validateProtocolCompatibility(ctx, common.EFromTo.S3Blob(), src, dst, nil, nil)
	a.NoError(err, "S3->Blob should skip validation and not panic with nil service clients")

	// Test that GCP->Blob doesn't call validation (should not panic with nil clients)  
	err = validateProtocolCompatibility(ctx, common.EFromTo.GCPBlob(), src, dst, nil, nil)
	a.NoError(err, "GCP->Blob should skip validation and not panic with nil service clients")

	// Test that Local->Blob doesn't call validation (should not panic with nil clients)
	err = validateProtocolCompatibility(ctx, common.EFromTo.LocalBlob(), src, dst, nil, nil)
	a.NoError(err, "Local->Blob should skip validation and not panic with nil service clients")
}
