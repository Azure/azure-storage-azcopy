package ste

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// PlanPath validates the binding and builds its native filename; five digits is a minimum width.
// It performs no filesystem access, uses no global plan directory, and establishes no ownership.
func (c JobExecutionSessionConfig) PlanPath(part common.PartNumber) (string, error) {
	if c.JobID == (common.JobID{}) || !filepath.IsAbs(c.PlanDirectory) || filepath.Clean(c.PlanDirectory) != c.PlanDirectory {
		return "", errors.New("nonzero job ID and canonical absolute plan directory required")
	}
	return filepath.Join(c.PlanDirectory, fmt.Sprintf(JobPartPlanFileNameFormat, c.JobID.String(), part, DataSchemaVersion)), nil
}

// validateClaimedRoot requires a credential-free URL with a valid container path and no query or fragment.
// HTTPS is required, except for HTTP loopback addresses used by local tests.
func validateClaimedRoot(root string) error {
	u, err := url.Parse(root)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(root, "#") ||
		len(root) > 1000 || u.Path == "" || u.Path == "/" {
		return errors.New("claimed roots must be credential-free URLs with a container path")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback()) {
		return errors.New("claimed roots require HTTPS (HTTP allowed only on loopback)")
	}
	return nil
}

// validateClaimedHeader binds a trusted planner-produced plan to this session.
// Transfer records and copy options are interpreted by the existing STE scheduler.
func (c JobExecutionSessionConfig) validateClaimedHeader(h *JobPartPlanHeader, part common.PartNumber) error {
	if h.Version != DataSchemaVersion {
		return errors.New("unsupported native plan schema")
	}
	if h.JobID != c.JobID || h.PartNum != part {
		return errors.New("claimed plan identity mismatch")
	}
	if h.FromTo != common.EFromTo.BlobBlob() {
		return errors.New("job execution sessions support Blob-to-Blob plans only")
	}
	if int(h.SourceRootLength) > len(h.SourceRoot) || int(h.DestinationRootLength) > len(h.DestinationRoot) ||
		string(h.SourceRoot[:h.SourceRootLength]) != c.SourceRoot ||
		string(h.DestinationRoot[:h.DestinationRootLength]) != c.DestinationRoot {
		return errors.New("claimed plan roots do not match the binding")
	}
	return nil
}
