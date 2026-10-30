package hostinfo

import (
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/stretchr/testify/assert"
)

// Pure classification tests with no host probes, so they need no hostinfoe2e tag.

func TestHostHardwareLinuxClassifyNetworkAndVMShareFSTypes(t *testing.T) {
	for _, fsType := range []string{"lustre", "ceph", "glusterfs", "beegfs", "gpfs", "afs",
		"fuse.blobfuse", "fuse.blobfuse2", "fuse.sshfs", "fuse.s3fs", "fuse.gcsfuse", "fuse.rclone", "fuse.glusterfs", "fuse.ceph-fuse"} {
		assert.Equal(t, telemetry.MountTypeNASOther, classifyLinuxFSType(fsType), fsType)
	}
	for _, fsType := range []string{"9p", "virtiofs", "vboxsf", "fuse.vmhgfs-fuse"} {
		assert.Equal(t, telemetry.MountTypeVMShare, classifyLinuxFSType(fsType), fsType)
	}
	for _, fsType := range []string{"fuseblk", "fuse.fuse-overlayfs", "fuse.portal", "fuse", "overlay", "tmpfs", "btrfs"} {
		assert.Equal(t, telemetry.MountTypeLocalDisk, classifyLinuxFSType(fsType), fsType)
	}
	assert.Equal(t, telemetry.MountTypeVMShare, LinuxMountTypeFromReader("/mnt/c/data/file", strings.NewReader(
		"22 1 8:1 / / rw - ext4 /dev/sda1 rw\n90 22 0:52 / /mnt/c rw - 9p drvfs rw\n")))
}

func TestHostHardwareLinuxClassifyFUSEBySource(t *testing.T) {
	for _, source := range []string{"blobfuse", "blobfuse2"} {
		assert.Equal(t, telemetry.MountTypeNASOther, classifyLinuxMount("fuse", source), source)
	}
	assert.Equal(t, telemetry.MountTypeLocalDisk, classifyLinuxMount("fuse", "portal"))
	assert.Equal(t, telemetry.MountTypeLocalDisk, classifyLinuxMount("fuseblk", "blobfuse2"))
	assert.Equal(t, telemetry.MountTypeNASNFS, classifyLinuxMount("nfs4", "server:/export"))
	mounts := "22 1 8:1 / / rw - ext4 /dev/sda1 rw\n" +
		"40 22 0:60 / /mnt/blob rw,nosuid,nodev - fuse blobfuse2 rw,user_id=0,group_id=0\n" +
		"41 22 0:61 / /mnt/other rw,nosuid,nodev - fuse portal rw,user_id=0,group_id=0\n"
	assert.Equal(t, telemetry.MountTypeNASOther, LinuxMountTypeFromReader("/mnt/blob/data", strings.NewReader(mounts)))
	assert.Equal(t, telemetry.MountTypeLocalDisk, LinuxMountTypeFromReader("/mnt/other/data", strings.NewReader(mounts)))
}

func TestHostHardwareWindowsNetworkMountType(t *testing.T) {
	tests := []struct {
		name, path   string
		drive        int
		target       string
		fileSystem   string
		queriedDrive string
		queriedRoot  string
		expected     telemetry.MountType
	}{
		{"mapped nfs drive", `Z:\data`, driveRemote, `\\nfs.invalid\export`, "NFS", "Z:", `Z:\`, "nas-nfs"},
		{"mapped smb drive", `Z:\data`, driveRemote, `\\server.invalid\share`, "NTFS", "Z:", `Z:\`, "nas-smb"},
		{"mapped drive name unavailable", `Z:\data`, driveRemote, "", "", "Z:", `Z:\`, "nas-smb"},
		{"mapped rdp drive", `Z:\data`, driveRemote, `\\tsclient\C`, "NTFS", "Z:", "", "nas-other"},
		{"mapped webdav drive", `y:\docs`, driveRemote, `\\contoso.invalid@SSL\DavWWWRoot\docs`, "", "y:", "", "nas-other"},
		{"mapped wsl drive", `W:\home`, driveRemote, `\\wsl.localhost\Ubuntu`, "", "W:", "", "vm-share"},
		{"fixed drive", `C:\data`, driveFixed, `\\tsclient\C`, "NFS", "", "", "local-disk"},
		{"unc nfs", `\\nfs.invalid\export\dir`, driveUnknown, "", "NFS", "", `\\nfs.invalid\export\`, "nas-nfs"},
		{"unc nfs lowercase", "//nfs.invalid/export/dir", driveUnknown, "", "nfs", "", `\\nfs.invalid\export\`, "nas-nfs"},
		{"extended unc nfs", `\\?\UNC\nfs.invalid\export\dir`, driveUnknown, "", "NFS", "", `\\nfs.invalid\export\`, "nas-nfs"},
		{"unc smb", `\\server.invalid\share\dir`, driveUnknown, "", "NTFS", "", `\\server.invalid\share\`, "nas-smb"},
		{"unc host only", `\\server.invalid`, driveUnknown, "", "NFS", "", "", "nas-smb"},
		{"rdp redirected drive", `\\tsclient\C\data`, driveUnknown, "", "NTFS", "", "", "nas-other"},
		{"webdav ssl", `\\contoso.invalid@SSL\DavWWWRoot\docs`, driveUnknown, "", "", "", "", "nas-other"},
		{"webdav port", `\\contoso.invalid@8080\share\docs`, driveUnknown, "", "", "", "", "nas-other"},
		{"webdav root share", `\\contoso.invalid\DavWWWRoot\docs`, driveUnknown, "", "", "", "", "nas-other"},
		{"wsl legacy", `\\wsl$\Ubuntu\home`, driveUnknown, "", "", "", "", "vm-share"},
		{"wsl localhost", `\\?\UNC\WSL.localhost\Ubuntu\home`, driveUnknown, "", "", "", "", "vm-share"},
		{"virtualbox shared folder", `\\VBoxSvr\shared\data`, driveUnknown, "", "", "", "", "vm-share"},
		{"vmware shared folder", `\\vmware-host\Shared Folders\data`, driveUnknown, "", "", "", "", "vm-share"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queriedDrive, queriedRoot := "", ""
			mountType := WindowsMountType(test.path, func(string) int { return test.drive }, func(drive string) string {
				queriedDrive = drive
				return test.target
			}, func(root string) string {
				queriedRoot = root
				return test.fileSystem
			})
			assert.Equal(t, test.expected, mountType)
			assert.Equal(t, test.queriedDrive, queriedDrive)
			assert.Equal(t, test.queriedRoot, queriedRoot)
		})
	}
}

func TestHostHardwareDarwinClassifyFSType(t *testing.T) {
	tests := []struct {
		fsType   string
		local    bool
		expected telemetry.MountType
	}{
		{"nfs", false, "nas-nfs"},
		{"nfs4", false, "nas-nfs"},
		{"smbfs", false, "nas-smb"},
		{"smb3", false, "nas-smb"},
		{"afpfs", false, "nas-other"},
		{"webdav", false, "nas-other"},
		{"macfuse", false, "nas-other"},
		{"macfuse", true, "local-disk"},
		{"apfs", true, "local-disk"},
		{"hfs", true, "local-disk"},
		{"exfat", true, "local-disk"},
		{"", false, ""},
	}
	for _, test := range tests {
		t.Run(test.fsType, func(t *testing.T) {
			assert.Equal(t, test.expected, DarwinMountType(test.fsType, test.local))
		})
	}
}
