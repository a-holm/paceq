//go:build linux

package store

import (
	"strings"
	"testing"
)

func TestClassifyFSMagicRefusesNetworkAndFuseFilesystems(t *testing.T) {
	cases := []struct {
		name  string
		magic uint64
	}{
		{name: "nfs", magic: 0x6969},
		{name: "smb", magic: 0x517B},
		{name: "cifs", magic: 0xFF534D42},
		{name: "smb2", magic: 0xFE534D42},
		{name: "fuse", magic: 0x65735546},
		{name: "9p", magic: 0x01021997},
		{name: "ceph", magic: 0x00C36400},
		{name: "gfs2", magic: 0x01161970},
		{name: "ocfs2", magic: 0x7461636F},
		{name: "afs", magic: 0x5346414F},
		{name: "kafs", magic: 0x6B414653},
		{name: "lustre", magic: 0x0BD00BD0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyFSMagic("/srv/state", tc.magic)
			if err == nil {
				t.Fatalf("classifyFSMagic(%s, %#x) = nil, want a refusal", tc.name, tc.magic)
			}
			msg := strings.ToLower(err.Error())
			// The message has to carry the consequence and the way out, or the
			// operator reads it as a portability nag and works around it.
			for _, want := range []string{"/srv/state", "corrupt", "local disk", "allow-network-fs"} {
				if !strings.Contains(msg, want) {
					t.Errorf("refusal for %s does not mention %q: %s", tc.name, want, err)
				}
			}
		})
	}
}

func TestClassifyFSMagicAcceptsLocalFilesystems(t *testing.T) {
	cases := []struct {
		name  string
		magic uint64
	}{
		{name: "ext4", magic: 0xEF53},
		{name: "xfs", magic: 0x58465342},
		{name: "btrfs", magic: 0x9123683E},
		{name: "tmpfs", magic: 0x01021994},
		{name: "zfs", magic: 0x2FC12FC1},
		{name: "overlayfs", magic: 0x794C7630},
		{name: "f2fs", magic: 0xF2F52010},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := classifyFSMagic("/srv/state", tc.magic); err != nil {
				t.Errorf("classifyFSMagic(%s, %#x) = %v, want nil", tc.name, tc.magic, err)
			}
		})
	}
}

// TestCheckLocalFSAcceptsTheTestDirectory keeps the syscall path exercised. CI
// and developer machines run on a local filesystem, so a refusal here is a bug
// in the check rather than a genuine network mount.
func TestCheckLocalFSAcceptsTheTestDirectory(t *testing.T) {
	if err := checkLocalFS(t.TempDir()); err != nil {
		t.Errorf("checkLocalFS(t.TempDir()) = %v, want nil", err)
	}
}

func TestCheckLocalFSReportsAMissingDirectory(t *testing.T) {
	if err := checkLocalFS(t.TempDir() + "/does-not-exist"); err == nil {
		t.Error("checkLocalFS accepted a directory that does not exist, want an error")
	}
}

func TestAllowNetworkFSBypassesTheCheck(t *testing.T) {
	missing := t.TempDir() + "/does-not-exist"

	if err := guardFilesystem(missing, false); err == nil {
		t.Fatal("guardFilesystem accepted a directory it could not stat, want an error")
	}
	if err := guardFilesystem(missing, true); err != nil {
		t.Errorf("guardFilesystem(%q, allowNetworkFS) = %v, want nil", missing, err)
	}
}
