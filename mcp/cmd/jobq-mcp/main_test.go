package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAddrAndDirAreMutuallyExclusive(t *testing.T) {
	dir := t.TempDir()
	err := run([]string{"-addr", "http://127.0.0.1:9999", "-dir", dir})
	if err == nil {
		t.Fatal("run succeeded with both -addr and -dir explicitly set, want an error")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("err = %v, want it to mention mutually exclusive", err)
	}
}

func TestDirOnALockedDirectoryExplainsItself(t *testing.T) {
	dir := t.TempDir()
	// Take the lock ourselves first, the same way a running jobqd would.
	lockPath := filepath.Join(dir, "jobq.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("seeding a lock file: %v", err)
	}
	defer f.Close()

	err = run([]string{"-dir", dir})
	if err == nil {
		t.Fatal("run succeeded against a locked directory, want an error")
	}
	if !strings.Contains(err.Error(), "jobqd") {
		t.Fatalf("err = %v, want the explanatory message to name jobqd rather than surface a raw lock error", err)
	}
	if !strings.Contains(err.Error(), "locked") {
		t.Fatalf("err = %v, want it to say the directory is locked", err)
	}
}

func TestInvalidSyncPolicyIsRejected(t *testing.T) {
	dir := t.TempDir()
	err := run([]string{"-dir", dir, "-sync", "sometimes"})
	if err == nil {
		t.Fatal("run succeeded with an invalid -sync value, want an error")
	}
	if !strings.Contains(err.Error(), "-sync") {
		t.Fatalf("err = %v, want it to mention -sync", err)
	}
}
