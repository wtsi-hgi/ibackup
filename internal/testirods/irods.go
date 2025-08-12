package testirods

import (
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//go:embed pseudo
var pseudo []byte

var exes = [...]string{"baton-do", "ils", "iget", "iput", "irm"} //nolint:gochecknoglobals

func AddPseudoIRODsToolsToPathIfRequired(t *testing.T) error {
	t.Helper()

	if env := os.Getenv("IBACKUP_TEST_COLLECTION"); env == "" {
		return addToolsToPath(t)
	}

	for _, exe := range exes {
		if _, err := exec.LookPath(exe); err != nil {
			return addToolsToPath(t)
		}
	}

	return nil
}

func addToolsToPath(t *testing.T) error {
	t.Helper()

	if env := os.Getenv("IBACKUP_TEST_COLLECTION"); env == "" {
		os.Setenv("IBACKUP_TEST_COLLECTION", "/irods/test")
	}

	dir := t.TempDir()
	pseudoPath := filepath.Join(dir, exes[0])

	if err := os.WriteFile(pseudoPath, pseudo, 0700); err != nil { //nolint:gosec,mnd
		return err
	}

	for _, exe := range exes[1:] {
		if err := os.Link(pseudoPath, filepath.Join(dir, exe)); err != nil {
			return err
		}
	}

	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	t.Setenv("IRODS_BASE", t.TempDir())

	return nil
}
