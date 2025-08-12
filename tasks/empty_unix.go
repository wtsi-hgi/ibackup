package tasks

import (
	"fmt"
	"os"
)

func newEmptyFile() (string, func(), error) {
	f, err := os.CreateTemp("", "")
	if err != nil {
		return "", nil, err
	}

	if err := os.Remove(f.Name()); err != nil {
		return "", nil, err
	}

	return fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), f.Fd()), func() { f.Close() }, nil
}
