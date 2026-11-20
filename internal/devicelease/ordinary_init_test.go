// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestEnrollmentPublicationBoundary(t *testing.T) {
	root := t.TempDir()
	parent, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}

	defer parent.Close()
	final := filepath.Join(root, "enrollment")
	injected := errors.New("incomplete staging write")
	err = publishEnrollment(parent, final, func(dir *os.File) error {
		if filepath.Dir(dir.Name()) != root || dir.Name() == final {
			t.Fatal("not a private sibling", dir.Name())
		}
		if _, err := os.Lstat(final); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("partial enrollment visible", err)
		}
		if err := os.WriteFile(filepath.Join(dir.Name(), "ledger.json"), []byte("{"), 0600); err != nil {
			t.Fatal(err)
		}
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if _, err := os.Lstat(final); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed write poisoned startup", err)
	}
	// Even an empty directory (not only a populated one) must not be
	// silently replaced. Plain rename would replace this corrupt enrollment.
	if err := os.Mkdir(final, 0700); err != nil {
		t.Fatal(err)
	}
	err = publishEnrollment(parent, final, func(d *os.File) error {
		return os.WriteFile(filepath.Join(d.Name(), "ledger.json"), []byte("{}"), 0600)
	})
	if !errors.Is(err, os.ErrExist) {
		t.Fatal("replaced existing enrollment", err)
	}
	if entries, err := os.ReadDir(final); err != nil || len(entries) != 0 {
		t.Fatal(entries, err)
	}
}

func TestDynamicStartupExcludesUnguardedDriver(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-enrolled-yet")
	old, err := PinEnrollment(path)
	if err != nil {
		t.Fatal(err)
	}
	if release, err := PinDynamicEnrollment(path); err == nil {
		release()
		t.Fatal("dynamic enrollment armed behind old driver")
	}
	old()
	release, err := PinDynamicEnrollment(path)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if old, err := PinEnrollment(path); err == nil {
		old()
		t.Fatal("overlapping unguarded driver started")
	}
}
