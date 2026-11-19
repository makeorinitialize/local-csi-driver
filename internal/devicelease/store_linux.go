// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// Open checks the provisioned state directory. Its parent and mount are
// operator-controlled and must not be moved or replaced during service.
func Open(path string, backend Backend) (*Provider, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 || int(info.Sys().(*syscall.Stat_t).Uid) != os.Geteuid() || backend == nil {
		return nil, errors.New("need a private provider-owned directory and backend")
	}
	return &Provider{directory: path, backend: backend}, nil
}

// Initialize is an explicit operator operation, not a Reserve recovery path.
// A missing journal on a populated pool must never cause reconstruction from
// a set of names, which would discard replay history.
func (p Provider) Initialize(ctx context.Context) error {
	return locked(p.directory, func(dir *os.File) error {
		if _, err := os.Lstat(filepath.Join(dir.Name(), "ledger.json")); !errors.Is(err, os.ErrNotExist) {
			return errors.New("ledger exists or cannot be inspected")
		}
		pool, err := p.backend.Inspect(ctx)
		if err != nil {
			return err
		}
		if pool.UUID == "" || len(pool.Volumes) != 0 {
			return errors.New("initialization requires an empty dedicated pool")
		}
		return saveLedger(dir, &ledger{Version: 1, PoolUUID: pool.UUID, Records: map[string]Reservation{}, Requests: map[string]reserveIntent{}})
	})
}

func locked(path string, fn func(*os.File) error) error {
	dir, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer dir.Close()
	// Lock the directory inode, never the snapshot inode replaced by rename.
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	return fn(dir)
}

func withLedger(path string, fn func(*ledger, func() error) error) error {
	return locked(path, func(dir *os.File) error {
		if err := dir.Sync(); err != nil {
			return err
		}
		data, err := os.ReadFile(filepath.Join(path, "ledger.json"))
		if err != nil {
			return fmt.Errorf("%w: %w", ErrUncertain, err)
		}
		d := json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		var s ledger
		if err := d.Decode(&s); err != nil {
			return fmt.Errorf("%w: %w", ErrUncertain, err)
		}
		if d.Decode(new(any)) != io.EOF {
			return ErrUncertain
		}
		if err := s.validate(); err != nil {
			return err
		}
		return fn(&s, func() error { return saveLedger(dir, &s) })
	})
}

func saveLedger(dir *os.File, s *ledger) error {
	if err := s.validate(); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir.Name(), ".pending-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	err = json.NewEncoder(f).Encode(s)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(f.Name(), filepath.Join(dir.Name(), "ledger.json")); err != nil {
		return err
	}
	return dir.Sync()
}
