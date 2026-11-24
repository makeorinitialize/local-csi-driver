// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package deviceuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const journalName = "use-ledger.json"

type FileStore struct {
	path  string
	scope Scope
	dev   uint64
	inode uint64
}

func Open(path string, scope Scope) (*FileStore, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !scope.Valid() || !st.IsDir() || st.Mode().Perm() != 0700 || int(st.Sys().(*syscall.Stat_t).Uid) != os.Geteuid() {
		return nil, status.Error(codes.Unavailable, "use journal requires a protected existing directory")
	}
	stat := st.Sys().(*syscall.Stat_t)
	s := &FileStore{path: path, scope: scope, dev: uint64(stat.Dev), inode: stat.Ino}
	if err := s.Transact(context.Background(), func(*Snapshot) (bool, error) { return false, nil }); err != nil {
		return nil, err
	}
	return s, nil
}

// Initialize is an explicit offline operation, never a startup recovery path.
// The persistent directory and its parent must not be moved or replaced.
func Initialize(path string, scope Scope) error {
	state, err := Initial(scope)
	if err != nil {
		return err
	}
	return lockDirectory(path, 0, 0, func(dir *os.File) error {
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			return status.Error(codes.AlreadyExists, "journal directory is not empty; state reuse or migration is forbidden")
		}
		return save(dir, state)
	})
}

func lockDirectory(path string, dev, inode uint64, fn func(*os.File) error) error {
	dir, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer dir.Close()
	st, err := dir.Stat()
	if err != nil {
		return err
	}
	stat := st.Sys().(*syscall.Stat_t)
	if st.Mode().Perm() != 0700 || int(stat.Uid) != os.Geteuid() || inode != 0 && (stat.Ino != inode || uint64(stat.Dev) != dev) {
		return status.Error(codes.Unavailable, "protected use journal directory replaced")
	}
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(dir.Fd()), syscall.LOCK_UN)
	return fn(dir)
}

func Decode(raw []byte, scope Scope) (*Snapshot, error) {
	if len(raw) > journalLimit {
		return nil, status.Error(codes.Unavailable, "use journal exceeds limit")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var s Snapshot
	if err := d.Decode(&s); err != nil {
		return nil, status.Error(codes.Unavailable, "invalid use journal")
	}
	if !errors.Is(d.Decode(new(any)), io.EOF) {
		return nil, status.Error(codes.Unavailable, "trailing use journal data")
	}
	if err := s.Validate(scope); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(&s)
	if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) {
		return nil, status.Error(codes.Unavailable, "noncanonical or duplicate use journal fields")
	}
	return &s, nil
}

func (s *FileStore) Transact(ctx context.Context, fn func(*Snapshot) (bool, error)) error {
	return lockDirectory(s.path, s.dev, s.inode, func(dir *os.File) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := dir.Sync(); err != nil {
			return err
		}
		f, err := os.OpenFile(filepath.Join(s.path, journalName), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return status.Error(codes.Unavailable, "use journal missing or unavailable")
		}
		st, statErr := f.Stat()
		if statErr != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 ||
			int(st.Sys().(*syscall.Stat_t).Uid) != os.Geteuid() {
			f.Close()
			return status.Error(codes.Unavailable, "use journal is not a protected regular file")
		}
		data, err := io.ReadAll(io.LimitReader(f, journalLimit+1))
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
		state, err := Decode(data, s.scope)
		if err != nil {
			return err
		}
		changed, err := fn(state)
		if err != nil || !changed {
			return err
		}
		if err := state.Validate(s.scope); err != nil {
			return err
		}
		return save(dir, state)
	})
}

func save(dir *os.File, state *Snapshot) error {
	data, err := json.Marshal(state)
	if err != nil || len(data) > journalLimit {
		return status.Error(codes.ResourceExhausted, "use journal capacity reached")
	}
	f, err := os.CreateTemp(dir.Name(), ".use-pending-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(f.Name(), filepath.Join(dir.Name(), journalName)); err != nil {
		return err
	}
	return dir.Sync()
}
