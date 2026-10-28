package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"time"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/backend/providers/distributed"
	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/store/sqlstore"
)

// The blob layer's collector settings have no library default (the writer's
// link margin and the collector's grace have to agree), so this binary picks
// them. They are also what blobRequested compares against: a flag left at one of
// these values is not a request for the blob layer.
const (
	defaultBlobGCGrace = time.Hour
	defaultBlobGCBatch = 200
)

// blobRequested reports whether any --blob-* flag differs from its default. The
// grace and batch defaults are non-zero, so "non-zero" would wrongly count an
// untouched flag as a request; a zero there can only come from a hand-built
// config and is rejected by validateBlobFlags when it comes from a flag.
func (c serverConfig) blobRequested() bool {
	return c.blobDir != "" || c.blobInlineMax != 0 || c.blobEnableRefWrites ||
		c.blobCompress || c.blobGCInterval != 0 ||
		(c.blobGCGrace != 0 && c.blobGCGrace != defaultBlobGCGrace) ||
		(c.blobGCBatch != 0 && c.blobGCBatch != defaultBlobGCBatch)
}

// validateBlobFlags refuses a blob selection that cannot work, at parse time and
// before anything is opened. Messages name flags only: --blob-dir may contain a
// user name, so its value never appears in an error.
func validateBlobFlags(cfg serverConfig) error {
	// The numbers are checked even when the layer is off: a typo found on the day
	// the layer is enabled is worse than one found at startup.
	switch {
	case cfg.blobInlineMax < 0:
		return errors.New("--blob-inline-max must not be negative (0 means the library default)")
	case cfg.blobGCGrace <= 0:
		return errors.New("--blob-gc-grace must be positive")
	case cfg.blobGCBatch <= 0:
		return errors.New("--blob-gc-batch must be positive")
	case cfg.blobGCInterval < 0:
		return errors.New("--blob-gc-interval must not be negative (0 means the collection loop's own default)")
	}
	if !cfg.blobRequested() {
		return nil
	}
	if cfg.blobDir == "" {
		return errors.New("--blob-* settings need --blob-dir, the directory blobs are stored in")
	}
	if cfg.memory {
		return errors.New("--blob-dir needs a Redis backend (--redis or --redis-mode); it cannot take effect with --memory or without Redis")
	}
	if cfg.mysqlDSN == "" {
		return errors.New("--blob-dir needs --mysql-dsn: blob metadata is kept in the database")
	}
	return nil
}

// buildBlobConfig turns the --blob-* flags into the blob layer's configuration.
// It returns nil, nil when no --blob-* flag was given, and in that case touches
// neither the database nor the filesystem. The directory is probed here, at
// startup, so an unwritable mount fails the process instead of the first large
// payload. db is the database the blob index lives in (the one the SQL store
// uses); errors never carry the directory.
func buildBlobConfig(cfg serverConfig, db *gorm.DB) (*distributed.BlobConfig, error) {
	if !cfg.blobRequested() {
		return nil, nil
	}
	if db == nil {
		return nil, errors.New("blob config: --blob-dir needs a database (--mysql-dsn)")
	}
	idx, err := sqlstore.NewBlobIndex(db, nil)
	if err != nil {
		return nil, fmt.Errorf("blob config: %w", err)
	}
	objects := objectstore.NewFSStore(cfg.blobDir)
	if err := probeBlobDir(cfg.blobDir); err != nil {
		return nil, err
	}
	return &distributed.BlobConfig{
		Store:      blob.NewObjectBacked(objects, blob.ObjectBackedOptions{Index: idx, Compress: cfg.blobCompress}),
		Index:      idx,
		Enabled:    cfg.blobEnableRefWrites,
		InlineMax:  cfg.blobInlineMax,
		Grace:      cfg.blobGCGrace,
		Batch:      cfg.blobGCBatch,
		GCInterval: cfg.blobGCInterval,
	}, nil
}

// probeBlobDir writes, reads back and deletes one object directly in the
// directory, bypassing the blob index, so a mount that is missing, read-only or
// unreadable fails at startup. The probe object is removed on every path, and
// no returned error contains the directory: the standard library's path errors
// embed it, so only their operation and underlying cause are kept.
func probeBlobDir(dir string) error {
	// FSStore creates a missing root on its first write. A blob directory that
	// is not there is a missing mount, not something to create silently.
	info, err := os.Stat(dir)
	if err != nil {
		return probeError("stat", err)
	}
	if !info.IsDir() {
		return errors.New("blob dir probe: stat: not a directory")
	}

	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("blob dir probe: nonce: %w", err)
	}
	key := ".probe-" + hex.EncodeToString(nonce[:])
	payload := []byte("xflow blob dir probe " + hex.EncodeToString(nonce[:]))

	objects := objectstore.NewFSStore(dir)
	ctx := context.Background()
	defer func() { _ = objects.DeleteObject(ctx, key) }()

	if _, err := objects.PutObject(ctx, key, bytes.NewReader(payload), int64(len(payload)), objectstore.PutOptions{}); err != nil {
		return probeError("write", err)
	}
	rc, _, err := objects.GetObject(ctx, key)
	if err != nil {
		return probeError("read", err)
	}
	got, err := io.ReadAll(io.LimitReader(rc, int64(len(payload))+1))
	_ = rc.Close()
	if err != nil {
		return probeError("read", err)
	}
	if !bytes.Equal(got, payload) {
		return errors.New("blob dir probe: read: content read back differs from content written")
	}
	if err := objects.DeleteObject(ctx, key); err != nil {
		return probeError("delete", err)
	}
	return nil
}

// probeError rebuilds err without any path. A *fs.PathError or *os.LinkError
// names the file it failed on; the operation and the underlying errno are
// enough for an operator, who knows which directory they configured.
func probeError(phase string, err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return fmt.Errorf("blob dir probe: %s: %s: %w", phase, pe.Op, pe.Err)
	}
	var le *os.LinkError
	if errors.As(err, &le) {
		return fmt.Errorf("blob dir probe: %s: %s: %w", phase, le.Op, le.Err)
	}
	return fmt.Errorf("blob dir probe: %s failed", phase)
}

// appendBlobOption adds the blob option to opts when a --blob-* flag was given
// and returns opts unchanged otherwise, logging nothing and touching nothing.
// It runs after the SQL store is open because the blob index needs its
// database. The startup line never carries the directory.
func appendBlobOption(opts []xflowsdk.ServerOption, cfg serverConfig, db *gorm.DB) ([]xflowsdk.ServerOption, error) {
	bc, err := buildBlobConfig(cfg, db)
	if err != nil {
		return opts, err
	}
	if bc == nil {
		return opts, nil
	}
	log.Printf("xflow-server: blob store enabled (ref_writes=%v inline_max=%d compress=%v)",
		bc.Enabled, bc.InlineMax, cfg.blobCompress)
	return append(opts, xflowsdk.WithServerBlob(*bc)), nil
}
