// Package cacher defines utilities for saving and restoring caches from Google
// Cloud storage.
package cacher

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/mholt/archiver/v4"

	"cloud.google.com/go/storage"
	"golang.org/x/crypto/blake2b"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	contentType  = "application/x-zstd-compressed-tar"
	cacheControl = "public,max-age=600"
)

// Cacher is responsible for saving and restoring caches.
type Cacher struct {
	client *storage.Client

	debug bool
}

// New creates a new cacher capable of saving and restoring the cache.
func New(ctx context.Context) (*Cacher, error) {
	client, err := storage.NewClient(ctx,
		option.WithUserAgent("gcs-cacher/1.0"))
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	return &Cacher{
		client: client,
	}, nil
}

// Debug enables or disables debugging for the cacher.
func (c *Cacher) Debug(val bool) {
	c.debug = val
}

// SaveRequest is used as input to the Save operation.
type SaveRequest struct {
	// Bucket is the name of the bucket from which to cache.
	Bucket string

	// Key is the cache key.
	Key string

	// Dir is the directory on disk to cache.
	Dir string
}

// Save caches the given directory in storage.
func (c *Cacher) Save(ctx context.Context, i *SaveRequest) (retErr error) {
	if i == nil {
		retErr = fmt.Errorf("missing cache options")
		return
	}

	bucket := i.Bucket
	if bucket == "" {
		retErr = fmt.Errorf("missing bucket")
		return
	}

	dir := i.Dir
	if dir == "" {
		retErr = fmt.Errorf("missing directory")
		return
	}

	key := i.Key
	if key == "" {
		retErr = fmt.Errorf("missing key")
		return
	}

	// Check if the object already exists. If it already exists, we do not want to
	// waste time overwriting the cache.
	attrs, err := c.client.Bucket(bucket).Object(key).Attrs(ctx)
	if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		retErr = fmt.Errorf("failed to check if cached object exists: %w", err)
		return
	}
	if attrs != nil {
		c.log("cached object already exists, skipping")
		return
	}

	// Create the storage writer
	dne := storage.Conditions{DoesNotExist: true}
	gcsw := c.client.Bucket(bucket).Object(key).If(dne).NewWriter(ctx)
	defer func() {
		c.log("closing gcs writer")
		if cerr := gcsw.Close(); cerr != nil {
			if retErr != nil {
				retErr = fmt.Errorf("%v: failed to close gcs writer: %w", retErr, cerr)
				return
			}
			retErr = fmt.Errorf("failed to close gcs writer: %w", cerr)
		}
	}()

	gcsw.ChunkSize = 128_000_000
	gcsw.ObjectAttrs.ContentType = contentType
	gcsw.ObjectAttrs.CacheControl = cacheControl
	gcsw.ProgressFunc = func(soFar int64) {
		fmt.Printf("uploaded %d bytes\n", soFar)
	}

	// Create the tar.zst writer
	files, err := archiver.FilesFromDisk(nil, map[string]string{
		dir: "",
	})
	if err != nil {
		return err
	}

	format := archiver.CompressedArchive{
		Compression: archiver.Zstd{},
		Archival:    archiver.Tar{},
	}

	err = format.Archive(ctx, gcsw, files)
	if err != nil {
		return err
	}

	return
}

// RestoreRequest is used as input to the Restore operation.
type RestoreRequest struct {
	// Bucket is the name of the bucket from which to cache.
	Bucket string

	// Keys is the ordered list of keys to restore.
	Keys []string

	// Dir is the directory on disk to cache.
	Dir string
}

// Restore restores the key from the cache into the dir on disk.
func (c *Cacher) Restore(ctx context.Context, i *RestoreRequest) (retErr error) {
	if i == nil {
		retErr = fmt.Errorf("missing cache options")
		return
	}

	bucket := i.Bucket
	if bucket == "" {
		retErr = fmt.Errorf("missing bucket")
		return
	}

	dir := i.Dir
	if dir == "" {
		retErr = fmt.Errorf("missing directory")
		return
	}

	keys := i.Keys
	if len(keys) < 1 {
		retErr = fmt.Errorf("expected at least one cache key")
		return
	}

	// Get the bucket handle
	bucketHandle := c.client.Bucket(bucket)

	// Try to find an earlier cached item by looking for the "newest" item with
	// one of the provided key fallbacks as a prefix.
	var match *storage.ObjectAttrs
	for _, key := range keys {
		c.log("searching for objects with prefix %s", key)

		it := bucketHandle.Objects(ctx, &storage.Query{
			Prefix: key,
		})

		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				retErr = fmt.Errorf("failed to list %s: %w", key, err)
				return
			}

			c.log("found object %s", key)

			if match == nil || attrs.Updated.After(match.Updated) {
				c.log("setting %s as best candidate", key)
				match = attrs
				continue
			}
		}
	}

	// Ensure we found one
	if match == nil {
		retErr = fmt.Errorf("failed to find cached objects among keys %q", keys)
		return
	}

	// Ensure the output directory exists
	c.log("making target directory %s", dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		retErr = fmt.Errorf("failed to make target directory: %w", err)
		return
	}

	// Create the gcs reader
	gcsr, err := bucketHandle.Object(match.Name).NewReader(ctx)
	if err != nil {
		retErr = fmt.Errorf("failed to create object reader: %w", err)
		return
	}
	defer func() {
		c.log("closing gcs reader")
		if cerr := gcsr.Close(); cerr != nil {
			if retErr != nil {
				retErr = fmt.Errorf("%v: failed to close gcs reader: %w", retErr, cerr)
				return
			}
			retErr = fmt.Errorf("failed to close gcs reader: %w", cerr)
		}
	}()

	format := archiver.CompressedArchive{
		Compression: archiver.Zstd{},
		Archival:    archiver.Tar{},
	}
	fileList := []string(nil)

	handler := func(ctx context.Context, f archiver.File) error {
		hdr, ok := f.Header.(*tar.Header)

		if !ok {
			return nil
		}

		var fpath = filepath.Join(dir, f.NameInArchive)

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(fpath, 0755); err != nil {
				return fmt.Errorf("failed to make directory %s: %w", fpath, err)
			}
			return nil

		case tar.TypeReg, tar.TypeRegA, tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
			if err := os.MkdirAll(filepath.Dir(fpath), 0755); err != nil {
				return fmt.Errorf("failed to make directory %s: %w", filepath.Dir(fpath), err)
			}

			out, err := os.Create(fpath)
			if err != nil {
				return fmt.Errorf("%s: creating new file: %v", fpath, err)
			}
			defer out.Close()

			err = out.Chmod(f.Mode())
			if err != nil && runtime.GOOS != "windows" {
				return fmt.Errorf("%s: changing file mode: %v", fpath, err)
			}

			in, err := f.Open()
			if err != nil {
				return fmt.Errorf("%s: opening file: %v", fpath, err)
			}

			_, err = io.Copy(out, in)
			if err != nil {
				return fmt.Errorf("%s: writing file: %v", fpath, err)
			}
			return nil

		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(fpath), 0755); err != nil {
				return fmt.Errorf("failed to make directory %s: %w", filepath.Dir(fpath), err)
			}

			err = os.Symlink(hdr.Linkname, fpath)
			if err != nil {
				return fmt.Errorf("%s: making symbolic link for: %v", fpath, err)
			}
			return nil

		case tar.TypeLink:
			if err := os.MkdirAll(filepath.Dir(fpath), 0755); err != nil {
				return fmt.Errorf("failed to make directory %s: %w", filepath.Dir(fpath), err)
			}

			err = os.Link(filepath.Join(fpath, hdr.Linkname), fpath)
			if err != nil {
				return fmt.Errorf("%s: making symbolic link for: %v", fpath, err)
			}
			return nil

		case tar.TypeXGlobalHeader:
			return nil // ignore the pax global header from git-generated tarballs
		default:
			return fmt.Errorf("%s: unknown type flag: %c", hdr.Name, hdr.Typeflag)
		}
	}

	format.Extract(ctx, gcsr, fileList, handler)

	return
}

// HashGlob hashes the files matched by the given glob.
func (c *Cacher) HashGlob(pattern string) (string, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", fmt.Errorf("failed to glob: %w", err)
	}
	return c.HashFiles(matches)
}

// HashFiles hashes the list of file and returns the hex-encoded SHA256.
func (c *Cacher) HashFiles(files []string) (string, error) {
	h, err := blake2b.New(16, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create hash: %w", err)
	}

	hashOne := func(name string, h hash.Hash) (retErr error) {
		c.log("opening %s", name)
		f, err := os.Open(name)
		if err != nil {
			retErr = fmt.Errorf("failed to open file: %w", err)
			return
		}
		defer func() {
			c.log("closing %s", name)
			if cerr := f.Close(); cerr != nil {
				if retErr != nil {
					retErr = fmt.Errorf("%v: failed to close file: %w", retErr, cerr)
					return
				}
				retErr = fmt.Errorf("failed to close file: %w", cerr)
			}
		}()

		c.log("stating %s", name)
		stat, err := f.Stat()
		if err != nil {
			retErr = fmt.Errorf("failed to stat file: %w", err)
			return
		}

		if stat.IsDir() {
			c.log("skipping %s (is a directory)", name)
			return
		}

		c.log("hashing %s", name)
		if _, err := io.Copy(h, f); err != nil {
			retErr = fmt.Errorf("failed to hash: %w", err)
			return
		}

		return
	}

	for _, name := range files {
		if err := hashOne(name, h); err != nil {
			return "", fmt.Errorf("failed to hash %s: %w", name, err)
		}
	}

	dig := h.Sum(nil)
	return fmt.Sprintf("%x", dig), nil
}

func (c *Cacher) log(msg string, vars ...interface{}) {
	if c.debug {
		log.Printf(msg, vars...)
	}
}
