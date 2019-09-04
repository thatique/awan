package fileblob

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/thatique/awan/blob"
	"github.com/thatique/awan/blob/driver"
	blobutil "github.com/thatique/awan/internal/blob"
	"github.com/thatique/awan/internal/escape"
	"github.com/thatique/awan/verr"
)

const (
	// fileBlobSysTmp prefix is for save metadata sent by Initialize Multipart Upload API.
	fileBlobSysTmp = "fileblob.sys.tmp"

	// defaultPagesize returned when listing object
	defaultPageSize = 1000

	Scheme = "file"
)

func init() {
	blob.DefaultURLMux().RegisterBucket(Scheme, &URLOpener{})
}

// URLOpener opens file bucket URLs like "file:///foo/bar/baz".
//
// The URL's host is ignored.
//
// If os.PathSeparator != "/", any leading "/" from the path is dropped
// and remaining '/' characters are converted to os.PathSeparator.
//
// No query options are supported. Examples:
//
//  - file:///a/directory
//    -> Passes "/a/directory" to OpenBucket.
//  - file://localhost/a/directory
//    -> Also passes "/a/directory".
//  - file:///c:/foo/bar on Windows.
//    -> Passes "c:\foo\bar".
//  - file://localhost/c:/foo/bar on Windows.
//    -> Also passes "c:\foo\bar".
type URLOpener struct{}

// OpenBucketURL opens a blob.Bucket based on u.
func (*URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	for param := range u.Query() {
		return nil, fmt.Errorf("open bucket %v: invalid query parameter %q", u, param)
	}
	path := u.Path
	if os.PathSeparator != '/' {
		path = strings.TrimPrefix(path, "/")
	}
	return OpenBucket(filepath.FromSlash(path), nil)
}

// Options sets options for constructing a *blob.Bucket backed by fileblob.
type Options struct {
	// URLSigner implements signing URLs (to allow access to a resource without
	// further authorization) and verifying that a given URL is unexpired and
	// contains a signature produced by the URLSigner.
	// URLSigner is only required for utilizing the SignedURL API.
	URLSigner URLSigner
}

var _ driver.Bucket = &bucket{}

type bucket struct {
	dir  string
	opts *Options
}

// openBucket creates a driver.Bucket that reads and writes to dir.
// dir must exist.
func openBucket(dir string, opts *Options) (driver.Bucket, error) {
	dir = filepath.Clean(dir)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dir)
	}
	if opts == nil {
		opts = &Options{}
	}
	return &bucket{dir: dir, opts: opts}, nil
}

// OpenBucket creates a *blob.Bucket backed by the filesystem and rooted at
// dir, which must exist. See the package documentation for an example.
func OpenBucket(dir string, opts *Options) (*blob.Bucket, error) {
	drv, err := openBucket(dir, opts)
	if err != nil {
		return nil, err
	}
	return blob.NewBucket(drv), nil
}

func (b *bucket) ErrorCode(err error) verr.ErrorCode {
	switch {
	case os.IsNotExist(err):
		return verr.NotFound
	default:
		return verr.Unknown
	}
}

func escapeBlobKey(s string) string {
	s = escape.HexEscape(s, func(r []rune, i int) bool {
		c := r[i]
		switch {
		case c < 32:
			return true
		// We're going to replace '/' with os.PathSeparator below. In order for this
		// to be reversible, we need to escape raw os.PathSeparators.
		case os.PathSeparator != '/' && c == os.PathSeparator:
			return true
		// For "../", escape the trailing slash.
		case i > 1 && c == '/' && r[i-1] == '.' && r[i-2] == '.':
			return true
		// For "//", escape the trailing slash.
		case i > 0 && c == '/' && r[i-1] == '/':
			return true
		// Escape the trailing slash in a key.
		case c == '/' && i == len(r)-1:
			return true
		// https://docs.microsoft.com/en-us/windows/desktop/fileio/naming-a-file
		case os.PathSeparator == '\\' && (c == '>' || c == '<' || c == ':' || c == '"' || c == '|' || c == '?' || c == '*'):
			return true
		}
		return false
	})
	// Replace "/" with os.PathSeparator if needed, so that the local filesystem
	// can use subdirectories.
	if os.PathSeparator != '/' {
		s = strings.Replace(s, "/", string(os.PathSeparator), -1)
	}
	return s
}

// unescapeKey reverses escapeKey.
func unescapeBlobKey(s string) string {
	if os.PathSeparator != '/' {
		s = strings.Replace(s, string(os.PathSeparator), "/", -1)
	}
	s = escape.HexUnescape(s)
	return s
}

// path returns the full path for a key
func (b *bucket) path(key string) (string, error) {
	path := filepath.Join(b.dir, escapeBlobKey(key))
	if strings.HasSuffix(path, attrsExt) {
		return "", errAttrsExt
	}
	return path, nil
}

// forKey returns the full path, os.FileInfo, and attributes for key.
func (b *bucket) forKey(key string) (string, os.FileInfo, *xattrs, error) {
	path, err := b.path(key)
	if err != nil {
		return "", nil, nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", nil, nil, err
	}
	xa, err := getAttrs(path)
	if err != nil {
		return "", nil, nil, err
	}
	return path, info, &xa, nil
}

func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	var pageToken string
	if len(opts.PageToken) > 0 {
		pageToken = string(opts.PageToken)
	}
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	// If opts.Delimiter != "", lastPrefix contains the last "directory" key we
	// added. It is used to avoid adding it again; all files in this "directory"
	// are collapsed to the single directory entry.
	var lastPrefix string

	// Do a full recursive scan of the root directory.
	var result driver.ListPage
	err := filepath.Walk(b.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Couldn't read this file/directory for some reason; just skip it.
			return nil
		}
		// Skip the self-generated attribute files.
		if strings.HasSuffix(path, attrsExt) {
			return nil
		}

		// os.Walk returns the root directory; skip it.
		if path == b.dir {
			return nil
		}

		// Strip the <b.dir> prefix from path; +1 is to include the separator.
		path = path[len(b.dir)+1:]
		// Unescape the path to get the key.
		key := unescapeBlobKey(path)
		// Skip all directories. If opts.Delimiter is set, we'll create
		// pseudo-directories later.
		// Note that returning nil means that we'll still recurse into it;
		// we're just not adding a result for the directory itself.
		if info.IsDir() {
			key += "/"
			if strings.HasPrefix(key, fileBlobSysTmp) {
				return filepath.SkipDir
			}
			// Avoid recursing into subdirectories if the directory name already
			// doesn't match the prefix; any files in it are guaranteed not to match.
			if len(key) > len(opts.Prefix) && !strings.HasPrefix(key, opts.Prefix) {
				return filepath.SkipDir
			}
			// Similarly, avoid recursing into subdirectories if we're making
			// "directories" and all of the files in this subdirectory are guaranteed
			// to collapse to a "directory" that we've already added.
			if lastPrefix != "" && strings.HasPrefix(key, lastPrefix) {
				return filepath.SkipDir
			}
			return nil
		}
		// Skip files/directories that don't match the Prefix.
		if !strings.HasPrefix(key, opts.Prefix) {
			return nil
		}
		var (
			md5  []byte
			etag string
		)
		if xa, err := getAttrs(path); err == nil {
			// Note: we only have the MD5 hash for blobs that we wrote.
			// For other blobs, md5 will remain nil.
			md5 = xa.MD5
			etag = xa.ETag
		}
		obj := &driver.ListObject{
			Key:     key,
			ModTime: info.ModTime(),
			Size:    info.Size(),
			MD5:     md5,
			ETag:    etag,
		}
		// If using Delimiter, collapse "directories".
		if opts.Delimiter != "" {
			// Strip the prefix, which may contain Delimiter.
			keyWithoutPrefix := key[len(opts.Prefix):]
			// See if the key still contains Delimiter.
			// If no, it's a file and we just include it.
			// If yes, it's a file in a "sub-directory" and we want to collapse
			// all files in that "sub-directory" into a single "directory" result.
			if idx := strings.Index(keyWithoutPrefix, opts.Delimiter); idx != -1 {
				prefix := opts.Prefix + keyWithoutPrefix[0:idx+len(opts.Delimiter)]
				// We've already included this "directory"; don't add it.
				if prefix == lastPrefix {
					return nil
				}
				// Update the object to be a "directory".
				obj = &driver.ListObject{
					Key:   prefix,
					IsDir: true,
				}
				lastPrefix = prefix
			}
		}
		// If there's a pageToken, skip anything before it.
		if pageToken != "" && obj.Key <= pageToken {
			return nil
		}
		// If we've already got a full page of results, set NextPageToken and stop.
		if len(result.Objects) == pageSize {
			result.NextPageToken = []byte(result.Objects[pageSize-1].Key)
			return io.EOF
		}
		result.Objects = append(result.Objects, obj)
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return &result, nil
}

func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	_, info, xa, err := b.forKey(key)
	if err != nil {
		return &driver.Attributes{}, err
	}
	return &driver.Attributes{
		CacheControl:       xa.CacheControl,
		ContentDisposition: xa.ContentDisposition,
		ContentEncoding:    xa.ContentEncoding,
		ContentLanguage:    xa.ContentLanguage,
		ContentType:        xa.ContentType,
		Metadata:           xa.Metadata,
		ModTime:            info.ModTime(),
		Size:               info.Size(),
		MD5:                xa.MD5,
		ETag:               xa.ETag,
	}, nil
}

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	path, info, xa, err := b.forKey(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	r := io.Reader(f)
	if length >= 0 {
		r = io.LimitReader(r, length)
	}
	return &reader{
		r: r,
		c: f,
		attrs: &driver.ReaderAttributes{
			ContentType: xa.ContentType,
			ModTime:     info.ModTime(),
			Size:        info.Size(),
		},
	}, nil
}

type reader struct {
	r     io.Reader
	c     io.Closer
	attrs *driver.ReaderAttributes
}

func (r *reader) Read(p []byte) (int, error) {
	if r.r == nil {
		return 0, io.EOF
	}
	return r.r.Read(p)
}

func (r *reader) Close() error {
	if r.c == nil {
		return nil
	}
	return r.c.Close()
}

func (r *reader) Attributes() *driver.ReaderAttributes {
	return r.attrs
}

// NewTypedWriter implements driver.NewTypedWriter.
func (b *bucket) NewTypedWriter(ctx context.Context, key string, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	path, err := b.path(key)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		return nil, err
	}
	f, err := ioutil.TempFile(filepath.Dir(path), "fileblob")
	if err != nil {
		return nil, err
	}
	var metadata map[string]string
	if len(opts.Metadata) > 0 {
		metadata = opts.Metadata
	}
	attrs := xattrs{
		CacheControl:       opts.CacheControl,
		ContentDisposition: opts.ContentDisposition,
		ContentEncoding:    opts.ContentEncoding,
		ContentLanguage:    opts.ContentLanguage,
		ContentType:        contentType,
		Metadata:           metadata,
	}
	w := &writer{
		ctx:        ctx,
		f:          f,
		path:       path,
		attrs:      attrs,
		contentMD5: opts.ContentMD5,
		md5hash:    md5.New(),
	}
	return w, nil
}

type writer struct {
	ctx        context.Context
	f          *os.File
	path       string
	attrs      xattrs
	contentMD5 []byte
	// We compute the MD5 hash so that we can store it with the file attributes,
	// not for verification.
	md5hash hash.Hash
}

func (w *writer) Write(p []byte) (n int, err error) {
	if _, err := w.md5hash.Write(p); err != nil {
		return 0, err
	}
	return w.f.Write(p)
}

func (w *writer) Close() error {
	err := w.f.Close()
	if err != nil {
		return err
	}
	// Always delete the temp file. On success, it will have been renamed so
	// the Remove will fail.
	defer func() {
		_ = os.Remove(w.f.Name())
	}()

	// Check if the write was cancelled.
	if err := w.ctx.Err(); err != nil {
		return err
	}

	md5sum := w.md5hash.Sum(nil)
	w.attrs.MD5 = md5sum
	w.attrs.ETag = blobutil.ToS3ETag(hex.EncodeToString(md5sum))

	// Write the attributes file.
	if err := setAttrs(w.path, w.attrs); err != nil {
		return err
	}
	// Rename the temp file to path.
	if err := os.Rename(w.f.Name(), w.path); err != nil {
		_ = os.Remove(w.path + attrsExt)
		return err
	}
	return nil
}

func (b *bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	// Note: we could use NewRangedReader here, but since we need to copy all of
	// the metadata (from xa), it's more efficient to do it directly.
	srcPath, _, xa, err := b.forKey(srcKey)
	if err != nil {
		return err
	}
	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// We'll write the copy using Writer, to avoid re-implementing making of a
	// temp file, cleaning up after partial failures, etc.
	wopts := driver.WriterOptions{
		CacheControl:       xa.CacheControl,
		ContentDisposition: xa.ContentDisposition,
		ContentEncoding:    xa.ContentEncoding,
		ContentLanguage:    xa.ContentLanguage,
		Metadata:           xa.Metadata,
	}
	// Create a cancelable context so we can cancel the write if there are
	// problems.
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := b.NewTypedWriter(writeCtx, dstKey, xa.ContentType, &wopts)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, f)
	if err != nil {
		cancel() // cancel before Close cancels the write
		w.Close()
		return err
	}
	return w.Close()
}

// Delete implements driver.Delete.
func (b *bucket) Delete(ctx context.Context, key string) error {
	path, err := b.path(key)
	if err != nil {
		return err
	}
	err = os.Remove(path)
	if err != nil {
		return err
	}
	if err = os.Remove(path + attrsExt); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// SignedURL implements driver.SignedURL
func (b *bucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	if b.opts.URLSigner == nil {
		return "", errors.New("sign fileblob url: bucket does not have an Options.URLSigner")
	}
	surl, err := b.opts.URLSigner.URLFromKey(ctx, key, opts)
	if err != nil {
		return "", err
	}
	return surl.String(), nil
}

func (b *bucket) Close() error {
	return nil
}