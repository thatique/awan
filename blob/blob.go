package blob

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/thatique/awan/blob/driver"
	"github.com/thatique/awan/internal/trace"
	"github.com/thatique/awan/verr"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// Reader reads bytes from a blob. It implements io.ReadCloser, and must be closed after
// reads are finished.
type Reader struct {
	b        driver.Bucket
	r        driver.Reader
	end      func(error) // called at Close to finish trace and metric collection
	provider string      // for metric collection
	closed   bool
}

func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	stats.RecordWithTags(context.Background(), []tag.Mutator{tag.Upsert(trace.ProviderKey, r.provider)},
		bytesReadMeasure.M(int64(n)))

	return n, wrapError(r.b, err)
}

// Close implements io.Closer (https://golang.org/pkg/io/#Closer).
func (r *Reader) Close() error {
	r.closed = true
	err := wrapError(r.b, r.r.Close())
	r.end(err)
	return err
}

// ContentType returns the MIME type of the blob.
func (r *Reader) ContentType() string {
	return r.r.Attributes().ContentType
}

// ModTime returns the time the blob was last modified.
func (r *Reader) ModTime() time.Time {
	return r.r.Attributes().ModTime
}

// Size returns the size of the blob content in bytes.
func (r *Reader) Size() int64 {
	return r.r.Attributes().Size
}

// Attributes contains attributes about a blob.
type Attributes struct {
	// CacheControl specifies caching attributes that providers may use
	// when serving the blob.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control
	CacheControl string
	// ContentDisposition specifies whether the blob content is expected to be
	// displayed inline or as an attachment.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition
	ContentDisposition string
	// ContentEncoding specifies the encoding used for the blob's content, if any.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding
	ContentEncoding string
	// ContentLanguage specifies the language used in the blob's content, if any.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Language
	ContentLanguage string
	// ContentType is the MIME type of the blob object. It must not be empty.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type
	ContentType string
	// Metadata holds key/value pairs associated with the blob.
	// Keys will be lowercased by the portable type before being returned
	// to the user. If there are duplicate case-insensitive keys (e.g.,
	// "foo" and "FOO"), only one value will be kept, and it is undefined
	// which one.
	Metadata map[string]string
	// ModTime is the time the blob object was last modified.
	ModTime time.Time
	// Size is the size of the object in bytes.
	Size int64
	// MD5 is an MD5 hash of the blob contents or nil if not available.
	MD5 []byte
	// Etag is the HTTP/1.1 Entity tag for the object. This field is readonly
	ETag string
	// List of individual parts, maximum size of upto 10,000
	Parts []ObjectPartInfo
}

// ObjectPartInfo Info of each part kept in the multipart metadata
// file after CompleteMultipartUpload() is called.
type ObjectPartInfo struct {
	Number     int
	Name       string
	ETag       string
	Size       int64
	ActualSize int64
}

// Writer writes bytes to a blob.
//
// It implements io.WriteCloser (https://golang.org/pkg/io/#Closer), and must be
// closed after all writes are done.
type Writer struct {
	b          driver.Bucket
	w          driver.Writer
	end        func(error) // called at Close to finish trace and metric collection
	cancel     func()      // cancels the ctx provided to NewTypedWriter if contentMD5 verification fails
	contentMD5 []byte
	md5hash    hash.Hash
	provider   string // for metric collection
	closed     bool

	// These fields exist only when w is not yet created.
	//
	// A ctx is stored in the Writer since we need to pass it into NewTypedWriter
	// when we finish detecting the content type of the blob and create the
	// underlying driver.Writer. This step happens inside Write or Close and
	// neither of them take a context.Context as an argument. The ctx is set
	// to nil after we have passed it to NewTypedWriter.
	ctx  context.Context
	key  string
	opts *driver.WriterOptions
	buf  *bytes.Buffer
}

// sniffLen is the byte size of Writer.buf used to detect content-type.
const sniffLen = 512

// Write implements the io.Writer interface (https://golang.org/pkg/io/#Writer).
//
// Writes may happen asynchronously, so the returned error can be nil
// even if the actual write eventually fails. The write is only guaranteed to
// have succeeded if Close returns no error.
func (w *Writer) Write(p []byte) (n int, err error) {
	if len(w.contentMD5) > 0 {
		if _, err := w.md5hash.Write(p); err != nil {
			return 0, err
		}
	}
	if w.w != nil {
		return w.write(p)
	}

	// If w is not yet created due to no content-type being passed in, try to sniff
	// the MIME type based on at most 512 bytes of the blob content of p.

	// Detect the content-type directly if the first chunk is at least 512 bytes.
	if w.buf.Len() == 0 && len(p) >= sniffLen {
		return w.open(p)
	}

	// Store p in w.buf and detect the content-type when the size of content in
	// w.buf is at least 512 bytes.
	w.buf.Write(p)
	if w.buf.Len() >= sniffLen {
		return w.open(w.buf.Bytes())
	}
	return len(p), nil
}

// Close closes the blob writer. The write operation is not guaranteed to have succeeded until
// Close returns with no error.
// Close may return an error if the context provided to create the Writer is
// canceled or reaches its deadline.
func (w *Writer) Close() (err error) {
	w.closed = true
	defer func() { w.end(err) }()
	if len(w.contentMD5) > 0 {
		// Verify the MD5 hash of what was written matches the ContentMD5 provided
		// by the user.
		md5sum := w.md5hash.Sum(nil)
		if !bytes.Equal(md5sum, w.contentMD5) {
			// No match! Return an error, but first cancel the context and call the
			// driver's Close function to ensure the write is aborted.
			w.cancel()
			if w.w != nil {
				_ = w.w.Close()
			}
			return verr.Newf(verr.FailedPrecondition, nil, "blob: the WriterOptions.ContentMD5 you specified (%X) did not match what was written (%X)", w.contentMD5, md5sum)
		}
	}

	defer w.cancel()
	if w.w != nil {
		return wrapError(w.b, w.w.Close())
	}
	if _, err := w.open(w.buf.Bytes()); err != nil {
		return err
	}
	return wrapError(w.b, w.w.Close())
}

// open tries to detect the MIME type of p and write it to the blob.
// The error it returns is wrapped.
func (w *Writer) open(p []byte) (int, error) {
	ct := http.DetectContentType(p)
	var err error
	if w.w, err = w.b.NewTypedWriter(w.ctx, w.key, ct, w.opts); err != nil {
		return 0, wrapError(w.b, err)
	}
	w.buf = nil
	w.ctx = nil
	w.key = ""
	w.opts = nil
	return w.write(p)
}

func (w *Writer) write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	stats.RecordWithTags(context.Background(), []tag.Mutator{tag.Upsert(trace.ProviderKey, w.provider)},
		bytesWrittenMeasure.M(int64(n)))
	return n, wrapError(w.b, err)
}

// ListOptions sets options for listing blobs via Bucket.List.
type ListOptions struct {
	// Prefix indicates that only blobs with a key starting with this prefix
	// should be returned.
	Prefix string
	// Delimiter sets the delimiter used to define a hierarchical namespace,
	// like a filesystem with "directories". It is highly recommended that you
	// use "" or "/" as the Delimiter. Other values should work through this API,
	// but provider UIs generally assume "/".
	//
	// An empty delimiter means that the bucket is treated as a single flat
	// namespace.
	//
	// A non-empty delimiter means that any result with the delimiter in its key
	// after Prefix is stripped will be returned with ListObject.IsDir = true,
	// ListObject.Key truncated after the delimiter, and zero values for other
	// ListObject fields. These results represent "directories". Multiple results
	// in a "directory" are returned as a single result.
	Delimiter string
}

// ListIterator iterates over List results.
type ListIterator struct {
	b       *Bucket
	opts    *driver.ListOptions
	page    *driver.ListObjectsInfo
	nextIdx int
}

// ObjectInfo represents a specific blob object returned from List.
type ObjectInfo struct {
	// Key is the key for this blob.
	Key string
	// ModTime is the time the blob object was last modified.
	ModTime time.Time
	// Size is the size of the object in bytes.
	Size int64
	// MD5 is an MD5 hash of the blob contents or nil if not available.
	MD5 []byte
	// Etag is the HTTP/1.1 Entity tag for the object. This field is readonly
	Etag string
	// IsDir indicates that this result represents a "directory" in the
	// hierarchical namespace, ending in ListOptions.Delimiter. Key can be
	// passed as ListOptions.Prefix to list items in the "directory".
	// Fields other than Key and IsDir will not be set if IsDir is true.
	IsDir bool
}

// Next returns a *ObjectInfo for the next blob. It returns (nil, io.EOF) if
// there are no more.
func (i *ListIterator) Next(ctx context.Context) (*ObjectInfo, error) {
	if i.page != nil {
		// We've already got a page of results.
		if i.nextIdx < len(i.page.Objects) {
			// Next object is in the page; return it.
			dobj := i.page.Objects[i.nextIdx]
			i.nextIdx++
			return &ObjectInfo{
				Key:     dobj.Key,
				ModTime: dobj.ModTime,
				Size:    dobj.Size,
				MD5:     dobj.MD5,
				IsDir:   dobj.IsDir,
			}, nil
		}
		if len(i.page.NextPageToken) == 0 {
			// Done with current page, and there are no more; return io.EOF.
			return nil, io.EOF
		}
		// We need to load the next page.
		i.opts.PageToken = i.page.NextPageToken
	}
	i.b.mu.RLock()
	defer i.b.mu.RUnlock()
	if i.b.closed {
		return nil, errClosed
	}
	// Loading a new page.
	p, err := i.b.b.ListPaged(ctx, i.opts)
	if err != nil {
		return nil, wrapError(i.b.b, err)
	}
	i.page = p
	i.nextIdx = 0
	return i.Next(ctx)
}

// Bucket provides an easy and portable way to interact with blobs
// within a "bucket", including read, write, and list operations.
// To create a Bucket, use constructors found in provider-specific
// subpackages.
type Bucket struct {
	b      driver.Bucket
	tracer *trace.Tracer

	// mu protects the closed variable.
	// Read locks are kept to prevent closing until a call finishes.
	mu     sync.RWMutex
	closed bool
}

const pkgName = "github.com/thatique/awan/blog"

var (
	latencyMeasure      = trace.LatencyMeasure(pkgName)
	bytesReadMeasure    = stats.Int64(pkgName+"/bytes_read", "Total bytes read", stats.UnitBytes)
	bytesWrittenMeasure = stats.Int64(pkgName+"/bytes_written", "Total bytes written", stats.UnitBytes)

	// OpenCensusViews are predefined views for OpenCensus metrics.
	// The views include counts and latency distributions for API method calls,
	// and total bytes read and written.
	// See the example at https://godoc.org/go.opencensus.io/stats/view for usage.
	OpenCensusViews = append(
		trace.Views(pkgName, latencyMeasure),
		&view.View{
			Name:        pkgName + "/bytes_read",
			Measure:     bytesReadMeasure,
			Description: "Sum of bytes read from the provider service.",
			TagKeys:     []tag.Key{trace.ProviderKey},
			Aggregation: view.Sum(),
		},
		&view.View{
			Name:        pkgName + "/bytes_written",
			Measure:     bytesWrittenMeasure,
			Description: "Sum of bytes written to the provider service.",
			TagKeys:     []tag.Key{trace.ProviderKey},
			Aggregation: view.Sum(),
		})
)

var errClosed = verr.Newf(verr.FailedPrecondition, nil, "blob: Bucket has been closed")

// NewBucket is intended for use by provider implementations.
var NewBucket = newBucket

// newBucket creates a new *Bucket based on a specific driver implementation.
// End users should use subpackages to construct a *Bucket instead of this
// function; see the package documentation for details.
func newBucket(b driver.Bucket) *Bucket {
	return &Bucket{
		b: b,
		tracer: &trace.Tracer{
			Package:        pkgName,
			Provider:       trace.ProviderName(b),
			LatencyMeasure: latencyMeasure,
		},
	}
}

// ReadAll is a shortcut for creating a Reader via NewReader with nil
// ReaderOptions, and reading the entire blob.
func (b *Bucket) ReadAll(ctx context.Context, key string) (_ []byte, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, errClosed
	}
	r, err := b.NewReader(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

// List returns a ListIterator that can be used to iterate over blobs in a
// bucket, in lexicographical order of UTF-8 encoded keys. The underlying
// implementation fetches results in pages.
//
// A nil ListOptions is treated the same as the zero value.
//
// List is not guaranteed to include all recently-written blobs;
// some providers are only eventually consistent.
func (b *Bucket) List(opts *ListOptions) *ListIterator {
	if opts == nil {
		opts = &ListOptions{}
	}
	dopts := &driver.ListOptions{
		Prefix:    opts.Prefix,
		Delimiter: opts.Delimiter,
	}
	return &ListIterator{b: b, opts: dopts}
}

// Exists returns true if a blob exists at key, false if it does not exist, or
// an error.
// It is a shortcut for calling Attributes and checking if it returns an error
// with code gcerrors.NotFound.
func (b *Bucket) Exists(ctx context.Context, key string) (bool, error) {
	_, err := b.Attributes(ctx, key)
	if err == nil {
		return true, nil
	}
	if verr.Code(err) == verr.NotFound {
		return false, nil
	}
	return false, err
}

// Attributes returns attributes for the blob stored at key.
//
// If the blob does not exist, Attributes returns an error for which
// gcerrors.Code will return gcerrors.NotFound.
func (b *Bucket) Attributes(ctx context.Context, key string) (_ Attributes, err error) {
	if !utf8.ValidString(key) {
		return Attributes{}, verr.Newf(verr.InvalidArgument, nil, "blob: Attributes key must be a valid UTF-8 string: %q", key)
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return Attributes{}, errClosed
	}
	ctx = b.tracer.Start(ctx, "Attributes")
	defer func() { b.tracer.End(ctx, err) }()

	a, err := b.b.Attributes(ctx, key)
	if err != nil {
		return Attributes{}, wrapError(b.b, err)
	}
	var md map[string]string
	if len(a.Metadata) > 0 {
		// Providers are inconsistent, but at least some treat keys
		// as case-insensitive. To make the behavior consistent, we
		// force-lowercase them when writing and reading.
		md = make(map[string]string, len(a.Metadata))
		for k, v := range a.Metadata {
			md[strings.ToLower(k)] = v
		}
	}
	var parts []ObjectPartInfo
	for _, part := range a.Parts {
		parts = append(parts, partInfoFromDriver(part))
	}
	return Attributes{
		CacheControl:       a.CacheControl,
		ContentDisposition: a.ContentDisposition,
		ContentEncoding:    a.ContentEncoding,
		ContentLanguage:    a.ContentLanguage,
		ContentType:        a.ContentType,
		Metadata:           md,
		ModTime:            a.ModTime,
		Size:               a.Size,
		MD5:                a.MD5,
		ETag:               a.ETag,
		Parts:              parts,
	}, nil
}

// NewReader is a shortcut for NewRangedReader with offset=0 and length=-1.
func (b *Bucket) NewReader(ctx context.Context, key string, opts *ReaderOptions) (*Reader, error) {
	return b.newRangeReader(ctx, key, 0, -1, opts)
}

// NewRangeReader returns a Reader to read content from the blob stored at key.
// It reads at most length bytes starting at offset (>= 0).
// If length is negative, it will read till the end of the blob.
//
// If the blob does not exist, NewRangeReader returns an error for which
// verr.Code will return verr.NotFound. Exists is a lighter-weight way
// to check for existence.
//
// A nil ReaderOptions is treated the same as the zero value.
//
// The caller must call Close on the returned Reader when done reading.
func (b *Bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *ReaderOptions) (_ *Reader, err error) {
	return b.newRangeReader(ctx, key, offset, length, opts)
}

func (b *Bucket) newRangeReader(ctx context.Context, key string, offset, length int64, opts *ReaderOptions) (_ *Reader, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, errClosed
	}
	if offset < 0 {
		return nil, verr.Newf(verr.InvalidArgument, nil, "blob: NewRangeReader offset must be non-negative (%d)", offset)
	}
	if !utf8.ValidString(key) {
		return nil, verr.Newf(verr.InvalidArgument, nil, "blob: NewRangeReader key must be a valid UTF-8 string: %q", key)
	}
	if opts == nil {
		opts = &ReaderOptions{}
	}
	dopts := &driver.ReaderOptions{}
	tctx := b.tracer.Start(ctx, "NewRangeReader")
	defer func() {
		// If err == nil, we handed the end closure off to the returned *Writer; it
		// will be called when the Writer is Closed.
		if err != nil {
			b.tracer.End(tctx, err)
		}
	}()
	dr, err := b.b.NewRangeReader(ctx, key, offset, length, dopts)
	if err != nil {
		return nil, wrapError(b.b, err)
	}
	end := func(err error) { b.tracer.End(tctx, err) }
	r := &Reader{b: b.b, r: dr, end: end, provider: b.tracer.Provider}
	_, file, lineno, ok := runtime.Caller(2)
	runtime.SetFinalizer(r, func(r *Reader) {
		if !r.closed {
			var caller string
			if ok {
				caller = fmt.Sprintf(" (%s:%d)", file, lineno)
			}
			log.Printf("A blob.Reader reading from %q was never closed%s", key, caller)
		}
	})
	return r, nil
}

// WriteAll is a shortcut for creating a Writer via NewWriter and writing p.
//
// If opts.ContentMD5 is not set, WriteAll will compute the MD5 of p and use it
// as the ContentMD5 option for the Writer it creates.
func (b *Bucket) WriteAll(ctx context.Context, key string, p []byte, opts *WriterOptions) (err error) {
	realOpts := new(WriterOptions)
	if opts != nil {
		*realOpts = *opts
	}
	if len(realOpts.ContentMD5) == 0 {
		sum := md5.Sum(p)
		realOpts.ContentMD5 = sum[:]
	}
	w, err := b.NewWriter(ctx, key, realOpts)
	if err != nil {
		return err
	}
	if _, err := w.Write(p); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

// NewWriter returns a Writer that writes to the blob stored at key.
// A nil WriterOptions is treated the same as the zero value.
//
// If a blob with this key already exists, it will be replaced.
// The blob being written is not guaranteed to be readable until Close
// has been called; until then, any previous blob will still be readable.
// Even after Close is called, newly written blobs are not guaranteed to be
// returned from List; some providers are only eventually consistent.
//
// The returned Writer will store ctx for later use in Write and/or Close.
// To abort a write, cancel ctx; otherwise, it must remain open until
// Close is called.
//
// The caller must call Close on the returned Writer, even if the write is
// aborted.
func (b *Bucket) NewWriter(ctx context.Context, key string, opts *WriterOptions) (_ *Writer, err error) {
	if !utf8.ValidString(key) {
		return nil, verr.Newf(verr.InvalidArgument, nil, "blob: NewWriter key must be a valid UTF-8 string: %q", key)
	}
	if opts == nil {
		opts = &WriterOptions{}
	}
	dopts := &driver.WriterOptions{
		CacheControl:       opts.CacheControl,
		ContentDisposition: opts.ContentDisposition,
		ContentEncoding:    opts.ContentEncoding,
		ContentLanguage:    opts.ContentLanguage,
		ContentMD5:         opts.ContentMD5,
		BufferSize:         opts.BufferSize,
	}
	if len(opts.Metadata) > 0 {
		// Providers are inconsistent, but at least some treat keys
		// as case-insensitive. To make the behavior consistent, we
		// force-lowercase them when writing and reading.
		md := make(map[string]string, len(opts.Metadata))
		for k, v := range opts.Metadata {
			if k == "" {
				return nil, verr.Newf(verr.InvalidArgument, nil, "blob: WriterOptions.Metadata keys may not be empty strings")
			}
			if !utf8.ValidString(k) {
				return nil, verr.Newf(verr.InvalidArgument, nil, "blob: WriterOptions.Metadata keys must be valid UTF-8 strings: %q", k)
			}
			if !utf8.ValidString(v) {
				return nil, verr.Newf(verr.InvalidArgument, nil, "blob: WriterOptions.Metadata values must be valid UTF-8 strings: %q", v)
			}
			lowerK := strings.ToLower(k)
			if _, found := md[lowerK]; found {
				return nil, verr.Newf(verr.InvalidArgument, nil, "blob: WriterOptions.Metadata has a duplicate case-insensitive metadata key: %q", lowerK)
			}
			md[lowerK] = v
		}
		dopts.Metadata = md
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, errClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	tctx := b.tracer.Start(ctx, "NewWriter")
	end := func(err error) { b.tracer.End(tctx, err) }
	defer func() {
		if err != nil {
			end(err)
		}
	}()

	w := &Writer{
		b:          b.b,
		end:        end,
		cancel:     cancel,
		key:        key,
		opts:       dopts,
		buf:        bytes.NewBuffer([]byte{}),
		contentMD5: opts.ContentMD5,
		md5hash:    md5.New(),
		provider:   b.tracer.Provider,
	}
	if opts.ContentType != "" {
		t, p, err := mime.ParseMediaType(opts.ContentType)
		if err != nil {
			cancel()
			return nil, err
		}
		ct := mime.FormatMediaType(t, p)
		dw, err := b.b.NewTypedWriter(ctx, key, ct, dopts)
		if err != nil {
			cancel()
			return nil, wrapError(b.b, err)
		}
		w.w = dw
	} else {
		// Save the fields needed to called NewTypedWriter later, once we've gotten
		// sniffLen bytes.
		w.ctx = ctx
		w.key = key
		w.opts = dopts
		w.buf = bytes.NewBuffer([]byte{})
	}
	_, file, lineno, ok := runtime.Caller(1)
	runtime.SetFinalizer(w, func(w *Writer) {
		if !w.closed {
			var caller string
			if ok {
				caller = fmt.Sprintf(" (%s:%d)", file, lineno)
			}
			log.Printf("A blob.Writer writing to %q was never closed%s", key, caller)
		}
	})
	return w, nil
}

// Copy the blob stored at srcKey to dstKey.
// A nil CopyOptions is treated the same as the zero value.
//
// If the source blob does not exist, Copy returns an error for which
// gcerrors.Code will return gcerrors.NotFound.
//
// If the destination blob already exists, it is overwritten.
func (b *Bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *CopyOptions) (err error) {
	if !utf8.ValidString(srcKey) {
		return verr.Newf(verr.InvalidArgument, nil, "blob: Copy srcKey must be a valid UTF-8 string: %q", srcKey)
	}
	if !utf8.ValidString(dstKey) {
		return verr.Newf(verr.InvalidArgument, nil, "blob: Copy dstKey must be a valid UTF-8 string: %q", dstKey)
	}
	dopts := &driver.CopyOptions{}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return errClosed
	}
	ctx = b.tracer.Start(ctx, "Copy")
	defer func() { b.tracer.End(ctx, err) }()
	return wrapError(b.b, b.b.Copy(ctx, dstKey, srcKey, dopts))
}

// Delete deletes the blob stored at key.
//
// If the blob does not exist, Delete returns an error for which
// gcerrors.Code will return gcerrors.NotFound.
func (b *Bucket) Delete(ctx context.Context, key string) (err error) {
	if !utf8.ValidString(key) {
		return verr.Newf(verr.InvalidArgument, nil, "blob: Delete key must be a valid UTF-8 string: %q", key)
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return errClosed
	}
	ctx = b.tracer.Start(ctx, "Delete")
	defer func() { b.tracer.End(ctx, err) }()
	return wrapError(b.b, b.b.Delete(ctx, key))
}

// SignedURL returns a URL that can be used to GET the blob for the duration
// specified in opts.Expiry.
//
// A nil SignedURLOptions is treated the same as the zero value.
//
// It is valid to call SignedURL for a key that does not exist.
//
// If the provider implementation does not support this functionality, SignedURL
// will return an error for which gcerrors.Code will return gcerrors.Unimplemented.
func (b *Bucket) SignedURL(ctx context.Context, key string, opts *SignedURLOptions) (string, error) {
	if !utf8.ValidString(key) {
		return "", verr.Newf(verr.InvalidArgument, nil, "blob: SignedURL key must be a valid UTF-8 string: %q", key)
	}
	if opts == nil {
		opts = &SignedURLOptions{}
	}
	if opts.Expiry < 0 {
		return "", verr.Newf(verr.InvalidArgument, nil, "blob: SignedURLOptions.Expiry must be >= 0 (%v)", opts.Expiry)
	}
	if opts.Expiry == 0 {
		opts.Expiry = DefaultSignedURLExpiry
	}
	dopts := driver.SignedURLOptions{
		Expiry: opts.Expiry,
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return "", errClosed
	}
	url, err := b.b.SignedURL(ctx, key, &dopts)
	return url, wrapError(b.b, err)
}

// Close releases any resources used for the bucket.
func (b *Bucket) Close() error {
	b.mu.Lock()
	prev := b.closed
	b.closed = true
	b.mu.Unlock()
	if prev {
		return errClosed
	}
	return b.b.Close()
}

// DefaultSignedURLExpiry is the default duration for SignedURLOptions.Expiry.
const DefaultSignedURLExpiry = 1 * time.Hour

// SignedURLOptions sets options for SignedURL.
type SignedURLOptions struct {
	// Expiry sets how long the returned URL is valid for.
	// Defaults to DefaultSignedURLExpiry.
	Expiry time.Duration
}

// ReaderOptions sets options for NewReader and NewRangedReader.
// It is provided for future extensibility.
type ReaderOptions struct{}

// CopyOptions controls options for Copy. It's provided for future extensibility.
type CopyOptions struct{}

// WriterOptions sets options for NewWriter.
type WriterOptions struct {
	// BufferSize changes the default size in bytes of the chunks that
	// Writer will upload in a single request; larger blobs will be split into
	// multiple requests.
	//
	// This option may be ignored by some provider implementations.
	//
	// If 0, the provider implementation will choose a reasonable default.
	//
	// If the Writer is used to do many small writes concurrently, using a
	// smaller BufferSize may reduce memory usage.
	BufferSize int

	// CacheControl specifies caching attributes that providers may use
	// when serving the blob.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control
	CacheControl string

	// ContentDisposition specifies whether the blob content is expected to be
	// displayed inline or as an attachment.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition
	ContentDisposition string

	// ContentEncoding specifies the encoding used for the blob's content, if any.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding
	ContentEncoding string

	// ContentLanguage specifies the language used in the blob's content, if any.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Language
	ContentLanguage string

	// ContentType specifies the MIME type of the blob being written. If not set,
	// it will be inferred from the content using the algorithm described at
	// http://mimesniff.spec.whatwg.org/.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type
	ContentType string

	// ContentMD5 is used as a message integrity check.
	// If len(ContentMD5) > 0, the MD5 hash of the bytes written must match
	// ContentMD5, or Close will return an error without completing the write.
	// https://tools.ietf.org/html/rfc1864
	ContentMD5 []byte

	// Metadata holds key/value strings to be associated with the blob, or nil.
	// Keys may not be empty, and are lowercased before being written.
	// Duplicate case-insensitive keys (e.g., "foo" and "FOO") will result in
	// an error.
	Metadata map[string]string
}

func partInfoFromDriver(part driver.ObjectPartInfo) ObjectPartInfo {
	return ObjectPartInfo{
		Number:     part.Number,
		Name:       part.Name,
		ETag:       part.ETag,
		Size:       part.Size,
		ActualSize: part.ActualSize,
	}
}

func wrapError(b driver.Bucket, err error) error {
	if err == nil {
		return nil
	}
	if verr.DoNotWrap(err) {
		return err
	}
	return verr.New(b.ErrorCode(err), err, 2, "blob")
}
