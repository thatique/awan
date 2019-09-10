package s3blob

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio-go/v6"
	"github.com/thatique/awan/blob/driver"
	"github.com/thatique/awan/internal/escape"
	"github.com/thatique/awan/verr"
)

const defaultPageSize = 1000

type lazyCredsOpener struct {
	init   sync.Once
	opener *URLOpener
	err    error
}

func (o *lazyCredsOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	o.init.Do(func() {
		// take the credentials from env
		accessKey := os.GetEnv("AWS_S3_ACCESS_KEY")
		if accessKey == "" {
			o.err = errors.New("s3blob: environment variable AWS_S3_ACCESS_KEY not set")
		}
		secretKey := os.GetEnv("AWS_S3_SECRET_KEY")
		if secretKey == "" {
			o.err = errors.New("s3blob: environment variable AWS_S3_SECRET_KEY not set")
		}
		o.URLOpener = &URLOpener{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}
	})
	if o.err != nil {
		return nil, fmt.Errorf("open bucket %v: %v", u, o.err)
	}
	return o.opener.OpenBucketURL(ctx, u)
}

type URLOpener struct {
	AccessKey, SecretKey string
	Options              Options
}

func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	q := u.Query()

	useSSL := false
	if i, err := strconv.Atoi(q.Get("ssl")); err != nil && i > 0 {
		useSSL = true
	}
	client, err := minio.New(u.Host, o.AccessKey, o.SecretKey, useSSL)
	if err != nil {
		return nil, fmt.Errorf("open bucket %v: %v", u, err)
	}
	options := &u.Options{}
	if i, err := strconv.Atoi(q.Get("legacylist")); err != nil && i > 0 {
		options.UseLegacyList = true
	}
	bucketName := u.Path
	i := 0
	e := -1
	if bucketName[0] == '/' {
		i += 1
	}
	if bucketName[len(bucketName)-1] == '/' {
		e -= 1
	}
	bucketName = bucketName[i:e]
	return OpenBucket(ctx, client, bucketName, options)
}

// Options sets options for constructing a *blob.Bucket backed by s3.
type Options struct {
	// UseLegacyList forces the use of ListObjects instead of ListObjectsV2.
	// ListObjectsV2.
	UseLegacyList bool
}

type bucket struct {
	name          string
	client        *minio.Client
	core          *minio.Core
	useLegacyList bool
}

// OpenBucket returns a *blob.Bucket backed by S3.
// See the package documentation for an example.
func OpenBucket(ctx context.Context, client *minio.Client, bucketName string, opts *Options) (*blob.Bucket, error) {
	drv, err := openBucket(ctx, sess, bucketName, opts)
	if err != nil {
		return nil, err
	}
	return blob.NewBucket(drv), nil
}

func openBucket(ctx context.Context, client *minio.Client, bucketName string, opts *Options) (*bucket, error) {
	if client == nil {
		return nil, errors.New("s3blob.OpenBucket: client is required")
	}
	if bucketName == "" {
		return nil, errors.New("s3blob.OpenBucket: bucketName is required")
	}
	if opts == nil {
		opts = &Options{}
	}
	return &bucket{name: bucketName, client: client, core: &minio.Core{client}, useLegacyList: opts.UseLegacyList}
}

type reader struct {
	body  io.ReadCloser
	attrs driver.ReaderAttributes
}

func (r *reader) Read(p []byte) (int, error) {
	return r.body.Read(p)
}

// Close closes the reader itself. It must be called when done reading.
func (r *reader) Close() error {
	return r.body.Close()
}

func (r *reader) Attributes() *driver.ReaderAttributes {
	return &r.attrs
}

type writer struct {
	c *minio.Client
	w *io.PipeWriter // created when the first byte is written

	ctx        context.Context
	bucketName string
	objectName string

	opts  minio.PutObjectOptions
	donec chan struct{} // closed when done writing
	// The following fields will be written before donec closes:
	err error
}

func (w *writer) Write(p []byte) (int, error) {
	// Avoid opening the pipe for a zero-length write;
	// the concrete can do these for empty blobs.
	if len(p) == 0 {
		return 0, nil
	}
	if w.w == nil {
		// We'll write into pw and use pr as an io.Reader for the
		// Upload call to S3.
		pr, pw := io.Pipe()
		w.w = pw
		if err := w.open(pr); err != nil {
			return 0, err
		}
	}
	select {
	case <-w.donec:
		return 0, w.err
	default:
	}
	return w.w.Write(p)
}

func (w *writer) open(pr *io.PipeReader) error {
	go func() {
		defer close(w.donec)
		var r io.Reader
		if pr == nil {
			r = http.NoBody
		} else {
			r = pr
		}
		_, err := w.c.PutObjectWithContext(w.ctx, w.bucketName, w.objectName, r, -1, w.opts)
		if err != nil {
			w.err = err
			if pr != nil {
				pr.closeWithError(err)
			}
			return
		}
	}()
	return nil
}

// Close completes the writer and closes it. Any error occurring during write
// will be returned. If a writer is closed before any Write is called, Close
// will create an empty file at the given key.
func (w *writer) Close() error {
	if w.w == nil {
		// We never got any bytes written.
		w.open(nil)
	} else if err := w.w.Close(); err != nil {
		return err
	}
	<-w.donec
	return w.err
}

func (b *bucket) Close() error {
	return nil
}

func (b *bucket) ErrorCode(err error) verr.ErrorCode {
	reserr, ok := err.(minio.ErrorResponse)
	if !ok {
		return verr.Unknown
	}
	switch reserr.StatusCode {
	case http.StatusNotFound:
		return verr.NotFound
	case http.StatusForbidden, http.StatusUnauthorized:
		return verr.PermissionDenied
	case http.StatusMethodNotAllowed:
		return verr.InvalidArgument
	default:
		return verr.Unknown
	}
}

func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	key = escapeKey(key)
	info, err := b.client.StatObject(b.name, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	attr, metadata := extractMetadata(info.Metadata)
	return &driver.Attributes{
		CacheControl:       attr.cacheControl,
		ContentDisposition: attr.contentDisposition,
		ContentEncoding:    attr.contentEncoding,
		ContentLanguage:    attr.contentLanguage,
		ContentType:        info.ContentType,
		Metadata:           metadata,
		ModTime:            info.LastModified,
		Size:               info.Size,
		MD5:                eTagToMD5(&info.ETag),
		ETag:               info.ETag,
	}, nil
}

func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	if opts.Prefix != "" {
		opts.Prefix = escapeKey(opts.Prefix)
	}
	if opts.Delimiter != "" {
		opts.Delimiter = escapeKey(opts.Delimiter)
	}
	resp, err := b.listObjects(ctx, opts)
	if err != nil {
		return nil, err
	}
	page := driver.ListPage{}
	if res.ContinuationToken != "" {
		page.NextPageToken = []byte(*resp.NextContinuationToken)
	}
	if n := len(res.Contents) + len(res.CommonPrefixes); n > 0 {
		page.Objects = make([]*driver.ListObject, n)
		for i, obj := range res.Contents {
			page.Objects[i] = &driver.ListObject{
				Key:     unescapeKey(obj.Key),
				ModTime: obj.LastModified,
				Size:    obj.Size,
				MD5:     eTagToMD5(obj.ETag),
				ETag:    obj.ETag,
			}
		}
		for i, prefix := range res.CommonPrefixes {
			page.Objects[i+len(resp.Contents)] = &driver.ListObject{
				Key:   unescapeKey(prefix.Prefix),
				IsDir: true,
			}
		}
		if len(resp.Contents) > 0 && len(resp.CommonPrefixes) > 0 {
			// S3 gives us blobs and "directories" in separate lists; sort them.
			sort.Slice(page.Objects, func(i, j int) bool {
				return page.Objects[i].Key < page.Objects[j].Key
			})
		}
	}
	return &page, nil
}

func (b *bucket) listObjects(ctx context.Context, opts *driver.ListOptions) (minio.ListBucketV2Result, error) {
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	if !b.useLegacyList {
		return b.core.ListObjectsV2(b.name, opts.Prefix, string(opts.PageToken), true, opts.Delimiter, pageSize, "")
	}

	res, err := b.core.ListObjects(b.name, opts.Prefix, string(opts.PageToken), opts.Delimiter, pageSize)
	if err != nil {
		return minio.ListBucketV2Result{}, err
	}
	var nextContinuationToken string
	if res.NextMarker != "" {
		nextContinuationToken = res.NextMarker
	} else if res.IsTruncated {
		nextContinuationToken = res.Contents[len(res.Contents)-1].Key
	}
	return minio.ListObjectsV2{
		CommonPrefixes:        res.CommonPrefixes,
		Contents:              res.Contents,
		NextContinuationToken: nextContinuationToken,
	}, nil
}

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	key = escapeKey(key)
	objectOptions := minio.GetObjectOptions{}
	objectOptions.SetRange(offset, len)

	obj, err := b.GetObjectWithContext(ctx, b.name, key, objectOptions)
	if err != nil {
		return nil, err
	}
	info, err := obj.Stat()
	if err != nil {
		return nil, err
	}
	return &reader{
		body: obj,
		attrs: driver.ReaderAttributes{
			ContentType: info.ContentType,
			ModTime:     info.LastModified,
			Size:        getSize(info),
		},
	}, nil
}

func (b *bucket) NewTypedWriter(ctx context.Context, key, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	key = escapeKey(key)
	md := make(map[string]*string, len(opts.Metadata))
	for k, v := range opts.Metadata {
		// See the package comments for more details on escaping of metadata
		// keys & values.
		k = escape.HexEscape(url.PathEscape(k), func(runes []rune, i int) bool {
			c := runes[i]
			return c == '@' || c == ':' || c == '='
		})
		md[k] = url.PathEscape(v)
	}
	putOpts := minio.PutObjectOptions{
		ContentType:  contentType,
		UserMetadata: md,
	}
	if opts.CacheControl != "" {
		putOpts.CacheControl = opts.CacheControl
	}
	if opts.ContentDisposition != "" {
		putOpts.ContentDisposition = opts.ContentDisposition
	}
	if opts.ContentEncoding != "" {
		putOpts.ContentEncoding = opts.ContentEncoding
	}
	if opts.ContentLanguage != "" {
		putOpts.ContentLanguage = opts.ContentLanguage
	}

	return &writer{
		c:          b.client,
		ctx:        ctx,
		bucketName: b.name,
		objectName: key,
		opts:       putOpts,
		donec:      make(chan struct{}),
	}, nil
}

func (b *bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	dstKey = escapeKey(dstKey)
	srcKey = escapeKey(srcKey)
	srcInfo := minio.NewSourceInfo(b.name, srcKey)
	dstInfo, err := minio.NewDestinationInfo(b.name, dstKey, nil, nil)
	if err != nil {
		return err
	}

	return b.client.CopyObject(dstInfo, srcInfo)
}

func (b *bucket) Delete(ctx context.Context, key string) error {
	if _, err := b.Attributes(ctx, key); err != nil {
		return err
	}
	key = escapeKey(key)
	return b.client.RemoveObject(b.name, key)
}

func (b *bucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	key = escapeKey(key)
	url, err := b.client.Presign(opts.Method, b.name, key, opts.Expiry, nil)
	if err != nil {
		return "", err
	}
	return url.String(), nil
}

// escapeKey does all required escaping for UTF-8 strings to work with S3.
func escapeKey(key string) string {
	return escape.HexEscape(key, func(r []rune, i int) bool {
		c := r[i]
		switch {
		// S3 doesn't handle these characters (determined via experimentation).
		case c < 32:
			return true
		// For "../", escape the trailing slash.
		case i > 1 && c == '/' && r[i-1] == '.' && r[i-2] == '.':
			return true
		// For "//", escape the trailing slash. Otherwise, S3 drops it.
		case i > 0 && c == '/' && r[i-1] == '/':
			return true
		}
		return false
	})
}

// unescapeKey reverses escapeKey.
func unescapeKey(key string) string {
	return escape.HexUnescape(key)
}

type objectAttr struct {
	cacheControl       string
	contentDisposition string
	contentEncoding    string
	contentLanguage    string
}

func extractMetadata(h http.Header) (objectAttr, map[string]string) {
	var keys = []string{
		"Cache-Control",
		"Content-Disposition",
		"Content-Encoding",
		"Content-Language",
	}
	return objectAttr{
		cacheControl:       h.Get("Cache-Control"),
		contentDisposition: h.Get("Content-Disposition"),
		contentEncoding:    h.Get("Content-Encoding"),
		contentLanguage:    h.Get("Content-Language"),
	}, filterHeader(h, keys)
}

// make a copy of http.Header
func cloneHeader(h http.Header) http.Header {
	h2 := make(http.Header, len(h))
	for k, vv := range h {
		vv2 := make([]string, len(vv))
		copy(vv2, vv)
		h2[k] = vv2
	}
	return h2
}

// Filter relevant response headers from
// the HEAD, GET http response. The function takes
// a list of headers which are filtered out and
// returned as a new http header.
func filterHeader(header http.Header, filterKeys []string) (filteredHeader http.Header) {
	filteredHeader = cloneHeader(header)
	for _, key := range filterKeys {
		filteredHeader.Del(key)
	}
	return filteredHeader
}

// etagToMD5 processes an ETag header and returns an MD5 hash if possible.
// S3's ETag header is sometimes a quoted hexstring of the MD5. Other times,
// notably when the object was uploaded in multiple parts, it is not.
// We do the best we can.
// Some links about ETag:
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
// https://github.com/aws/aws-sdk-net/issues/815
// https://teppen.io/2018/06/23/aws_s3_etags/
func eTagToMD5(etag *string) []byte {
	if etag == nil {
		// No header at all.
		return nil
	}
	// Strip the expected leading and trailing quotes.
	quoted := *etag
	if quoted[0] != '"' || quoted[len(quoted)-1] != '"' {
		return nil
	}
	unquoted := quoted[1 : len(quoted)-1]
	// Un-hex; we return nil on error. In particular, we'll get an error here
	// for multi-part uploaded blobs, whose ETag will contain a "-" and so will
	// never be a legal hex encoding.
	md5, err := hex.DecodeString(unquoted)
	if err != nil {
		return nil
	}
	return md5
}

func getSize(obj minio.ObjectInfo) int64 {
	size := obj.Size
	if cr := obj.Metadata.Get("Content-Range"); cr != "" {
		parts := strings.Split(cr, "/")
		if len(parts) == 2 {
			if i, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
				size = i
			}
		}
	}
	return size
}
