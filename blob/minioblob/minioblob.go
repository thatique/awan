package minioblob

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/thatique/awan/internal/escape"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"gocloud.dev/gcerrors"
)

const (
	defaultPageSize = 1000
	// Scheme of this blob implementation
	Scheme = "minio"
)

func init() {
	blob.DefaultURLMux().RegisterBucket(Scheme, new(URLOpener))
}

// Options supported by this blob
type Options struct {
	// UseLegacyList forces the use of ListObjects instead of ListObjectsV2.
	// ListObjectsV2.
	UseLegacyList bool
}

// URLOpener implements blob url opener for minio
type URLOpener struct {
	Options Options
}

type bucket struct {
	name          string
	core          *minio.Core
	client        *minio.Client
	useLegacyList bool
}

// OpenBucketURL open bucket
func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	q := u.Query()

	useSSL := false
	if i, err := strconv.Atoi(q.Get("ssl")); err != nil && i > 0 {
		useSSL = true
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewEnvMinio(),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("open bucket %v: %v", u, err)
	}
	options := &Options{}
	if i, err := strconv.Atoi(q.Get("legacylist")); err != nil && i > 0 {
		options.UseLegacyList = true
	}
	bucketName := u.Path
	i := 0
	e := -1
	if bucketName[0] == '/' {
		i++
	}
	if bucketName[len(bucketName)-1] == '/' {
		e--
	}
	bucketName = bucketName[i:e]
	return OpenBucket(ctx, client, bucketName, options)
}

// OpenBucket returns a *blob.Bucket backed by S3.
// See the package documentation for an example.
func OpenBucket(ctx context.Context, client *minio.Client, bucketName string, opts *Options) (*blob.Bucket, error) {
	drv, err := openBucket(ctx, client, bucketName, opts)
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
	return &bucket{name: bucketName, client: client, core: &minio.Core{client}, useLegacyList: opts.UseLegacyList}, nil
}

type reader struct {
	body  io.ReadCloser
	raw   minio.Object
	attrs driver.ReaderAttributes
}

// Close closes the reader itself. It must be called when done reading.
func (r *reader) Close() error {
	return r.body.Close()
}

func (r *reader) Attributes() *driver.ReaderAttributes {
	return &r.attrs
}

func (r *reader) Read(p []byte) (int, error) {
	return r.body.Read(p)
}

func (r *reader) As(i interface{}) bool {
	p, ok := i.(*minio.Object)
	if !ok {
		return false
	}
	*p = r.raw
	return true
}

type writer struct {
	c *minio.Client
	w *io.PipeWriter

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
		_, err := w.c.PutObject(w.ctx, w.bucketName, w.objectName, r, -1, w.opts)
		if err != nil {
			w.err = err
			if pr != nil {
				pr.CloseWithError(err)
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

func (b *bucket) ErrorCode(err error) gcerrors.ErrorCode {
	reserr := minio.ToErrorResponse(err)
	switch {
	case reserr.Code == "AccessDenied":
		return gcerrors.PermissionDenied
	case reserr.Code == "NoSuchKey" || reserr.Code == "NotFound" || reserr.Code == "NoSuchBucket":
		return gcerrors.NotFound
	default:
		return gcerrors.Unknown
	}
}

func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	prefix := ""
	if opts.Prefix != "" {
		prefix = escapeKey(opts.Prefix, true)
	}
	delimiter := ""
	if opts.Delimiter != "" {
		delimiter = escapeKey(opts.Delimiter, true)
	}
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	res, err := b.listObjects(ctx, prefix, string(opts.PageToken), delimiter, pageSize)
	if err != nil {
		return nil, err
	}
	page := driver.ListPage{}
	if res.NextContinuationToken != "" {
		page.NextPageToken = []byte(res.NextContinuationToken)
	}
	if n := len(res.Contents) + len(res.CommonPrefixes); n > 0 {
		page.Objects = make([]*driver.ListObject, n)
		for i, obj := range res.Contents {
			page.Objects[i] = &driver.ListObject{
				Key:     unescapeKey(obj.Key),
				ModTime: obj.LastModified,
				Size:    obj.Size,
				MD5:     eTagToMD5(&obj.ETag),
				AsFunc: func(i interface{}) bool {
					p, ok := i.(*minio.ObjectInfo)
					if !ok {
						return false
					}
					*p = obj
					return true
				},
			}
		}
		for i, prefix := range res.CommonPrefixes {
			page.Objects[i+len(res.Contents)] = &driver.ListObject{
				Key:   unescapeKey(prefix.Prefix),
				IsDir: true,
				AsFunc: func(i interface{}) bool {
					p, ok := i.(*minio.CommonPrefix)
					if !ok {
						return false
					}
					*p = prefix
					return true
				},
			}
		}
		if len(res.Contents) > 0 && len(res.CommonPrefixes) > 0 {
			// S3 gives us blobs and "directories" in separate lists; sort them.
			sort.Slice(page.Objects, func(i, j int) bool {
				return page.Objects[i].Key < page.Objects[j].Key
			})
		}
	}
	return &page, nil
}

func (b *bucket) listObjects(ctx context.Context, prefix, token, delimiter string, pageSize int) (minio.ListBucketV2Result, error) {
	if !b.useLegacyList {
		return b.core.ListObjectsV2(b.name, prefix, token, true, delimiter, pageSize)
	}

	res, err := b.core.ListObjects(b.name, prefix, token, delimiter, pageSize)
	if err != nil {
		return minio.ListBucketV2Result{}, err
	}
	var nextContinuationToken string
	if res.NextMarker != "" {
		nextContinuationToken = res.NextMarker
	} else if res.IsTruncated {
		nextContinuationToken = res.Contents[len(res.Contents)-1].Key
	}
	return minio.ListBucketV2Result{
		CommonPrefixes:        res.CommonPrefixes,
		Contents:              res.Contents,
		NextContinuationToken: nextContinuationToken,
	}, nil
}

// As implements driver.As.
func (b *bucket) As(i interface{}) bool {
	p, ok := i.(**minio.Client)
	if !ok {
		return false
	}
	*p = b.client
	return true
}

// As implements driver.ErrorAs.
func (b *bucket) ErrorAs(err error, i interface{}) bool {
	switch v := err.(type) {
	case minio.ErrorResponse:
		if p, ok := i.(*minio.ErrorResponse); ok {
			*p = v
			return true
		}
	}
	return false
}

func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	key = escapeKey(key, false)
	info, err := b.client.StatObject(ctx, b.name, key, minio.StatObjectOptions{})
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
		AsFunc: func(i interface{}) bool {
			p, ok := i.(*minio.ObjectInfo)
			if !ok {
				return false
			}
			*p = info
			return true
		},
	}, nil
}

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	key = escapeKey(key, false)
	objectOptions := minio.GetObjectOptions{}
	if offset > 0 && length < 0 {
		objectOptions.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	} else if length == 0 {
		// AWS doesn't support a zero-length read; we'll read 1 byte and then
		// ignore it in favor of http.NoBody below.
		objectOptions.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset))
	} else if length >= 0 {
		objectOptions.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	}

	if opts.BeforeRead != nil {
		asFunc := func(i interface{}) bool {
			if p, ok := i.(*minio.GetObjectOptions); ok {
				*p = objectOptions
				return true
			}
			return false
		}
		if err := opts.BeforeRead(asFunc); err != nil {
			return nil, err
		}
	}

	obj, err := b.client.GetObject(ctx, b.name, key, objectOptions)
	if err != nil {
		return nil, err
	}
	info, err := obj.Stat()

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
	key = escapeKey(key, false)
	md := make(map[string]string, len(opts.Metadata))
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

	if opts.BeforeWrite != nil {
		asFunc := func(i interface{}) bool {
			po, ok := i.(*minio.PutObjectOptions)
			if ok {
				*po = putOpts
				return true
			}
			return false
		}

		if err := opts.BeforeWrite(asFunc); err != nil {
			return nil, err
		}
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
	dstKey = escapeKey(dstKey, false)
	srcKey = escapeKey(srcKey, false)

	dstInfo := minio.CopyDestOptions{
		Bucket: b.name,
		Object: dstKey,
	}
	srcInfo := minio.CopySrcOptions{
		Bucket: b.name,
		Object: srcKey,
	}

	if opts.BeforeCopy != nil {
		asFunc := func(i interface{}) bool {
			switch v := i.(type) {
			case *minio.CopyDestOptions:
				*v = dstInfo
				return true
			case *minio.CopySrcOptions:
				*v = srcInfo
			}
			return false
		}

		if err := opts.BeforeCopy(asFunc); err != nil {
			return err
		}
	}

	_, err := b.client.CopyObject(ctx, dstInfo, srcInfo)
	return err
}

func (b *bucket) Delete(ctx context.Context, key string) error {
	if _, err := b.Attributes(ctx, key); err != nil {
		return err
	}
	key = escapeKey(key, false)
	return b.client.RemoveObject(ctx, b.name, key, minio.RemoveObjectOptions{})
}

func (b *bucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	key = escapeKey(key, false)
	url, err := b.client.Presign(ctx, opts.Method, b.name, key, opts.Expiry, nil)
	if err != nil {
		return "", err
	}
	return url.String(), nil
}

// escapeKey does all required escaping for UTF-8 strings to work with S3.
func escapeKey(key string, isPrefix bool) string {
	return escape.HexEscape(key, func(r []rune, i int) bool {
		c := r[i]
		switch {
		// S3 doesn't handle these characters (determined via experimentation).
		case c < 32:
			return true
		// these chars supported by S3 but Minio didn't support it well, so escape them
		case c == '\n' || c == '^' || c == '*' || c == '|' || c == '\\' || c == '"':
			return true
		// Escape the trailing slash in a key, Minio didn't like that
		case !isPrefix && c == '/' && i == len(r)-1:
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
	filtered := filterHeader(h, keys)
	metadata := make(map[string]string, len(filtered))
	for k, _ := range filtered {
		if strings.HasPrefix(k, "X-Amz-Meta-") {
			mk := strings.TrimPrefix(k, "X-Amz-Meta-")
			metadata[escape.HexUnescape(escape.URLUnescape(mk))] = escape.URLUnescape(filtered.Get(k))
		}
	}

	return objectAttr{
		cacheControl:       h.Get("Cache-Control"),
		contentDisposition: h.Get("Content-Disposition"),
		contentEncoding:    h.Get("Content-Encoding"),
		contentLanguage:    h.Get("Content-Language"),
	}, metadata
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
