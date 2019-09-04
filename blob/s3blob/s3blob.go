package s3blob

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio-go/v6"
	"github.com/thatique/awan/internal/escape"
	"github.com/thatique/awan/blob/driver"
	"github.com/thatique/awan/verr"
)

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
	return OpenBucket(ctx, client, bucketName, &u.Options)
}

// Options sets options for constructing a *blob.Bucket backed by s3.
type Options struct {
	// UseLegacyList forces the use of ListObjects instead of ListObjectsV2.
	// ListObjectsV2.
	UseLegacyList bool
}

type bucket struct {
	name   string
	client *minio.Client
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
	return &bucket{name: bucketName, client: client}
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

func extractMetadata(info minio.ObjectInfo) map[string]string {
	metadata := make(map[string]string, len(info.Metadata))
	for k, _ : range info.Metadata {
		metadata[escape.HexUnescape(escape.URLUnescape(k)] = escape.URLUnescape(info.Metadata.Get(k))
	}
	return metadata
}