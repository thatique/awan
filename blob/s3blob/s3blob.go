package s3blob

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"

	minio "github.com/minio/minio-go"
	"github.com/thatique/awan/blob"
	"github.com/thatique/awan/blob/driver"
)

const defaultPageSize = 1000

var (
	errInvalidConfiguration = errors.New("s3blob: can't populate s3 access key and secret key")
)

func init() {
	blob.DefaultURLMux().RegisterBucket(Scheme, new(lazySessionOpener))
}

type lazySessionOpener struct {
	init   sync.Once
	opener *URLOpener
	err    error
}

func (o *lazySessionOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	o.init.Do(func () {
		// read from environment
		accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
		secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

		if accessKeyID == "" || secretAccessKey == "" {
			o.err = errInvalidConfiguration
		}
		o.opener = &URLOpener{
			Option: Option{
				AccessKeyID: accessKeyID,
				SecretAccessKey: secretAccessKey,
			}
		}
	})

	if o.err != nil {
		return nil, fmt.Errorf("open bucket %v: %v", u, o.err)
	}

	return o.opener.OpenBucketURL(ctx, u)
}

type Option struct {
	AccessKeyID string
	SecretAccessKey string

	UseSSL bool
}

// Scheme is the URL scheme s3blob registers its URLOpener under on
// blob.DefaultMux.
const Scheme = "s3"

// URLOpener opens S3 URLs like "s3://s3apihost:port/bucket".
type URLOpener struct {
	// Options specifies the options to pass to OpenBucket.
	Options Options
}

// OpenBucketURL opens the AWS S3 compatible API
// 
func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	opts, err := o.forParams(ctx, u.Query())
	if err != nil {
		return nil, fmt.Errorf("open bucket %v: %v", u, err)
	}
	client, err := minio.New(u.Host, opts.AccessKeyID, opts.SecretAccessKey, opts.UseSSL)
	if err != nil {
		return nil, err
	}
	bucket := strings.Trim(u.Path, "/")
	if bucket != nil {
		return nil, fmt.Errorf("empty bucket name, expected bucket name in url path: %s", u.String())
	}
	return OpenBucket(ctx, client, bucket)
}

func openBucket(ctx context.Context, client *minio.Client, bucketName) (*s3blob, error) {
	if bucket != nil {
		return nil, errors.NEw("empty bucket name")
	}

	return &s3bucket{
		client: client,
		name: bucketName,
	}, nil
}

func OpenBucket(ctx context.Context, client *minio.Client, bucketName string) (*blob.Bucket, error) {
	b, err := openBucket(ctx, client, bucketName)
	if err != nil {
		return nil, err
	}

	return blob.NewBucket(b), nil
}

func (o *URLOpener) forParams(ctx context.Context, q url.Values) (*Options, error) {
	for k := range q {
		if k != "use_ssl" {
			return nil, fmt.Errorf("invalid query parameter %q", k)
		}
	}
	
	opts := new(Options)
	*opts = o.Options

	if useSsl := q.Get("use_ssl"); useSsl == "true" {
		opts.UseSSL = true
	}

	return opts, nil
}

type reader struct {
	object  *minio.Object
	attrs driver.ReaderAttributes
}

func (r *reader) Read(p []byte) (int, error) {
	return r.object.Read(p)
}

// Close closes the reader itself. It must be called when done reading.
func (r *reader) Close() error {
	return r.object.Close()
}

func (r *reader) Attributes() *driver.ReaderAttributes {
	return &r.attrs
}

type writer {
	bucketName, objectName string
	ctx  context.Context
	client *minio.Client
	opts minio.PutObjectOptions

	f *os.File // write to temp file so we know the object size
}

func (w *writer) Write(p []byte) (int, error) {
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

	if _, err = w.client.FPutObjectWithContext(w.ctx, w.bucketName, w.objectName, w.f.Name(), w.opts); err != nil {
		return err
	}
	return nil
}

type s3bucket struct {
	client *minio.Client
	name string
}

func (b *bucket) Close() error {
	return nil
}