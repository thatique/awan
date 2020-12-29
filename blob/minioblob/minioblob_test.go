package minioblob

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/thatique/awan/internal/escape"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/blob/drivertest"
)

const (
	minioAccessKey  = "Q3AM3UQ867SPQQA43P2F"
	minioSecretKey  = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	minioBucketName = "testbucket-awan"
)

type harness struct {
	c      *minio.Client
	opts   *Options
	closer func()
}

func newHarness(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
	c, err := minio.New("play.min.io", &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}
	return &harness{c: c, opts: nil}, nil
}

func newHarnessUsingLegacyList(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
	c, err := minio.New("play.min.io", &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}
	return &harness{c: c, opts: &Options{UseLegacyList: true}}, nil
}

func (h *harness) HTTPClient() *http.Client {
	return &http.Client{}
}

func (h *harness) MakeDriver(ctx context.Context) (driver.Bucket, error) {
	return openBucket(ctx, h.c, minioBucketName, h.opts)
}

func (h *harness) MakeDriverForNonexistentBucket(ctx context.Context) (driver.Bucket, error) {
	return openBucket(ctx, h.c, "bucket-does-not-exist", h.opts)
}

func (h *harness) Close() {
}

func TestEscapeKey(t *testing.T) {
	for _, k := range escape.WeirdStrings {
		s := escapeKey(k, false)
		if !isValidObjectName(s) {
			t.Fatalf("%s is not valid object name", s)
		}
		s2 := unescapeKey(s)
		if s2 != k {
			t.Fatalf("can't reverse escaped string. original: %s, unescaped result: %s", k, s2)
		}
	}
}

func TestConformance(t *testing.T) {
	drivertest.RunConformanceTests(t, newHarness, []drivertest.AsTest{verifyContentLanguage{usingLegacyList: false}})
}

func TestConformanceUsingLegacyList(t *testing.T) {
	drivertest.RunConformanceTests(t, newHarnessUsingLegacyList, []drivertest.AsTest{verifyContentLanguage{usingLegacyList: true}})
}

const language = "nl"

// verifyContentLanguage uses As to access the underlying GCS types and
// read/write the ContentLanguage field.
type verifyContentLanguage struct {
	usingLegacyList bool
}

func (verifyContentLanguage) Name() string {
	return "verify ContentLanguage can be written and read through As"
}

func (verifyContentLanguage) BucketCheck(b *blob.Bucket) error {
	var client *minio.Client
	if !b.As(&client) {
		return errors.New("Bucket.As failed")
	}
	return nil
}

func (verifyContentLanguage) ErrorCheck(b *blob.Bucket, err error) error {
	var e minio.ErrorResponse
	if !b.ErrorAs(err, &e) {
		return errors.New("blob.ErrorAs failed")
	}
	return nil
}

func (verifyContentLanguage) BeforeRead(as func(interface{}) bool) error {
	var req minio.GetObjectOptions
	if !as(&req) {
		return errors.New("BeforeRead As failed")
	}
	return nil
}

func (verifyContentLanguage) BeforeWrite(as func(interface{}) bool) error {
	var req minio.PutObjectOptions
	if !as(&req) {
		return errors.New("BeforeRead As failed")
	}
	return nil
}

func (verifyContentLanguage) BeforeCopy(as func(interface{}) bool) error {
	var dest minio.CopyDestOptions
	var src minio.CopySrcOptions
	if !as(&dest) || !as(&src) {
		return errors.New("BeforeCopy.As failed")
	}
	return nil
}

func (v verifyContentLanguage) BeforeList(as func(interface{}) bool) error {
	// Nothing to do.
	return nil
}

func (v verifyContentLanguage) BeforeSign(as func(interface{}) bool) error {
	// Nothing to do.
	return nil
}

func (verifyContentLanguage) AttributesCheck(attrs *blob.Attributes) error {
	var oi minio.ObjectInfo
	if !attrs.As(&oi) {
		return errors.New("Attributes.As returned false")
	}
	return nil
}

func (verifyContentLanguage) ListObjectCheck(o *blob.ListObject) error {
	if o.IsDir {
		var commonPrefix minio.CommonPrefix
		if !o.As(&commonPrefix) {
			return errors.New("ListObject.As for directory returned false")
		}
		return nil
	}
	var obj minio.ObjectInfo
	if !o.As(&obj) {
		return errors.New("ListObject.As for object returned false")
	}
	if obj.Key == "" || o.Key != obj.Key {
		return errors.New("ListObject.As for object returned a different item")
	}
	// Nothing to check.
	return nil
}

func (verifyContentLanguage) ReaderCheck(r *blob.Reader) error {
	var mo minio.Object
	if !r.As(&mo) {
		return errors.New("Reader.As returned false")
	}

	return nil
}

func isValidObjectName(object string) bool {
	if len(object) == 0 {
		return false
	}
	if strings.HasSuffix(object, "/") {
		return false
	}
	return isValidObjectPrefix(object)
}

func isValidObjectPrefix(object string) bool {
	if hasBadPathComponent(object) {
		return false
	}
	if !utf8.ValidString(object) {
		return false
	}
	// Reject unsupported characters in object name.
	if strings.ContainsAny(object, "\\") {
		return false
	}
	return true
}

// Check if the incoming path has bad path components,
// such as ".." and "."
func hasBadPathComponent(path string) bool {
	path = strings.TrimSpace(path)
	for _, p := range strings.Split(path, "/") {
		switch strings.TrimSpace(p) {
		case "..":
			return true
		case ".":
			return true
		}
	}
	return false
}
