package s3blob

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/minio/minio-go/v6"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"

	"github.com/thatique/awan/blob/driver"
	"github.com/thatique/awan/blob/drivertest"
	"github.com/thatique/awan/internal/escape"
)

const (
	minioAccessKey  = "AKIAPGHOWLGSFH4TALBQ"
	minioSecretKey  = "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"
	minioBucketName = "testbucket"
	minioRegion     = "us-west-1"
)

type harness struct {
	c      *minio.Client
	opts    *Options
	closer  func()
}

func newHarness(ctx context.Context, host string, t *testing.T) (drivertest.Harness, error) {
	c, err := minio.New(host, minioAccessKey, minioSecretKey, false)
	if err != nil {
		return nil, err
	}
	return &harness{c: c, opts: nil}, nil
}

func newHarnessUsingLegacyList(ctx context.Context, host string,  t *testing.T) (drivertest.Harness, error) {
	c, err := minio.New(host, minioAccessKey, minioSecretKey, false)
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

func (h *harness) Close() {
	objectsCh := make(chan string)
	go func() {
		defer close(objectsCh)

		doneCh := make(chan struct{})

		// Indicate to our routine to exit cleanly upon return.
		defer close(doneCh)

		// List all objects from a bucket-name with a matching prefix.
		for object := range h.c.ListObjects(minioBucketName, "", true, doneCh) {
			if object.Err != nil {
				log.Fatalln(object.Err)
			}
			objectsCh <- object.Key
		}
	}()
	errorCh := h.c.RemoveObjects(minioBucketName, objectsCh)
	// Print errors received from RemoveObjects API
	for e := range errorCh {
		log.Fatalln("Failed to remove " + e.ObjectName + ", error: " + e.Err.Error())
	}
}

func TestEscapeKey(t *testing.T) {
	for _, k := range escape.WeirdStrings {
		s := escapeKey(k, false)
		if !isValidObjectName(s) {
			t.Fatalf("%s is not valid object name", s)
		}
		s2 :=  unescapeKey(s)
		if s2 != k {
			t.Fatalf("can't reverse escaped string. original: %s, unescaped result: %s", k, s2)
		}
	}
}

func TestConformance(t *testing.T) {
	closer, host := prepareMinioServer()
	defer closer()
	t.Run("TestConformance", func(t *testing.T) {
		drivertest.RunConformanceTests(t, func(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
			return newHarness(ctx, host, t)
		})
	})
	t.Run("ConformanceUsingLegacyList", func(t *testing.T) {
		drivertest.RunConformanceTests(t, func(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
			return newHarnessUsingLegacyList(ctx, host, t)
		})
	})
}

func prepareMinioServer() (func(), string) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatal(err)
	}
	options := &dockertest.RunOptions{
	    Repository: "minio/minio",
	    Tag:        "latest",
	    Cmd:        []string{"server", "/data"},
	    PortBindings: map[dc.Port][]dc.PortBinding{
	        "9000": []dc.PortBinding{{HostPort: "9000"}},
	    },
	    Env: []string{
	    	fmt.Sprintf("MINIO_ACCESS_KEY=%s", minioAccessKey),
	    	fmt.Sprintf("MINIO_SECRET_KEY=%s", minioSecretKey),
	    },
	}
	resource, err := pool.RunWithOptions(options)
	if err != nil {
		log.Fatal(err)
	}
	host := fmt.Sprintf("localhost:%s", resource.GetPort("9000/tcp"))
	cleanup := func() {
		pool.Purge(resource)
	}
	setup := func() error {
		c, err := minio.New(host, minioAccessKey, minioSecretKey, false)
		if err != nil {
			return err
		}
		return c.MakeBucket(minioBucketName, minioRegion)
	}
	if pool.Retry(setup); err != nil {
		cleanup()
		log.Fatal(err)
	}
	return cleanup, host
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