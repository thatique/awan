package fileblob

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/thatique/awan/blob/driver"
	blobutil "github.com/thatique/awan/internal/blob"
)

func init() {
	globalMinPartSize = 5
}

func TestCompleteMultipartUpload(t *testing.T) {
	temp, err := ioutil.TempDir("", "filebblob-test")
	defer func() {
		os.RemoveAll(temp)
	}()
	objectName := "object"
	if err != nil {
		t.Fatalf("fatal creating temporary dir")
	}
	b := &bucket{dir: temp}
	uploadID, err := b.NewMultipartUpload(context.Background(), objectName, "text/plain", &driver.WriterOptions{Metadata: map[string]string{"anycontent": "3f"}})
	if err != nil {
		t.Fatal("Unexpected error creating mulipart upload", err)
	}
	w, err := b.NewMultipartWriter(context.Background(), objectName, uploadID, 1, &driver.WriterOptions{})
	if err != nil {
		t.Fatal("Unexpected error creating new multipart writer", err)
	}
	w.Write([]byte(blobutil.GetSHA256Hash([]byte("012345"))))
	p1, err := w.Close()
	if err != nil {
		t.Fatal("Unexpected error closing part 1", err)
	}

	w, err = b.NewMultipartWriter(context.Background(), objectName, uploadID, 2, &driver.WriterOptions{})
	if err != nil {
		t.Fatal("Unexpected error creating new multipart writer", err)
	}
	w.Write([]byte(blobutil.GetSHA256Hash([]byte("67890"))))
	p2, err := w.Close()
	if err != nil {
		t.Fatal("Unexpected error closing part 2", err)
	}

	parts := []driver.CompletePart{{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag}}
	if _, err := b.CompleteMultipartUpload(context.Background(), objectName, uploadID, parts); err != nil {
		t.Fatal("failed completed multipart upload", err)
	}
}
