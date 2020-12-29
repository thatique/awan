package minioblob_test

import (
	"context"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/thatique/awan/blob/minioblob"
	"gocloud.dev/blob"
)

const (
	minioAccessKey  = "Q3AM3UQ867SPQQA43P2F"
	minioSecretKey  = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	minioBucketName = "testbucket-awan"
)

func ExampleOpenBucket() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	// Establish an AWS session.
	// See https://docs.aws.amazon.com/sdk-for-go/api/aws/session/ for more info.
	// The region must match the region for "my-bucket".
	client, err := minio.New("play.min.io", &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create a *blob.Bucket.
	bucket, err := minioblob.OpenBucket(ctx, client, minioBucketName, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer bucket.Close()
}

func Example_openBucketFromURL() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, add a blank import: _ "gocloud.dev/blob/s3blob"
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	// blob.OpenBucket creates a *blob.Bucket from a URL.
	bucket, err := blob.OpenBucket(ctx, "minio://play.min.io/testbucket-awan?region=us-west-1")
	if err != nil {
		log.Fatal(err)
	}
	defer bucket.Close()
}
