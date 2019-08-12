package drivertest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/thatique/awan/blob"
	"github.com/thatique/awan/blob/driver"
)

type Harness interface {
	// MakeDriver creates a driver.Bucket to test.
	// Multiple calls to MakeDriver during a test run must refer to the
	// same storage bucket; i.e., a blob created using one driver.Bucket must
	// be readable by a subsequent driver.Bucket.
	MakeDriver(ctx context.Context) (driver.Bucket, error)
	// HTTPClient should return an unauthorized *http.Client, or nil.
	// Required if the service supports SignedURL.
	HTTPClient() *http.Client
	// Close closes resources used by the harness.
	Close() error
}

// HarnessMaker describes functions that construct a harness for running tests.
// It is called exactly once per test; Harness.Close() will be called when the test is complete.
type HarnessMaker func(ctx context.Context, t *testing.T) (Harness, error)

func RunConformanceTests(t *testing.T, newHarness HarnessMaker) {
	t.Run("TestList", func(t *testing.T) {
		testList(t, newHarness)
	})
}

// testList tests the functionality of List.
func testList(t *testing.T, newHarness HarnessMaker) {
	const keyPrefix = "blob-for-list"
	content := []byte("hello")

	keyForIndex := func(i int) string { return fmt.Sprintf("%s-%d", keyPrefix, i) }
	gotIndices := func(t *testing.T, objs []*driver.ListObject) []int {
		var got []int
		for _, obj := range objs {
			if !strings.HasPrefix(obj.Key, keyPrefix) {
				t.Errorf("got name %q, expected it to have prefix %q", obj.Key, keyPrefix)
				continue
			}
			i, err := strconv.Atoi(obj.Key[len(keyPrefix)+1:])
			if err != nil {
				t.Error(err)
				continue
			}
			got = append(got, i)
		}
		return got
	}

	tests := []struct {
		name      string
		pageSize  int
		prefix    string
		wantPages [][]int
		want      []int
	}{
		{
			name:      "no objects",
			prefix:    "no-objects-with-this-prefix",
			wantPages: [][]int{nil},
		},
		{
			name:      "exactly 1 object due to prefix",
			prefix:    keyForIndex(1),
			wantPages: [][]int{{1}},
			want:      []int{1},
		},
		{
			name:      "no pagination",
			prefix:    keyPrefix,
			wantPages: [][]int{{0, 1, 2}},
			want:      []int{0, 1, 2},
		},
		{
			name:      "by 1",
			prefix:    keyPrefix,
			pageSize:  1,
			wantPages: [][]int{{0}, {1}, {2}},
			want:      []int{0, 1, 2},
		},
		{
			name:      "by 2",
			prefix:    keyPrefix,
			pageSize:  2,
			wantPages: [][]int{{0, 1}, {2}},
			want:      []int{0, 1, 2},
		},
		{
			name:      "by 3",
			prefix:    keyPrefix,
			pageSize:  3,
			wantPages: [][]int{{0, 1, 2}},
			want:      []int{0, 1, 2},
		},
	}

	ctx := context.Background()

	init := func(t *testing.T) (driver.Bucket, func()) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// See if the blobs are already there.
		b := blob.NewBucket(drv)
		iter := b.List(&blob.ListOptions{Prefix: keyPrefix})
		found := iterToSetOfKeys(ctx, t, iter)
		for i := 0; i < 3; i++ {
			key := keyForIndex(i)
			if !found[key] {
				if err := b.WriteAll(ctx, key, content, nil); err != nil {
					b.Close()
					t.Fatal(err)
				}
			}
		}
		return drv, func() { b.Close(); h.Close() }
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			drv, done := init(t)
			defer done()

			var gotPages [][]int
			var got []int
			var nextPageToken []byte
			for {
				page, err := drv.ListPaged(ctx, &driver.ListOptions{
					PageSize:  tc.pageSize,
					Prefix:    tc.prefix,
					PageToken: nextPageToken,
				})
				if err != nil {
					t.Fatal(err)
				}
				gotThisPage := gotIndices(t, page.Objects)
				got = append(got, gotThisPage...)
				gotPages = append(gotPages, gotThisPage)
				if len(page.NextPageToken) == 0 {
					break
				}
				nextPageToken = page.NextPageToken
			}
			if diff := cmp.Diff(gotPages, tc.wantPages); diff != "" {
				t.Errorf("got\n%v\nwant\n%v\ndiff\n%s", gotPages, tc.wantPages, diff)
			}
			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Errorf("got\n%v\nwant\n%v\ndiff\n%s", got, tc.want, diff)
			}
		})
	}

	// Verify pagination works when inserting in a retrieved page.
	t.Run("PaginationConsistencyAfterInsert", func(t *testing.T) {
		drv, done := init(t)
		defer done()

		// Fetch a page of 2 results: 0, 1.
		page, err := drv.ListPaged(ctx, &driver.ListOptions{
			PageSize: 2,
			Prefix:   keyPrefix,
		})
		if err != nil {
			t.Fatal(err)
		}
		got := gotIndices(t, page.Objects)
		want := []int{0, 1}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Fatalf("got\n%v\nwant\n%v\ndiff\n%s", got, want, diff)
		}

		// Insert a key "0a" in the middle of the page we already retrieved.
		b := blob.NewBucket(drv)
		defer b.Close()
		key := page.Objects[0].Key + "a"
		if err := b.WriteAll(ctx, key, content, nil); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = b.Delete(ctx, key)
		}()

		// Fetch the next page. It should not include 0, 0a, or 1, and it should
		// include 2.
		page, err = drv.ListPaged(ctx, &driver.ListOptions{
			Prefix:    keyPrefix,
			PageToken: page.NextPageToken,
		})
		if err != nil {
			t.Fatal(err)
		}
		got = gotIndices(t, page.Objects)
		want = []int{2}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("got\n%v\nwant\n%v\ndiff\n%s", got, want, diff)
		}
	})

}

func iterToSetOfKeys(ctx context.Context, t *testing.T, iter *blob.ListIterator) map[string]bool {
	retval := map[string]bool{}
	for {
		if item, err := iter.Next(ctx); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		} else {
			retval[item.Key] = true
		}
	}
	return retval
}