package drivertest

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/thatique/awan/blob"
	"github.com/thatique/awan/blob/driver"
	"github.com/thatique/awan/internal/escape"
	"github.com/thatique/awan/verr"
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
	Close()
}

// HarnessMaker describes functions that construct a harness for running tests.
// It is called exactly once per test; Harness.Close() will be called when the test is complete.
type HarnessMaker func(ctx context.Context, t *testing.T) (Harness, error)

func RunConformanceTests(t *testing.T, newHarness HarnessMaker) {
	t.Run("TestList", func(t *testing.T) {
		testList(t, newHarness)
	})
	t.Run("TestListWeirdKeys", func(t *testing.T) {
		testListWeirdKeys(t, newHarness)
	})
	t.Run("TestListDelimiters", func(t *testing.T) {
		testListDelimiters(t, newHarness)
	})
	t.Run("TestRead", func(t *testing.T) {
		testRead(t, newHarness)
	})
	t.Run("TestAttributes", func(t *testing.T) {
		testAttributes(t, newHarness)
	})
	t.Run("TestWrite", func(t *testing.T) {
		testWrite(t, newHarness)
	})
	t.Run("TestCanceledWrite", func(t *testing.T) {
		testCanceledWrite(t, newHarness)
	})
	t.Run("TestConcurrentWriteAndRead", func(t *testing.T) {
		testConcurrentWriteAndRead(t, newHarness)
	})
	t.Run("TestMetadata", func(t *testing.T) {
		testMetadata(t, newHarness)
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

// testListWeirdKeys tests the functionality of List on weird keys.
func testListWeirdKeys(t *testing.T, newHarness HarnessMaker) {
	const keyPrefix = "list-weirdkeys-"
	content := []byte("hello")
	ctx := context.Background()

	// We're going to create a blob for each of the weird key strings, and
	// then verify we can see them with List.
	want := map[string]bool{}
	for _, k := range escape.WeirdStrings {
		want[keyPrefix+k] = true
	}

	// Creates blobs for sub-tests below.
	// We only create the blobs once, for efficiency and because there's
	// no guarantee that after we create them they will be immediately returned
	// from List. The very first time the test is run against a Bucket, it may be
	// flaky due to this race.
	init := func(t *testing.T) (*blob.Bucket, func()) {
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
		for _, k := range escape.WeirdStrings {
			key := keyPrefix + k
			if !found[key] {
				if err := b.WriteAll(ctx, key, content, nil); err != nil {
					b.Close()
					t.Fatal(err)
				}
			}
		}
		return b, func() { b.Close(); h.Close() }
	}

	b, done := init(t)
	defer done()

	iter := b.List(&blob.ListOptions{Prefix: keyPrefix})
	got := iterToSetOfKeys(ctx, t, iter)

	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("got\n%v\nwant\n%v\ndiff\n%s", got, want, diff)
	}
}

// listResult is a recursive view of the hierarchy. It's used to verify List
// using Delimiter.
type listResult struct {
	Key   string
	IsDir bool
	// If IsDir is true and recursion is enabled, the recursive listing of the directory.
	Sub []listResult
}

// doList lists b using prefix and delim.
// If recurse is true, it recurses into directories filling in listResult.Sub.
func doList(ctx context.Context, b *blob.Bucket, prefix, delim string, recurse bool) ([]listResult, error) {
	iter := b.List(&blob.ListOptions{
		Prefix:    prefix,
		Delimiter: delim,
	})
	var retval []listResult
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			if obj != nil {
				return nil, errors.New("obj is not nil on EOF")
			}
			break
		}
		if err != nil {
			return nil, err
		}
		var sub []listResult
		if obj.IsDir && recurse {
			sub, err = doList(ctx, b, obj.Key, delim, true)
			if err != nil {
				return nil, err
			}
		}
		retval = append(retval, listResult{
			Key:   obj.Key,
			IsDir: obj.IsDir,
			Sub:   sub,
		})
	}
	return retval, nil
}

// testListDelimiters tests the functionality of List using Delimiters.
func testListDelimiters(t *testing.T, newHarness HarnessMaker) {
	const keyPrefix = "blob-for-delimiters-"
	content := []byte("hello")

	// The set of files to use for these tests. The strings in each entry will
	// be joined using delim, so the result is a directory structure like this
	// (using / as delimiter):
	// dir1/a.txt
	// dir1/b.txt
	// dir1/subdir/c.txt
	// dir1/subdir/d.txt
	// dir2/e.txt
	// f.txt
	keys := [][]string{
		{"dir1", "a.txt"},
		{"dir1", "b.txt"},
		{"dir1", "subdir", "c.txt"},
		{"dir1", "subdir", "d.txt"},
		{"dir2", "e.txt"},
		{"f.txt"},
	}

	// Test with several different delimiters.
	tests := []struct {
		name, delim string
		// Expected result of doList with an empty delimiter.
		// All keys should be listed at the top level, with no directories.
		wantFlat []listResult
		// Expected result of doList with delimiter and recurse = true.
		// All keys should be listed, with keys in directories in the Sub field
		// of their directory.
		wantRecursive []listResult
		// Expected result of repeatedly calling driver.ListPaged with delimiter
		// and page size = 1.
		wantPaged []listResult
		// expected result of doList with delimiter and recurse = false
		// after dir2/e.txt is deleted
		// dir1/ and f.txt should be listed; dir2/ should no longer be present
		// because there are no keys in it.
		wantAfterDel []listResult
	}{
		{
			name:  "fwdslash",
			delim: "/",
			wantFlat: []listResult{
				{Key: keyPrefix + "/dir1/a.txt"},
				{Key: keyPrefix + "/dir1/b.txt"},
				{Key: keyPrefix + "/dir1/subdir/c.txt"},
				{Key: keyPrefix + "/dir1/subdir/d.txt"},
				{Key: keyPrefix + "/dir2/e.txt"},
				{Key: keyPrefix + "/f.txt"},
			},
			wantRecursive: []listResult{
				{
					Key:   keyPrefix + "/dir1/",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "/dir1/a.txt"},
						{Key: keyPrefix + "/dir1/b.txt"},
						{
							Key:   keyPrefix + "/dir1/subdir/",
							IsDir: true,
							Sub: []listResult{
								{Key: keyPrefix + "/dir1/subdir/c.txt"},
								{Key: keyPrefix + "/dir1/subdir/d.txt"},
							},
						},
					},
				},
				{
					Key:   keyPrefix + "/dir2/",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "/dir2/e.txt"},
					},
				},
				{Key: keyPrefix + "/f.txt"},
			},
			wantPaged: []listResult{
				{
					Key:   keyPrefix + "/dir1/",
					IsDir: true,
				},
				{
					Key:   keyPrefix + "/dir2/",
					IsDir: true,
				},
				{Key: keyPrefix + "/f.txt"},
			},
			wantAfterDel: []listResult{
				{
					Key:   keyPrefix + "/dir1/",
					IsDir: true,
				},
				{Key: keyPrefix + "/f.txt"},
			},
		},
		{
			name:  "backslash",
			delim: "\\",
			wantFlat: []listResult{
				{Key: keyPrefix + "\\dir1\\a.txt"},
				{Key: keyPrefix + "\\dir1\\b.txt"},
				{Key: keyPrefix + "\\dir1\\subdir\\c.txt"},
				{Key: keyPrefix + "\\dir1\\subdir\\d.txt"},
				{Key: keyPrefix + "\\dir2\\e.txt"},
				{Key: keyPrefix + "\\f.txt"},
			},
			wantRecursive: []listResult{
				{
					Key:   keyPrefix + "\\dir1\\",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "\\dir1\\a.txt"},
						{Key: keyPrefix + "\\dir1\\b.txt"},
						{
							Key:   keyPrefix + "\\dir1\\subdir\\",
							IsDir: true,
							Sub: []listResult{
								{Key: keyPrefix + "\\dir1\\subdir\\c.txt"},
								{Key: keyPrefix + "\\dir1\\subdir\\d.txt"},
							},
						},
					},
				},
				{
					Key:   keyPrefix + "\\dir2\\",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "\\dir2\\e.txt"},
					},
				},
				{Key: keyPrefix + "\\f.txt"},
			},
			wantPaged: []listResult{
				{
					Key:   keyPrefix + "\\dir1\\",
					IsDir: true,
				},
				{
					Key:   keyPrefix + "\\dir2\\",
					IsDir: true,
				},
				{Key: keyPrefix + "\\f.txt"},
			},
			wantAfterDel: []listResult{
				{
					Key:   keyPrefix + "\\dir1\\",
					IsDir: true,
				},
				{Key: keyPrefix + "\\f.txt"},
			},
		},
		{
			name:  "abc",
			delim: "abc",
			wantFlat: []listResult{
				{Key: keyPrefix + "abcdir1abca.txt"},
				{Key: keyPrefix + "abcdir1abcb.txt"},
				{Key: keyPrefix + "abcdir1abcsubdirabcc.txt"},
				{Key: keyPrefix + "abcdir1abcsubdirabcd.txt"},
				{Key: keyPrefix + "abcdir2abce.txt"},
				{Key: keyPrefix + "abcf.txt"},
			},
			wantRecursive: []listResult{
				{
					Key:   keyPrefix + "abcdir1abc",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "abcdir1abca.txt"},
						{Key: keyPrefix + "abcdir1abcb.txt"},
						{
							Key:   keyPrefix + "abcdir1abcsubdirabc",
							IsDir: true,
							Sub: []listResult{
								{Key: keyPrefix + "abcdir1abcsubdirabcc.txt"},
								{Key: keyPrefix + "abcdir1abcsubdirabcd.txt"},
							},
						},
					},
				},
				{
					Key:   keyPrefix + "abcdir2abc",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "abcdir2abce.txt"},
					},
				},
				{Key: keyPrefix + "abcf.txt"},
			},
			wantPaged: []listResult{
				{
					Key:   keyPrefix + "abcdir1abc",
					IsDir: true,
				},
				{
					Key:   keyPrefix + "abcdir2abc",
					IsDir: true,
				},
				{Key: keyPrefix + "abcf.txt"},
			},
			wantAfterDel: []listResult{
				{
					Key:   keyPrefix + "abcdir1abc",
					IsDir: true,
				},
				{Key: keyPrefix + "abcf.txt"},
			},
		},
	}

	ctx := context.Background()

	// Creates blobs for sub-tests below.
	// We only create the blobs once, for efficiency and because there's
	// no guarantee that after we create them they will be immediately returned
	// from List. The very first time the test is run against a Bucket, it may be
	// flaky due to this race.
	init := func(t *testing.T, delim string) (driver.Bucket, *blob.Bucket, func()) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := blob.NewBucket(drv)

		// See if the blobs are already there.
		prefix := keyPrefix + delim
		iter := b.List(&blob.ListOptions{Prefix: prefix})
		found := iterToSetOfKeys(ctx, t, iter)
		for _, keyParts := range keys {
			key := prefix + strings.Join(keyParts, delim)
			if !found[key] {
				if err := b.WriteAll(ctx, key, content, nil); err != nil {
					b.Close()
					t.Fatal(err)
				}
			}
		}
		return drv, b, func() { b.Close(); h.Close() }
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			drv, b, done := init(t, tc.delim)
			defer done()

			// Fetch without using delimiter.
			got, err := doList(ctx, b, keyPrefix+tc.delim, "", true)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, tc.wantFlat); diff != "" {
				t.Errorf("with no delimiter, got\n%v\nwant\n%v\ndiff\n%s", got, tc.wantFlat, diff)
			}

			// Fetch using delimiter, recursively.
			got, err = doList(ctx, b, keyPrefix+tc.delim, tc.delim, true)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, tc.wantRecursive); diff != "" {
				t.Errorf("with delimiter, got\n%v\nwant\n%v\ndiff\n%s", got, tc.wantRecursive, diff)
			}

			// Test pagination via driver.ListPaged.
			var nextPageToken []byte
			got = nil
			for {
				page, err := drv.ListPaged(ctx, &driver.ListOptions{
					Prefix:    keyPrefix + tc.delim,
					Delimiter: tc.delim,
					PageSize:  1,
					PageToken: nextPageToken,
				})
				if err != nil {
					t.Fatal(err)
				}
				if len(page.Objects) > 1 {
					t.Errorf("got %d objects on a page, want 0 or 1", len(page.Objects))
				}
				for _, obj := range page.Objects {
					got = append(got, listResult{
						Key:   obj.Key,
						IsDir: obj.IsDir,
					})
				}
				if len(page.NextPageToken) == 0 {
					break
				}
				nextPageToken = page.NextPageToken
			}
			if diff := cmp.Diff(got, tc.wantPaged); diff != "" {
				t.Errorf("paged got\n%v\nwant\n%v\ndiff\n%s", got, tc.wantPaged, diff)
			}

			// Delete dir2/e.txt and verify that dir2/ is no longer returned.
			key := strings.Join(append([]string{keyPrefix}, "dir2", "e.txt"), tc.delim)
			if err := b.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}
			// Attempt to restore dir2/e.txt at the end of the test for the next run.
			defer func() {
				_ = b.WriteAll(ctx, key, content, nil)
			}()

			got, err = doList(ctx, b, keyPrefix+tc.delim, tc.delim, false)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, tc.wantAfterDel); diff != "" {
				t.Errorf("after delete, got\n%v\nwant\n%v\ndiff\n%s", got, tc.wantAfterDel, diff)
			}
		})
	}
}

// testRead tests the functionality of NewReader, NewRangeReader, and Reader.
func testRead(t *testing.T, newHarness HarnessMaker) {
	const key = "blob-for-reading"
	content := []byte("abcdefghijklmnopqurstuvwxyz")
	contentSize := int64(len(content))

	tests := []struct {
		name           string
		key            string
		offset, length int64
		want           []byte
		wantReadSize   int64
		wantErr        bool
		// set to true to skip creation of the object for
		// tests where we expect an error without any actual
		// read.
		skipCreate bool
	}{
		{
			name:    "read of nonexistent key fails",
			key:     "key-does-not-exist",
			length:  -1,
			wantErr: true,
		},
		{
			name:       "negative offset fails",
			key:        key,
			offset:     -1,
			wantErr:    true,
			skipCreate: true,
		},
		{
			name: "length 0 read",
			key:  key,
			want: []byte{},
		},
		{
			name:         "read from positive offset to end",
			key:          key,
			offset:       10,
			length:       -1,
			want:         content[10:],
			wantReadSize: contentSize - 10,
		},
		{
			name:         "read a part in middle",
			key:          key,
			offset:       10,
			length:       5,
			want:         content[10:15],
			wantReadSize: 5,
		},
		{
			name:         "read in full",
			key:          key,
			length:       -1,
			want:         content,
			wantReadSize: contentSize,
		},
		{
			name:         "read in full with negative length not -1",
			key:          key,
			length:       -42,
			want:         content,
			wantReadSize: contentSize,
		},
	}

	ctx := context.Background()

	// Creates a blob for sub-tests below.
	init := func(t *testing.T, skipCreate bool) (*blob.Bucket, func()) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}

		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := blob.NewBucket(drv)
		if skipCreate {
			return b, func() { b.Close(); h.Close() }
		}
		if err := b.WriteAll(ctx, key, content, nil); err != nil {
			b.Close()
			t.Fatal(err)
		}
		return b, func() {
			_ = b.Delete(ctx, key)
			b.Close()
			h.Close()
		}
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, done := init(t, tc.skipCreate)
			defer done()

			r, err := b.NewRangeReader(ctx, tc.key, tc.offset, tc.length, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("got err %v want error %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			defer r.Close()
			// Make the buffer bigger than needed to make sure we actually only read
			// the expected number of bytes.
			got := make([]byte, tc.wantReadSize+10)
			n, err := r.Read(got)
			// EOF error is optional, see https://golang.org/pkg/io/#Reader.
			if err != nil && err != io.EOF {
				t.Errorf("unexpected error during read: %v", err)
			}
			if int64(n) != tc.wantReadSize {
				t.Errorf("got read length %d want %d", n, tc.wantReadSize)
			}
			if !cmp.Equal(got[:tc.wantReadSize], tc.want) {
				t.Errorf("got %q want %q", string(got), string(tc.want))
			}
			if r.Size() != contentSize {
				t.Errorf("got size %d want %d", r.Size(), contentSize)
			}
			if r.ModTime().IsZero() {
				t.Errorf("got zero mod time, want non-zero")
			}
		})
	}
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

// testAttributes tests Attributes.
func testAttributes(t *testing.T, newHarness HarnessMaker) {
	const (
		key                = "blob-for-attributes"
		contentType        = "text/plain"
		cacheControl       = "no-cache"
		contentDisposition = "inline"
		contentEncoding    = "identity"
		contentLanguage    = "en"
	)
	content := []byte("Hello World!")

	ctx := context.Background()

	// Creates a blob for sub-tests below.
	init := func(t *testing.T) (*blob.Bucket, func()) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := blob.NewBucket(drv)
		opts := &blob.WriterOptions{
			ContentType:        contentType,
			CacheControl:       cacheControl,
			ContentDisposition: contentDisposition,
			ContentEncoding:    contentEncoding,
			ContentLanguage:    contentLanguage,
		}
		if err := b.WriteAll(ctx, key, content, opts); err != nil {
			b.Close()
			t.Fatal(err)
		}
		return b, func() {
			_ = b.Delete(ctx, key)
			b.Close()
			h.Close()
		}
	}

	b, done := init(t)
	defer done()

	_, err := b.Attributes(ctx, "not-found")
	if err == nil {
		t.Errorf("got nil want error")
	} else if verr.Code(err) != verr.NotFound {
		t.Errorf("got %v want NotFound error", err)
	} else if !strings.Contains(err.Error(), "not-found") {
		t.Errorf("got %v want error to include missing key", err)
	}
	a, err := b.Attributes(ctx, key)
	if err != nil {
		t.Fatalf("failed Attributes: %v", err)
	}
	// Also make a Reader so we can verify the subset of attributes
	// that it exposes.
	r, err := b.NewReader(ctx, key, nil)
	if err != nil {
		t.Fatalf("failed Attributes: %v", err)
	}
	if a.CacheControl != cacheControl {
		t.Errorf("got CacheControl %q want %q", a.CacheControl, cacheControl)
	}
	if a.ContentDisposition != contentDisposition {
		t.Errorf("got ContentDisposition %q want %q", a.ContentDisposition, contentDisposition)
	}
	if a.ContentEncoding != contentEncoding {
		t.Errorf("got ContentEncoding %q want %q", a.ContentEncoding, contentEncoding)
	}
	if a.ContentLanguage != contentLanguage {
		t.Errorf("got ContentLanguage %q want %q", a.ContentLanguage, contentLanguage)
	}
	if a.ContentType != contentType {
		t.Errorf("got ContentType %q want %q", a.ContentType, contentType)
	}
	if r.ContentType() != contentType {
		t.Errorf("got Reader.ContentType() %q want %q", r.ContentType(), contentType)
	}
	if a.Size != int64(len(content)) {
		t.Errorf("got Size %d want %d", a.Size, len(content))
	}
	if r.Size() != int64(len(content)) {
		t.Errorf("got Reader.Size() %d want %d", r.Size(), len(content))
	}
	r.Close()

	t1 := a.ModTime
	if err := b.WriteAll(ctx, key, content, nil); err != nil {
		t.Fatal(err)
	}
	a2, err := b.Attributes(ctx, key)
	if err != nil {
		t.Errorf("failed Attributes#2: %v", err)
	}
	t2 := a2.ModTime
	if t2.Before(t1) {
		t.Errorf("ModTime %v is before %v", t2, t1)
	}
}

// loadTestData loads test data, inlined using go-bindata.
func loadTestData(t testing.TB, name string) []byte {
	data, err := Asset(name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// testWrite tests the functionality of NewWriter and Writer.
func testWrite(t *testing.T, newHarness HarnessMaker) {
	const key = "blob-for-reading"
	const existingContent = "existing content"
	smallText := loadTestData(t, "test-small.txt")
	mediumHTML := loadTestData(t, "test-medium.html")
	largeJpg := loadTestData(t, "test-large.jpg")
	helloWorld := []byte("hello world")
	helloWorldMD5 := md5.Sum(helloWorld)

	tests := []struct {
		name            string
		key             string
		exists          bool
		content         []byte
		contentType     string
		contentMD5      []byte
		firstChunk      int
		wantContentType string
		wantErr         bool
		wantReadErr     bool // if wantErr is true, and Read after err should fail with something other than NotExists
	}{
		{
			name:        "write to empty key fails",
			wantErr:     true,
			wantReadErr: true, // read from empty key fails, but not always with NotExists
		},
		{
			name: "no write then close results in empty blob",
			key:  key,
		},
		{
			name: "no write then close results in empty blob, blob existed",
			key:  key,
		},
		{
			name:        "invalid ContentType fails",
			key:         key,
			contentType: "application/octet/stream",
			wantErr:     true,
		},
		{
			name:            "ContentType is discovered if not provided",
			key:             key,
			content:         mediumHTML,
			wantContentType: "text/html",
		},
		{
			name:            "write with explicit ContentType overrides discovery",
			key:             key,
			content:         mediumHTML,
			contentType:     "application/json",
			wantContentType: "application/json",
		},
		{
			name:       "Content md5 match",
			key:        key,
			content:    helloWorld,
			contentMD5: helloWorldMD5[:],
		},
		{
			name:       "Content md5 did not match",
			key:        key,
			content:    []byte("not hello world"),
			contentMD5: helloWorldMD5[:],
			wantErr:    true,
		},
		{
			name:       "Content md5 did not match, blob existed",
			exists:     true,
			key:        key,
			content:    []byte("not hello world"),
			contentMD5: helloWorldMD5[:],
			wantErr:    true,
		},
		{
			name:            "a small text file",
			key:             key,
			content:         smallText,
			wantContentType: "text/html",
		},
		{
			name:            "a large jpg file",
			key:             key,
			content:         largeJpg,
			wantContentType: "image/jpg",
		},
		{
			name:            "a large jpg file written in two chunks",
			key:             key,
			firstChunk:      10,
			content:         largeJpg,
			wantContentType: "image/jpg",
		},
		// TODO(issue #304): Fails for GCS.
		/*
			{
				name:            "ContentType is parsed and reformatted",
				key:             key,
				content:         []byte("foo"),
				contentType:     `FORM-DATA;name="foo"`,
				wantContentType: `form-data; name=foo`,
			},
		*/
	}

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, err := newHarness(ctx, t)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			drv, err := h.MakeDriver(ctx)
			if err != nil {
				t.Fatal(err)
			}
			b := blob.NewBucket(drv)
			defer b.Close()

			// If the test wants the blob to already exist, write it.
			if tc.exists {
				if err := b.WriteAll(ctx, key, []byte(existingContent), nil); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = b.Delete(ctx, key)
				}()
			}

			// Write the content.
			opts := &blob.WriterOptions{
				ContentType: tc.contentType,
				ContentMD5:  tc.contentMD5[:],
			}
			w, err := b.NewWriter(ctx, tc.key, opts)
			if err == nil {
				if len(tc.content) > 0 {
					if tc.firstChunk == 0 {
						// Write the whole thing.
						_, err = w.Write(tc.content)
					} else {
						// Write it in 2 chunks.
						_, err = w.Write(tc.content[:tc.firstChunk])
						if err == nil {
							_, err = w.Write(tc.content[tc.firstChunk:])
						}
					}
				}
				if err == nil {
					err = w.Close()
				}
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("NewWriter or Close got err %v want error %v", err, tc.wantErr)
			}
			if err != nil {
				// The write failed; verify that it had no effect.
				buf, err := b.ReadAll(ctx, tc.key)
				if tc.exists {
					// Verify the previous content is still there.
					if !bytes.Equal(buf, []byte(existingContent)) {
						t.Errorf("Write failed as expected, but content doesn't match expected previous content; got \n%s\n want \n%s", string(buf), existingContent)
					}
				} else {
					// Verify that the read fails with NotFound.
					if err == nil {
						t.Error("Write failed as expected, but Read after that didn't return an error")
					} else if !tc.wantReadErr && verr.Code(err) != verr.NotFound {
						t.Errorf("Write failed as expected, but Read after that didn't return the right error; got %v want NotFound", err)
					} else if !strings.Contains(err.Error(), tc.key) {
						t.Errorf("got %v want error to include missing key", err)
					}
				}
				return
			}
			defer func() { _ = b.Delete(ctx, tc.key) }()

			// Read it back.
			buf, err := b.ReadAll(ctx, tc.key)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(buf, tc.content) {
				if len(buf) < 100 && len(tc.content) < 100 {
					t.Errorf("read didn't match write; got \n%s\n want \n%s", string(buf), string(tc.content))
				} else {
					t.Error("read didn't match write, content too large to display")
				}
			}
		})
	}
}

// testCanceledWrite tests the functionality of canceling an in-progress write.
func testCanceledWrite(t *testing.T, newHarness HarnessMaker) {
	const key = "blob-for-canceled-write"
	content := []byte("hello world")
	cancelContent := []byte("going to cancel")

	tests := []struct {
		description string
		contentType string
		exists      bool
	}{
		{
			// The write will be buffered in the portable type as part of
			// ContentType detection, so the first call to the Driver will be Close.
			description: "EmptyContentType",
		},
		{
			// The write will be sent to the Driver, which may do its own
			// internal buffering.
			description: "NonEmptyContentType",
			contentType: "text/plain",
		},
		{
			description: "BlobExists",
			exists:      true,
		},
		// TODO(issue #482): Find a way to test that a chunked upload that's interrupted
		// after some chunks are uploaded cancels correctly.
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			cancelCtx, cancel := context.WithCancel(ctx)
			h, err := newHarness(ctx, t)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			drv, err := h.MakeDriver(ctx)
			if err != nil {
				t.Fatal(err)
			}
			b := blob.NewBucket(drv)
			defer b.Close()

			opts := &blob.WriterOptions{
				ContentType: test.contentType,
			}
			// If the test wants the blob to already exist, write it.
			if test.exists {
				if err := b.WriteAll(ctx, key, content, opts); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = b.Delete(ctx, key)
				}()
			}

			// Create a writer with the context that we're going
			// to cancel.
			w, err := b.NewWriter(cancelCtx, key, opts)
			if err != nil {
				t.Fatal(err)
			}
			// Write the content.
			if _, err := w.Write(cancelContent); err != nil {
				t.Fatal(err)
			}

			// Verify that the previous content (if any) is still readable,
			// because the write hasn't been Closed yet.
			got, err := b.ReadAll(ctx, key)
			if test.exists {
				// The previous content should still be there.
				if !cmp.Equal(got, content) {
					t.Errorf("during unclosed write, got %q want %q", string(got), string(content))
				}
			} else {
				// The read should fail; the write hasn't been Closed so the
				// blob shouldn't exist.
				if err == nil {
					t.Error("wanted read to return an error when write is not yet Closed")
				}
			}

			// Cancel the context to abort the write.
			cancel()
			// Close should return some kind of canceled context error.
			// We can't verify the kind of error cleanly, so we just verify there's
			// an error.
			if err := w.Close(); err == nil {
				t.Errorf("got Close error %v want canceled ctx error", err)
			}

			// Verify the write was truly aborted.
			got, err = b.ReadAll(ctx, key)
			if test.exists {
				// The previous content should still be there.
				if !cmp.Equal(got, content) {
					t.Errorf("after canceled write, got %q want %q", string(got), string(content))
				}
			} else {
				// The read should fail; the write was aborted so the
				// blob shouldn't exist.
				if err == nil {
					t.Error("wanted read to return an error when write was canceled")
				}
			}
		})
	}
}

// testConcurrentWriteAndRead tests that concurrent writing to multiple blob
// keys and concurrent reading from multiple blob keys works.
func testConcurrentWriteAndRead(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h, err := newHarness(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	drv, err := h.MakeDriver(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b := blob.NewBucket(drv)
	defer b.Close()

	// Prepare data. Each of the numKeys blobs has dataSize bytes, with each byte
	// set to the numeric key index. For example, the blob at "key0" consists of
	// all dataSize bytes set to 0.
	const numKeys = 20
	const dataSize = 4 * 1024
	keyData := make(map[int][]byte)
	for k := 0; k < numKeys; k++ {
		data := make([]byte, dataSize)
		for i := 0; i < dataSize; i++ {
			data[i] = byte(k)
		}
		keyData[k] = data
	}

	blobName := func(k int) string {
		return fmt.Sprintf("key%d", k)
	}

	var wg sync.WaitGroup

	// Write all blobs concurrently.
	for k := 0; k < numKeys; k++ {
		wg.Add(1)
		go func(key int) {
			if err := b.WriteAll(ctx, blobName(key), keyData[key], nil); err != nil {
				t.Fatal(err)
			}
			wg.Done()
		}(k)
		defer b.Delete(ctx, blobName(k))
	}
	wg.Wait()

	// Read all blobs concurrently and verify that they contain the expected data.
	for k := 0; k < numKeys; k++ {
		wg.Add(1)
		go func(key int) {
			buf, err := b.ReadAll(ctx, blobName(key))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(buf, keyData[key]) {
				t.Errorf("read data mismatch for key %d", key)
			}
			wg.Done()
		}(k)
	}
	wg.Wait()
}

// testMetadata tests writing and reading the key/value metadata for a blob.
func testMetadata(t *testing.T, newHarness HarnessMaker) {
	const key = "blob-for-metadata"
	hello := []byte("hello")

	weirdMetadata := map[string]string{}
	for _, k := range escape.WeirdStrings {
		weirdMetadata[k] = k
	}

	tests := []struct {
		name        string
		metadata    map[string]string
		content     []byte
		contentType string
		want        map[string]string
		wantErr     bool
	}{
		{
			name:     "empty",
			content:  hello,
			metadata: map[string]string{},
			want:     nil,
		},
		{
			name:     "empty key fails",
			content:  hello,
			metadata: map[string]string{"": "empty key value"},
			wantErr:  true,
		},
		{
			name:     "duplicate case-insensitive key fails",
			content:  hello,
			metadata: map[string]string{"abc": "foo", "aBc": "bar"},
			wantErr:  true,
		},
		{
			name:    "valid metadata",
			content: hello,
			metadata: map[string]string{
				"key_a": "value-a",
				"kEy_B": "value-b",
				"key_c": "vAlUe-c",
			},
			want: map[string]string{
				"key_a": "value-a",
				"key_b": "value-b",
				"key_c": "vAlUe-c",
			},
		},
		{
			name:     "valid metadata with empty body",
			content:  nil,
			metadata: map[string]string{"foo": "bar"},
			want:     map[string]string{"foo": "bar"},
		},
		{
			name:        "valid metadata with content type",
			content:     hello,
			contentType: "text/plain",
			metadata:    map[string]string{"foo": "bar"},
			want:        map[string]string{"foo": "bar"},
		},
		{
			name:     "weird metadata keys",
			content:  hello,
			metadata: weirdMetadata,
			want:     weirdMetadata,
		},
		{
			name:     "non-utf8 metadata key",
			content:  hello,
			metadata: map[string]string{escape.NonUTF8String: "bar"},
			wantErr:  true,
		},
		{
			name:     "non-utf8 metadata value",
			content:  hello,
			metadata: map[string]string{"foo": escape.NonUTF8String},
			wantErr:  true,
		},
	}

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, err := newHarness(ctx, t)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()

			drv, err := h.MakeDriver(ctx)
			if err != nil {
				t.Fatal(err)
			}
			b := blob.NewBucket(drv)
			defer b.Close()
			opts := &blob.WriterOptions{
				Metadata:    tc.metadata,
				ContentType: tc.contentType,
			}
			err = b.WriteAll(ctx, key, hello, opts)
			if (err != nil) != tc.wantErr {
				t.Errorf("got error %v want error %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			defer func() {
				_ = b.Delete(ctx, key)
			}()
			a, err := b.Attributes(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(a.Metadata, tc.want); diff != "" {
				t.Errorf("got\n%v\nwant\n%v\ndiff\n%s", a.Metadata, tc.want, diff)
			}
		})
	}
}