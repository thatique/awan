package fileblob

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/thatique/awan/blob/driver"
	blobutil "github.com/thatique/awan/internal/blob"
	"github.com/thatique/awan/posix"
	"go.uber.org/atomic"
)

// FS format, and object metadata.
const (
	// fs.json object metadata.
	fsMultipartJSONFile = "multipart.json"

	// gcs.json version number
	fsMultipartMetaCurrentVersion = "1"

	ReservedMetadataPrefix = "X-Fileblob-Internal-"
)

var (
	// Invalid format.
	errInvalidFormat = fmt.Errorf("Unknown format")

	// Minimum Part size for multipart upload is 5MiB
	globalMinPartSize = 5 * humanize.MiByte
)

type fileblobMultipartMetaV1 struct {
	Version string `json:"version"` // Version number
	Key     string `json:"key"`     // Object key
}

func getUploadIDDir(key, uploadID string) string {
	return filepath.Join(multipartDirTmp, blobutil.GetSHA256Hash([]byte(key)), uploadID)
}

func getMultipartSHADir(key string) string {
	return filepath.Join(multipartDirTmp, blobutil.GetSHA256Hash([]byte(key)))
}

// Create an s3 compatible MD5sum for complete multipart transaction.
func getCompleteMultipartMD5(ctx context.Context, parts []driver.CompletePart) (string, error) {
	var finalMD5Bytes []byte
	for _, part := range parts {
		md5Bytes, err := hex.DecodeString(blobutil.CanonicalizeETag(part.ETag))
		if err != nil {
			return "", err
		}
		finalMD5Bytes = append(finalMD5Bytes, md5Bytes...)
	}
	s3MD5 := fmt.Sprintf("%s-%d", blobutil.GetMD5Hash(finalMD5Bytes), len(parts))
	return s3MD5, nil
}

// Returns partNumber.etag
func encodePartFile(partNumber int, etag string, actualSize uint64) string {
	return fmt.Sprintf("%.5d.%s.%d", partNumber, etag, actualSize)
}

// Returns partNumber and etag
func decodePartFile(name string) (partNumber int, etag string, actualSize int64, err error) {
	result := strings.Split(name, ".")
	if len(result) != 3 {
		return 0, "", 0, errInvalidFormat
	}
	partNumber, err = strconv.Atoi(result[0])
	if err != nil {
		return 0, "", 0, errInvalidFormat
	}
	actualSize, err = strconv.ParseInt(result[2], 10, 64)
	if err != nil {
		return 0, "", 0, errInvalidFormat
	}
	return partNumber, result[1], actualSize, nil
}

// Returns the part file name which matches the partNumber and etag.
func getPartFile(entries []string, partNumber int, etag string) string {
	for _, entry := range entries {
		if strings.HasPrefix(entry, fmt.Sprintf("%.5d.%s.", partNumber, etag)) {
			return entry
		}
	}
	return ""
}

// Check if part size is more than or equal to minimum allowed size.
func isMinAllowedPartSize(size int64) bool {
	return size >= int64(globalMinPartSize)
}

func (b *bucket) NewMultipartUpload(ctx context.Context, key, contentType string, opts *driver.WriterOptions) (string, error) {
	uploadID := blobutil.MustGetUUID()
	w, err := b.NewTypedWriter(ctx,
		filepath.Join(getUploadIDDir(key, uploadID), fsMultipartJSONFile),
		contentType,
		opts,
	)
	defer w.Close()

	if err != nil {
		return "", err
	}
	if err = json.NewEncoder(w).Encode(&fileblobMultipartMetaV1{
		Version: fsMultipartMetaCurrentVersion,
		Key:     key,
	}); err != nil {
		return "", err
	}

	return uploadID, nil
}

func (b *bucket) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	fullpath := filepath.Join(b.dir, getUploadIDDir(key, uploadID), fsMultipartJSONFile)
	if _, err := os.Stat(fullpath); err != nil {
		return err
	}
	// Ignore the error returned as Windows fails to remove directory if a file in it
	// is Open()ed by the backgroundAppend()
	os.RemoveAll(filepath.Join(b.dir, getUploadIDDir(key, uploadID)))
	// It is safe to ignore any directory not empty error (in case there were multiple uploadIDs on the same object)
	os.Remove(filepath.Join(b.dir, getMultipartSHADir(key)))

	return nil
}

func (b *bucket) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []driver.CompletePart) (*driver.ObjectInfo, error) {
	uploadDir := getUploadIDDir(key, uploadID)
	multipartFile, _, xa, err := b.forKey(filepath.Join(uploadDir, fsMultipartJSONFile))
	if err != nil {
		return nil, err
	}
	f, err := os.Open(multipartFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	multipartMeta := fileblobMultipartMetaV1{}
	if err = json.NewDecoder(f).Decode(&multipartMeta); err != nil {
		return nil, err
	}

	// the stored version should match the current version
	if multipartMeta.Version != fsMultipartMetaCurrentVersion {
		return nil, errInvalidFormat
	}
	// check the key match
	if multipartMeta.Key != key {
		return nil, errInvalidFormat
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := getCompleteMultipartMD5(ctx, parts)
	if err != nil {
		return nil, err
	}

	partSize := int64(-1) // Used later to ensure that all parts sizes are same.
	var wattrs = &xattrs{}
	*wattrs = *xa

	// Allocate parts similar to incoming slice.
	wattrs.Parts = make([]driver.ObjectPartInfo, len(parts))

	_entries, err := posix.ReadDir(filepath.Join(b.dir, uploadDir))
	entries := _entries[:0]
	for _, x := range _entries {
		if !strings.HasSuffix(x, attrsExt) {
			entries = append(entries, x)
		}
	}
	if err != nil {
		return nil, err
	}

	// ensure that part ETag is canonicalized to strip off extraneous quotes
	for i := range parts {
		parts[i].ETag = blobutil.CanonicalizeETag(parts[i].ETag)
	}

	// Save consolidated actual size.
	var (
		objectActualSize int64
		actualSize       int64
	)
	for i, part := range parts {
		partFile := getPartFile(entries, part.PartNumber, part.ETag)
		if partFile == "" {
			return nil, InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
		}
		// Read the actualSize from the pathFileName.
		subParts := strings.Split(partFile, ".")
		actualSize, err = strconv.ParseInt(subParts[len(subParts)-1], 10, 64)
		if err != nil {
			return nil, InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
		}
		partPath := filepath.Join(b.dir, uploadDir, partFile)
		var fi os.FileInfo
		fi, err = os.Stat(partPath)
		if err != nil {
			return nil, err
		}
		if partSize == -1 {
			partSize = actualSize
		}

		wattrs.Parts[i] = driver.ObjectPartInfo{
			Number:     part.PartNumber,
			ETag:       part.ETag,
			Size:       fi.Size(),
			ActualSize: actualSize,
		}

		// Consolidate the actual size.
		objectActualSize += actualSize

		if i == len(parts)-1 {
			break
		}

		// All parts except the last part has to be atleast 5MB.
		if !isMinAllowedPartSize(actualSize) {
			return nil, PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   actualSize,
				PartETag:   part.ETag,
			}
		}
	}

	// We'll write the copy using Writer, to avoid re-implementing making of a
	// temp file, cleaning up after partial failures, etc.
	wopts := driver.WriterOptions{
		CacheControl:       xa.CacheControl,
		ContentDisposition: xa.ContentDisposition,
		ContentEncoding:    xa.ContentEncoding,
		ContentLanguage:    xa.ContentLanguage,
		Metadata:           xa.Metadata,
	}
	// Create a cancelable context so we can cancel the write if there are
	// problems.
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	w, err := b.NewTypedWriter(writeCtx, key, xa.ContentType, &wopts)
	if err != nil {
		w.Close()
		return nil, err
	}
	var buf = make([]byte, humanize.MiByte)
	for _, part := range parts {
		partPath := getPartFile(entries, part.PartNumber, part.ETag)
		partFile, err := os.Open(filepath.Join(b.dir, uploadDir, partPath))
		if err != nil {
			w.Close()
			return nil, err
		}
		defer partFile.Close()
		_, err = io.CopyBuffer(w, partFile, buf)
		if err != nil {
			w.Close()
		}
	}

	// we need to close the writer right now so it's write our data
	if err = w.Close(); err != nil {
		return nil, err
	}
	objectPath, finfo, xa2, err := b.forKey(key)
	if err != nil {
		return nil, err
	}
	xa2.Etag = s3MD5
	xa2.Parts = wattrs.Parts
	if xa2.Metadata == nil {
		xa2.Metadata = make(map[string]string)
	}
	xa2.Metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)
	if err = setAttrs(objectPath, *xa2); err != nil {
		return nil, err
	}

	// Ignore the error returned as Windows fails to remove directory if a file in it
	// is Open()ed by the backgroundAppend()
	os.RemoveAll(filepath.Join(b.dir, uploadDir))
	// It is safe to ignore any directory not empty error (in case there were multiple uploadIDs on the same object)
	os.Remove(filepath.Join(b.dir, getMultipartSHADir(key)))

	return &driver.ObjectInfo{
		Key:     key,
		ModTime: finfo.ModTime(),
		Size:    finfo.Size(),
		MD5:     xa2.MD5,
		Etag:    s3MD5,
		IsDir:   false,
	}, nil
}

func (b *bucket) CopyObjectPart(ctx context.Context, dstKey, srcKey, uploadID string, partNumber int, opts *driver.CopyOptions) error {
	// Note: we could use NewRangedReader here, but since we need to copy all of
	// the metadata (from xa), it's more efficient to do it directly.
	srcPath, _, xa, err := b.forKey(srcKey)
	if err != nil {
		return err
	}
	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// We'll write the copy using Writer, to avoid re-implementing making of a
	// temp file, cleaning up after partial failures, etc.
	wopts := driver.WriterOptions{
		CacheControl:       xa.CacheControl,
		ContentDisposition: xa.ContentDisposition,
		ContentEncoding:    xa.ContentEncoding,
		ContentLanguage:    xa.ContentLanguage,
		Metadata:           xa.Metadata,
	}
	// Create a cancelable context so we can cancel the write if there are
	// problems.
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	w, err := b.NewMultipartWriter(writeCtx, dstKey, uploadID, partNumber, &wopts)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, f)
	if err != nil {
		cancel() // cancel before Close cancels the write
		w.Close()
		return err
	}
	_, err = w.Close()
	return err
}

func (b *bucket) NewMultipartWriter(ctx context.Context, key, uploadID string, partNumber int, opts *driver.WriterOptions) (driver.MultipartWriter, error) {
	uploadDir := getUploadIDDir(key, uploadID)
	// check if the multipart upload initialized
	fullpath := filepath.Join(b.dir, uploadDir, fsMultipartJSONFile)
	if _, err := os.Stat(fullpath); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(b.dir, uploadDir)), 0777); err != nil {
		return nil, err
	}
	// create temp file for this
	f, err := ioutil.TempFile(filepath.Dir(filepath.Join(b.dir, uploadDir)), "fileblob")
	if err != nil {
		return nil, err
	}
	var metadata map[string]string
	if len(opts.Metadata) > 0 {
		metadata = opts.Metadata
	}
	attrs := xattrs{
		CacheControl:       opts.CacheControl,
		ContentDisposition: opts.ContentDisposition,
		ContentEncoding:    opts.ContentEncoding,
		ContentLanguage:    opts.ContentLanguage,
		ContentType:        "application/octet-stream",
		Metadata:           metadata,
	}

	w := &multipartWriter{
		ctx:        ctx,
		f:          f,
		path:       filepath.Join(b.dir, uploadDir),
		partNumber: partNumber,
		attrs:      attrs,
		contentMD5: opts.ContentMD5,
		md5hash:    md5.New(),
	}

	return w, nil
}

type multipartWriter struct {
	ctx        context.Context
	f          *os.File
	path       string
	partNumber int
	attrs      xattrs
	contentMD5 []byte
	// We compute the MD5 hash so that we can store it with the file attributes,
	// not for verification.
	md5hash     hash.Hash
	sizeWritten atomic.Uint64
}

func (w *multipartWriter) Write(p []byte) (n int, err error) {
	if _, err := w.md5hash.Write(p); err != nil {
		return 0, err
	}
	n, err = w.f.Write(p)
	if err != nil {
		return n, err
	}
	w.sizeWritten.Add(uint64(n))
	return n, nil
}

func (w *multipartWriter) Close() (driver.PartInfo, error) {
	err := w.f.Close()
	if err != nil {
		return driver.PartInfo{}, err
	}
	// Always delete the temp file. On success, it will have been renamed so
	// the Remove will fail.
	defer func() {
		_ = os.Remove(w.f.Name())
	}()

	// Check if the write was cancelled.
	if err := w.ctx.Err(); err != nil {
		return driver.PartInfo{}, err
	}

	md5sum := w.md5hash.Sum(nil)
	if len(w.contentMD5) > 0 {
		if !bytes.Equal(md5sum, w.contentMD5) {
			return driver.PartInfo{}, &BadDigest{
				ExpectedMD5:   hex.EncodeToString(w.contentMD5),
				CalculatedMD5: hex.EncodeToString(md5sum),
			}
		}
	}
	w.attrs.MD5 = md5sum
	w.attrs.Etag = hex.EncodeToString(md5sum)

	path := filepath.Join(w.path, encodePartFile(w.partNumber, w.attrs.Etag, w.sizeWritten.Load()))

	// Write the attributes file.
	if err = setAttrs(path, w.attrs); err != nil {
		return driver.PartInfo{}, err
	}
	if err = os.Rename(w.f.Name(), path); err != nil {
		return driver.PartInfo{}, err
	}

	return driver.PartInfo{
		PartNumber:   w.partNumber,
		LastModified: time.Now(),
		ETag:         w.attrs.Etag,
		Size:         int64(w.sizeWritten.Load()),
		ActualSize:   int64(w.sizeWritten.Load()),
	}, nil
}
