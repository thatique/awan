package driver

import (
	"context"
	"io"
	"time"

	"github.com/thatique/awan/verr"
)

// Bucket provides read, write and delete operations on objects within it on the
// blob service.
type Bucket interface {
	// ErrorCode should return a code that describes the error, which was returned by
	// one of the other methods in this interface.
	ErrorCode(error) verr.ErrorCode

	// Attributes returns attributes for the blob. If the specified object does
	// not exist, Attributes must return an error for which ErrorCode returns
	// gcerrors.NotFound.
	Attributes(ctx context.Context, key string) (Attributes, error)

	// ListPaged lists objects in the bucket, in lexicographical order by
	// UTF-8-encoded key, returning pages of objects at a time.
	// Providers are only required to be eventually consistent with respect
	// to recently written or deleted objects. That is to say, there is no
	// guarantee that an object that's been written will immediately be returned
	// from ListPaged.
	// opts is guaranteed to be non-nil.
	ListPaged(ctx context.Context, opts *ListOptions) (*ListObjectsInfo, error)

	// NewRangeReader returns a Reader that reads part of an object, reading at
	// most length bytes starting at the given offset. If length is negative, it
	// will read until the end of the object. If the specified object does not
	// exist, NewRangeReader must return an error for which ErrorCode returns
	// gcerrors.NotFound.
	// opts is guaranteed to be non-nil.
	NewRangeReader(ctx context.Context, key string, offset, length int64, opts *ReaderOptions) (Reader, error)

	// NewTypedWriter returns Writer that writes to an object associated with key.
	//
	// A new object will be created unless an object with this key already exists.
	// Otherwise any previous object with the same key will be replaced.
	// The object may not be available (and any previous object will remain)
	// until Close has been called.
	//
	// contentType sets the MIME type of the object to be written. It must not be
	// empty. opts is guaranteed to be non-nil.
	//
	// The caller must call Close on the returned Writer when done writing.
	//
	// Implementations should abort an ongoing write if ctx is later canceled,
	// and do any necessary cleanup in Close. Close should then return ctx.Err().
	NewTypedWriter(ctx context.Context, key, contentType string, opts *WriterOptions) (Writer, error)

	// Copy copies the object associated with srcKey to dstKey.
	//
	// If the source object does not exist, Copy must return an error for which
	// ErrorCode returns verr.NotFound.
	//
	// If the destination object already exists, it should be overwritten.
	//
	// opts is guaranteed to be non-nil.
	Copy(ctx context.Context, dstKey, srcKey string, opts *CopyOptions) error

	// Delete deletes the object associated with key. If the specified object does
	// not exist, Delete must return an error for which ErrorCode returns
	// verr.NotFound.
	Delete(ctx context.Context, key string) error

	// SignedURL returns a URL that can be used to GET the blob for the duration
	// specified in opts.Expiry. opts is guaranteed to be non-nil.
	// If not supported, return an error for which ErrorCode returns
	// verr.Unimplemented.
	SignedURL(ctx context.Context, key string, opts *SignedURLOptions) (string, error)

	// NewMultipartUpload initialize multipart uploads and return upload ID,
	// which is a unique identifier for your multipart upload. You must include
	// this upload ID whenever you upload parts, list the parts, complete an upload,
	// or abort an upload. If you want to provide any metadata describing the object being uploaded,
	// you must provide it in the request to initiate multipart upload.
	NewMultipartUpload(ctx context.Context, key, contentType string, opts *WriterOptions) (string, error)

	// AbortMultipartUpload, abort the multipart upload
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error

	// Complete the multipart upload step
	CompleteMultipartUpload(ctx context.Context, key, uploadID string, uploadedParts []CompletePart) (objInfo *ObjectInfo, err error)

	// ListMultipartUploads list all incomplete multipart uploads
	ListMultipartUploads(ctx context.Context, key string, opts *ListMultipartsOptions) (*ListMultipartsInfo, error)

	// Uploads a part by copying data from an existing object as data source.
	CopyObjectPart(ctx context.Context, dstKey, srcKey, uploadID string, partNumber int, opts *CopyOptions) error

	// NewMultipartWriter returns Writer that writes to an object part associated with key.
	NewMultipartWriter(ctx context.Context, key, uploadID string, partID int, opts *WriterOptions) (MultipartWriter, error)

	// ListObjectParts
	ListObjectParts(ctx context.Context, key, uploadID string, opts *ListPartsOptions) (*ListPartsInfo, error)

	// Close cleans up any resources used by the Bucket. Once Close is called,
	// there will be no method calls to the Bucket other than As, ErrorAs, and
	// ErrorCode. There may be open readers or writers that will receive calls.
	// It is up to the driver as to how these will be handled.
	Close() error
}

// ReaderOptions controls Reader behaviors. It is provided for future extensibility.
type ReaderOptions struct{}

// Reader reads an object from the blob.
type Reader interface {
	io.ReadCloser

	// Attributes returns a subset of attributes about the blob.
	Attributes() ReaderAttributes
}

// Writer writes an object to the blob.
type Writer interface {
	io.WriteCloser
}

// MultipartWriter write a part
type MultipartWriter interface {
	io.Writer

	Close() (info PartInfo, err error)
}

// WriterOptions controls behaviors of Writer.
type WriterOptions struct {
	// BufferSize changes the default size in byte of the maximum part Writer can
	// write in a single request, if supported. Larger objects will be split into
	// multiple requests.
	BufferSize int
	// CacheControl specifies caching attributes that providers may use
	// when serving the blob.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control
	CacheControl string
	// ContentDisposition specifies whether the blob content is expected to be
	// displayed inline or as an attachment.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition
	ContentDisposition string
	// ContentEncoding specifies the encoding used for the blob's content, if any.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding
	ContentEncoding string
	// ContentLanguage specifies the language used in the blob's content, if any.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Language
	ContentLanguage string
	// ContentMD5 is used as a message integrity check.
	// The portable type checks that the MD5 hash of the bytes written matches
	// ContentMD5.
	// If len(ContentMD5) > 0, driver implementations may pass it to their
	// underlying network service to guarantee the integrity of the bytes in
	// transit.
	ContentMD5 []byte
	// Metadata holds key/value strings to be associated with the blob.
	// Keys are guaranteed to be non-empty and lowercased.
	Metadata map[string]string
}

// CopyOptions controls options for Copy. It's provided for future extensibility.
type CopyOptions struct {
}

// ReaderAttributes contains a subset of attributes about a blob that are
// accessible from Reader.
type ReaderAttributes struct {
	// ContentType is the MIME type of the blob object. It must not be empty.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type
	ContentType string
	// ModTime is the time the blob object was last modified.
	ModTime time.Time
	// Size is the size of the object in bytes.
	Size int64
}

// Attributes contains attributes about a blob.
type Attributes struct {
	// CacheControl specifies caching attributes that providers may use
	// when serving the blob.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control
	CacheControl string
	// ContentDisposition specifies whether the blob content is expected to be
	// displayed inline or as an attachment.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition
	ContentDisposition string
	// ContentEncoding specifies the encoding used for the blob's content, if any.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding
	ContentEncoding string
	// ContentLanguage specifies the language used in the blob's content, if any.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Language
	ContentLanguage string
	// ContentType is the MIME type of the blob object. It must not be empty.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type
	ContentType string
	// Metadata holds key/value pairs associated with the blob.
	// Keys will be lowercased by the portable type before being returned
	// to the user. If there are duplicate case-insensitive keys (e.g.,
	// "foo" and "FOO"), only one value will be kept, and it is undefined
	// which one.
	Metadata map[string]string
	// ModTime is the time the blob object was last modified.
	ModTime time.Time
	// Size is the size of the object in bytes.
	Size int64
	// MD5 is an MD5 hash of the blob contents or nil if not available.
	MD5 []byte
	// ETag is the HTTP/1.1 Entity tag for the object. This field is readonly
	ETag string
	// List of individual parts, maximum size of upto 10,000
	Parts []ObjectPartInfo
}

// ListOptions sets options for listing objects in the bucket.
type ListOptions struct {
	// Prefix indicates that only results with the given prefix should be
	// returned.
	Prefix string
	// Delimiter sets the delimiter used to define a hierarchical namespace,
	// like a filesystem with "directories".
	//
	// An empty delimiter means that the bucket is treated as a single flat
	// namespace.
	//
	// A non-empty delimiter means that any result with the delimiter in its key
	// after Prefix is stripped will be returned with ListObject.IsDir = true,
	// ListObject.Key truncated after the delimiter, and zero values for other
	// ListObject fields. These results represent "directories". Multiple results
	// in a "directory" are returned as a single result.
	Delimiter string
	// PageSize sets the maximum number of objects to be returned.
	// 0 means no maximum; driver implementations should choose a reasonable
	// max. It is guaranteed to be >= 0.
	PageSize int
	// PageToken may be filled in with the NextPageToken from a previous
	// ListPaged call.
	PageToken []byte
}

// ObjectInfo represents a specific blob object returned from ListPaged.
type ObjectInfo struct {
	// Key is the key for this blob.
	Key string
	// ModTime is the time the blob object was last modified.
	ModTime time.Time
	// Size is the size of the object in bytes.
	Size int64
	// MD5 is an MD5 hash of the blob contents or nil if not available.
	MD5 []byte
	// ETag is the HTTP/1.1 Entity tag for the object. This field is readonly
	ETag string
	// IsDir indicates that this result represents a "directory" in the
	// hierarchical namespace, ending in ListOptions.Delimiter. Key can be
	// passed as ListOptions.Prefix to list items in the "directory".
	// Fields other than Key and IsDir will not be set if IsDir is true.
	IsDir bool
}

// ListObjectsInfo represents a page of results return from ListPaged.
type ListObjectsInfo struct {
	// Objects is the slice of objects found. If ListOptions.PageSize > 0,
	// it should have at most ListOptions.PageSize entries.
	//
	// Objects should be returned in lexicographical order of UTF-8 encoded keys,
	// including across pages. I.e., all objects returned from a ListPage request
	// made using a PageToken from a previous ListPage request's NextPageToken
	// should have Key >= the Key for all objects from the previous request.
	Objects []*ObjectInfo
	// NextPageToken should be left empty unless there are more objects
	// to return. The value may be returned as ListOptions.PageToken on a
	// subsequent ListPaged call, to fetch the next page of results.
	// It can be an arbitrary []byte; it need not be a valid key.
	NextPageToken []byte
}

// ObjectPartInfo Info of each part kept in the multipart metadata
// file after CompleteMultipartUpload() is called.
type ObjectPartInfo struct {
	Number     int
	Name       string
	ETag       string
	Size       int64
	ActualSize int64
}

// ListPartsOptions is option for listing ListPartsInfo
type ListPartsOptions struct {
	PartNumberMarker int
	MaxParts         int
}

// ListPartsInfo - represents list of all parts.
type ListPartsInfo struct {
	// Key is the key for this blob.
	Key string

	// Upload ID identifying the multipart upload whose parts are being listed.
	UploadID string

	// Part number after which listing begins.
	PartNumberMarker int

	// this element specifies the last part in the list, as well as the value to use
	// for the part-number-marker request parameter in a subsequent request.
	NextPartNumberMarker int

	// Maximum number of parts that were allowed in the response.
	MaxParts int

	// List of all parts.
	Parts []PartInfo

	// Any metadata set during InitMultipartUpload
	Metadata map[string]string

	EncodingType string // Not supported yet.
}

// ListMultipartsOptions - is option to list all incomplete multipart uploads
type ListMultipartsOptions struct {
	KeyMarker      string
	UploadIDMarker string
	Delimiter      string
	MaxUploads     int
}

// ListMultipartsInfo - represnets bucket resources for incomplete multipart uploads.
type ListMultipartsInfo struct {
	// Together with upload-id-marker, this parameter specifies the multipart upload
	// after which listing should begin.
	KeyMarker string

	// Together with key-marker, specifies the multipart upload after which listing
	// should begin. If key-marker is not specified, the upload-id-marker parameter
	// is ignored.
	UploadIDMarker string

	// When a list is truncated, this element specifies the value that should be
	// used for the key-marker request parameter in a subsequent request.
	NextKeyMarker string

	// When a list is truncated, this element specifies the value that should be
	// used for the upload-id-marker request parameter in a subsequent request.
	NextUploadIDMarker string

	// Maximum number of multipart uploads that could have been included in the
	// response.
	MaxUploads int

	// Indicates whether the returned list of multipart uploads is truncated. A
	// value of true indicates that the list was truncated. The list can be truncated
	// if the number of multipart uploads exceeds the limit allowed or specified
	// by max uploads.
	IsTruncated bool

	// List of all pending uploads.
	Uploads []MultipartInfo

	// When a prefix is provided in the request, The result contains only keys
	// starting with the specified prefix.
	Prefix string

	// A character used to truncate the object prefixes.
	// NOTE: only supported delimiter is '/'.
	Delimiter string

	// CommonPrefixes contains all (if there are any) keys between Prefix and the
	// next occurrence of the string specified by delimiter.
	CommonPrefixes []string

	EncodingType string // Not supported yet.
}

// PartInfo - represents individual part metadata.
type PartInfo struct {
	// Part number that identifies the part. This is a positive integer between
	// 1 and 10,000.
	PartNumber int

	// Date and time at which the part was uploaded.
	LastModified time.Time

	// Entity tag returned when the part was initially uploaded.
	ETag string

	// Size in bytes of the part.
	Size int64

	// Decompressed Size.
	ActualSize int64
}

// MultipartInfo - represents metadata in progress multipart upload.
type MultipartInfo struct {
	// Object name for which the multipart upload was initiated.
	Key string

	// Unique identifier for this multipart upload.
	UploadID string

	// Date and time at which the multipart upload was initiated.
	Initiated time.Time

	StorageClass string // Not supported yet.
}

// CompletePart - represents the part that was completed, this is sent by the client
// during CompleteMultipartUpload request.
type CompletePart struct {
	// Part number identifying the part. This is a positive integer between 1 and
	// 10,000
	PartNumber int

	// Entity tag returned when the part was uploaded.
	ETag string
}

// CompletedParts - is a collection satisfying sort.Interface.
type CompletedParts []CompletePart

func (a CompletedParts) Len() int           { return len(a) }
func (a CompletedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a CompletedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

// SignedURLOptions sets options for SignedURL.
type SignedURLOptions struct {
	// Expiry sets how long the returned URL is valid for. It is guaranteed to be > 0.
	Expiry time.Duration
}
