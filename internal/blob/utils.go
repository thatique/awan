package blob

import (
	"crypto/md5"
	"encoding/hex"
	"regexp"
	"strings"

	"github.com/minio/sha256-simd"
	"github.com/thatique/awan/internal/uuid"
)

var (
	etagRegex = regexp.MustCompile("\"*?([^\"]*?)\"*?$")
)

// CanonicalizeETag return the canonicalized etag
func CanonicalizeETag(etag string) string {
	return etagRegex.ReplaceAllString(etag, "$1")
}

// GetSHA256Hash returns SHA-256 hash in hex encoding of given data.
func GetSHA256Hash(data []byte) string {
	return hex.EncodeToString(GetSHA256Sum(data))
}

// GetSHA256Sum returns SHA-256 sum of given data.
func GetSHA256Sum(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// GetMD5Sum returns MD5 sum of given data.
func GetMD5Sum(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// GetMD5Hash returns MD5 hash in hex encoding of given data.
func GetMD5Hash(data []byte) string {
	return hex.EncodeToString(GetMD5Sum(data))
}

// MustGetUUID - get a random UUID.
func MustGetUUID() string {
	uuid := uuid.Generate()

	return uuid.String()
}

// GenETag - generate UUID based ETag
func GenETag() string {
	return ToS3ETag(GetMD5Hash([]byte(MustGetUUID())))
}

// ToS3ETag - return checksum to ETag
func ToS3ETag(etag string) string {
	etag = CanonicalizeETag(etag)

	if !strings.HasSuffix(etag, "-1") {
		// Tools like s3cmd uses ETag as checksum of data to validate.
		// Append "-1" to indicate ETag is not a checksum.
		etag += "-1"
	}

	return etag
}
