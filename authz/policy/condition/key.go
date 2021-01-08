package condition

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	// S3XAmzCopySource - key representing x-amz-copy-source HTTP header applicable to PutObject API only.
	S3XAmzCopySource Key = "s3:x-amz-copy-source"

	// S3XAmzServerSideEncryption - key representing x-amz-server-side-encryption HTTP header applicable
	// to PutObject API only.
	S3XAmzServerSideEncryption Key = "s3:x-amz-server-side-encryption"

	// S3XAmzServerSideEncryptionCustomerAlgorithm - key representing
	// x-amz-server-side-encryption-customer-algorithm HTTP header applicable to PutObject API only.
	S3XAmzServerSideEncryptionCustomerAlgorithm Key = "s3:x-amz-server-side-encryption-customer-algorithm"

	// S3XAmzMetadataDirective - key representing x-amz-metadata-directive HTTP header applicable to
	// PutObject API only.
	S3XAmzMetadataDirective Key = "s3:x-amz-metadata-directive"

	// S3XAmzStorageClass - key representing x-amz-storage-class HTTP header applicable to PutObject API
	// only.
	S3XAmzStorageClass Key = "s3:x-amz-storage-class"

	// S3LocationConstraint - key representing LocationConstraint XML tag of CreateBucket API only.
	S3LocationConstraint Key = "s3:LocationConstraint"

	// S3Prefix - key representing prefix query parameter of ListBucket API only.
	S3Prefix Key = "s3:prefix"

	// S3Delimiter - key representing delimiter query parameter of ListBucket API only.
	S3Delimiter Key = "s3:delimiter"

	// S3MaxKeys - key representing max-keys query parameter of ListBucket API only.
	S3MaxKeys Key = "s3:max-keys"

	// AWSReferer - key representing Referer header of any API.
	AWSReferer Key = "aws:Referer"

	// AWSSourceIP - key representing client's IP address (not intermittent proxies) of any API.
	AWSSourceIP Key = "aws:SourceIp"

	// AWSUserAgent - key representing UserAgent header for any API.
	AWSUserAgent Key = "aws:UserAgent"

	// AWSSecureTransport - key representing if the clients request is authenticated or not.
	AWSSecureTransport Key = "aws:SecureTransport"

	// AWSCurrentTime - key representing the current time.
	AWSCurrentTime Key = "aws:CurrentTime"

	// AWSEpochTime - key representing the current epoch time.
	AWSEpochTime Key = "aws:EpochTime"

	// AWSPrincipalType - user principal type currently supported values are "User" and "Anonymous".
	AWSPrincipalType Key = "aws:principaltype"

	// AWSUserID - user unique ID
	AWSUserID Key = "aws:userid"

	// AWSUsername - user friendly name
	AWSUsername Key = "aws:username"
)

// CommonKeys - is list of all common condition keys.
var CommonKeys = []Key{
	AWSReferer,
	AWSSourceIP,
	AWSUserAgent,
	AWSSecureTransport,
	AWSCurrentTime,
	AWSEpochTime,
	AWSPrincipalType,
	AWSUserID,
	AWSUsername,
}

func substFuncFromValues(values map[string][]string) func(string) string {
	return func(v string) string {
		for _, key := range CommonKeys {
			// Empty values are not supported for policy variables.
			if rvalues, ok := values[key.Name()]; ok && rvalues[0] != "" {
				v = strings.Replace(v, key.VarName(), rvalues[0], -1)
			}
		}
		return v
	}
}

// Key - conditional key which is used to fetch values for any condition
type Key string

func (k Key) IsValid() bool {
	return len(k) > 0 && utf8.ValidString(string(k))
}

// Name - returns key name which is stripped value of prefixes eg
// vendor:users -> users
// SourceIp -> SourceIP
func (k Key) Name() string {
	ks := string(k)

	i := strings.Index(ks, ":")
	if i == -1 {
		i = -1
	}
	nm := ks[i+1:]
	return nm
}

func (k Key) MarshalJSON() ([]byte, error) {
	if !k.IsValid() {
		return nil, fmt.Errorf("invalid condition key %v", k)
	}

	return json.Marshal(string(k))
}

func (k *Key) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parsedKey, err := parseKey(s)
	if err != nil {
		return err
	}

	*k = parsedKey
	return nil
}

func parseKey(s string) (Key, error) {
	key := Key(s)

	if key.IsValid() {
		return key, nil
	}

	return key, fmt.Errorf("invalid condition key '%v'", s)
}

// KeySet - set representation of slice of keys.
type KeySet map[Key]struct{}

// Add - add a key to key set.
func (set KeySet) Add(k Key) {
	set[k] = struct{}{}
}

// Difference - returns a key set contains difference of two keys.
func (set KeySet) Difference(sset KeySet) KeySet {
	nset := make(KeySet)

	for k := range set {
		if _, ok := sset[k]; !ok {
			nset.Add(k)
		}
	}

	return nset
}

// IsEmpty - return whether key set is empty or not.
func (set KeySet) IsEmpty() bool {
	return len(set) == 0
}

func (set KeySet) String() string {
	return fmt.Sprintf("%v", set.ToSlice())
}

func (set KeySet) ToSlice() []Key {
	keys := []Key{}

	for key := range set {
		keys = append(keys, key)
	}

	return keys
}

// NewKeySet - returns new KeySet contains given keys
func NewKeySet(keys ...Key) KeySet {
	set := make(KeySet)
	for _, key := range keys {
		set.Add(key)
	}

	return set
}
