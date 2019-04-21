package fileblob

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/thatique/awan/blob/driver"
)

const attrsExt = ".attrs"

// ErrAttrsExt is thrown when the caller tried to use attrsExt
var errAttrsExt = fmt.Errorf("file extension %q is reserved", attrsExt)

// xattrs stores extended attributes for an object. The format is like
// filesystem extended attributes, see
// https://www.freedesktop.org/wiki/CommonExtendedAttributes.
type xattrs struct {
	CacheControl       string                  `json:"user.cache_control"`
	ContentDisposition string                  `json:"user.content_disposition"`
	ContentEncoding    string                  `json:"user.content_encoding"`
	ContentLanguage    string                  `json:"user.content_language"`
	ContentType        string                  `json:"user.content_type"`
	Metadata           map[string]string       `json:"user.metadata"`
	MD5                []byte                  `json:"md5"`
	Etag               string                  `json:"etag"`
	Parts              []driver.ObjectPartInfo `json:"parts,omitempty"`
}

// setAttrs creates a "path.attrs" file along with blob to store the attributes,
// it uses JSON format.
func setAttrs(path string, xa xattrs) error {
	f, err := os.Create(path + attrsExt)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(xa); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// getAttrs looks at the "path.attrs" file to retrieve the attributes and
// decodes them into a xattrs struct. It doesn't return error when there is no
// such .attrs file.
func getAttrs(path string) (xattrs, error) {
	f, err := os.Open(path + attrsExt)
	if err != nil {
		if os.IsNotExist(err) {
			// Handle gracefully for non-existent .attr files.
			return xattrs{
				ContentType: "application/octet-stream",
			}, nil
		}
		return xattrs{}, err
	}
	xa := new(xattrs)
	if err := json.NewDecoder(f).Decode(xa); err != nil {
		f.Close()
		return xattrs{}, err
	}
	return *xa, f.Close()
}
