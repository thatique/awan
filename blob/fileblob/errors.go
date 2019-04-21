package fileblob

import "fmt"

// BadDigest - Content-MD5 you specified did not match what we received.
type BadDigest struct {
	ExpectedMD5   string
	CalculatedMD5 string
}

func (e BadDigest) Error() string {
	return "Bad digest: Expected " + e.ExpectedMD5 + " is not valid with what we calculated " + e.CalculatedMD5
}

// InvalidPart One or more of the specified parts could not be found
type InvalidPart struct {
	PartNumber int
	ExpETag    string
	GotETag    string
}

func (e InvalidPart) Error() string {
	return fmt.Sprintf("Specified part could not be found. PartNumber %d, Expected %s, got %s",
		e.PartNumber, e.ExpETag, e.GotETag)
}

// PartTooSmall thrown when the part is too small
type PartTooSmall struct {
	PartNumber int
	PartSize   int64
	PartETag   string
}

func (e PartTooSmall) Error() string {
	return fmt.Sprintf("Part size for %d should be at least 5MB", e.PartNumber)
}
