package driver

import (
	"context"
	"io"

	"github.com/thatique/awan/verr"
)

// WriterTo is like standard io.Writer without the need to return the length written
type WriterTo interface {
	// WriteTo write this data to provided io.Writer and return non nil error if success
	WriteTo(w io.Writer) error
}

// Transport provides functionality for sending email
type Transport interface {
	// Send send email to provided address, if connection lost when user call
	// this, then the transport should reconnect if it can, otherwise return
	// error.
	Send(ctx context.Context, from string, to []string, msg WriterTo) error

	// Close, close the connection. Once Close is called, there will be no method
	// except `ErrorCode` calls to Transport method that able to success.
	Close() error

	// ErrorCode should return a code that describes the error, which was returned by
	// one of the other methods in this interface.
	ErrorCode(err error) verr.ErrorCode
}
