package mailer

import (
	"io"
	"net/mail"
	"strings"

	"github.com/thatique/awan/mailer/driver"
)

// FormatAdressList fromat the given address list
func FormatAdressList(xs []*mail.Address) string {
	formatted := make([]string, len(xs))
	for i, a := range xs {
		formatted[i] = a.String()
	}
	return strings.Join(formatted, ", ")
}

type wrapWriterTo struct {
	w io.WriterTo
}

// WrapWriterTo wrap standard io.WriterTo so it return `driver.WriterTo`
func WrapWriterTo(w io.WriterTo) driver.WriterTo {
	return &wrapWriterTo{w: w}
}

// WriteTo is the implementation of *wrapWriterTo for `driver.WriterTo`
func (w *wrapWriterTo) WriteTo(io io.Writer) error {
	_, err := w.w.WriteTo(io)
	return err
}
