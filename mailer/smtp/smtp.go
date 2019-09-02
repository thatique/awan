package smtp

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/smtp"
	"net/url"
	"strings"
	"sync"

	"github.com/thatique/awan/mailer"
	"github.com/thatique/awan/mailer/driver"
	"github.com/thatique/awan/verr"
)

var (
	// ErrAlreadyClosed the connection already closed
	ErrAlreadyClosed = errors.New("mail.smtp: already closed")
	// ErrConnNotEstablished returned when we can't establish a connection to
	// smtp server
	ErrConnNotEstablished = errors.New("mailer.smtp: connection to smtp server not establish")
)

// Scheme is constant for our scheme when using URL opener
const Scheme = "smtp"

func init() {
	mailer.DefaultURLMux().RegisterTransport(Scheme, new(URLOpener))
}

// URLOpener opens Mailer URLs like
// smtp://username:password@host:port
type URLOpener struct{}

// OpenTransportURL open `mailer.Transport`
func (uo *URLOpener) OpenTransportURL(ctx context.Context, u *url.URL) (*mailer.Transport, error) {
	options := &Options{
		Addr: u.Host,
	}
	if u.User != nil {
		pswd, isset := u.User.Password()
		if isset {
			options.Password = pswd
		}
		options.Username = u.User.Username()
	}
	return NewTransport(options)
}

// NewTransport create ne instance of `mailer.Transport` using SMTP backend
func NewTransport(options *Options) (*mailer.Transport, error) {
	dr, err := newSMTPTransport(options)
	if err != nil {
		return nil, err
	}
	return mailer.NewTransport(dr), nil
}

// Options for connecting to SMTP server
type Options struct {
	// The addr must include a port, as in "mail.example.com:smtp".
	Addr string
	// Username is the username to use to authenticate to the SMTP server.
	Username string
	// Password is the password to use to authenticate to the SMTP server.
	Password string
}

type smtpTransport struct {
	locker sync.Mutex
	conn   *smtp.Client
	closed bool
	option *Options

	serverName string
}

func newSMTPTransport(option *Options) (*smtpTransport, error) {
	host, _, _ := net.SplitHostPort(option.Addr)

	t := &smtpTransport{
		option:     option,
		serverName: host,
	}
	return t, nil
}

func (t *smtpTransport) Send(ctx context.Context, from string, to []string, msg driver.WriterTo) error {
	c := make(chan error, 1)
	go func() { c <- t.send(from, to, msg) }()

	select {
	case <-ctx.Done():
		<-c
		return ctx.Err()
	case err := <-c:
		return err
	}
}

func (t *smtpTransport) send(from string, to []string, msg driver.WriterTo) (err error) {
	t.locker.Lock()
	defer func() {
		// close connection after this
		t.closeSMTPConnection()
		t.locker.Unlock()
	}()

	if t.closed {
		return ErrAlreadyClosed
	}

	if err = t.open(); err != nil {
		return
	}

	if err = t.conn.Mail(from); err != nil {
		return err
	}

	for _, addr := range to {
		if err = t.conn.Rcpt(addr); err != nil {
			return err
		}
	}

	w, err := t.conn.Data()

	if err != nil {
		return err
	}

	if err = msg.WriteTo(w); err != nil {
		return err
	}

	return nil
}

// Close close the SMTP transport connection
func (t *smtpTransport) Close() error {
	t.locker.Lock()
	defer t.locker.Unlock()
	t.closed = true
	return t.closeSMTPConnection()
}

func (t *smtpTransport) closeSMTPConnection() error {
	if t.conn == nil {
		return nil
	}

	err := t.conn.Quit()
	t.conn = nil
	return err
}

func (t *smtpTransport) open() error {
	c, err := smtp.Dial(t.option.Addr)
	if err != nil {
		return err
	}

	if err = c.Hello("localhost"); err != nil {
		return err
	}

	// Start TLS if possible
	if ok, _ := c.Extension("STARTTLS"); ok {
		config := &tls.Config{ServerName: t.serverName}
		if err = c.StartTLS(config); err != nil {
			return err
		}
	}

	// auth is non nil
	if t.option.Username != "" {
		if ok, auths := c.Extension("AUTH"); ok {
			var auth smtp.Auth
			if strings.Contains(auths, "CRAM-MD5") {
				auth = smtp.CRAMMD5Auth(t.option.Username, t.option.Password)
			} else if strings.Contains(auths, "LOGIN") &&
				!strings.Contains(auths, "PLAIN") {
				auth = &loginAuth{
					username: t.option.Username,
					password: t.option.Password,
					host:     t.serverName,
				}
			} else {
				auth = smtp.PlainAuth("", t.option.Username, t.option.Password, t.serverName)
			}

			if err = c.Auth(auth); err != nil {
				return err
			}
		} else {
			return ErrAuthNotSupported
		}
	}

	// connection establish, store it and return
	t.conn = c

	return nil
}

func (t *smtpTransport) ErrorCode(err error) verr.ErrorCode {
	if err == nil {
		return verr.OK
	}
	if err == ErrTLSRequired || err == ErrInvalidHost || err == ErrAuthNotSupported {
		return verr.InvalidArgument
	}

	if err == ErrConnNotEstablished {
		return verr.FailedPrecondition
	}

	return verr.Unknown
}
