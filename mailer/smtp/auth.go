package smtp

import (
	"bytes"
	"errors"
	"fmt"
	"net/smtp"
)

var (
	ErrTLSRequired      = errors.New("mailer.smtp: login mechanism need TLS connection")
	ErrInvalidHost      = errors.New("mailer.smtp: invalid servername/host")
	ErrAuthNotSupported = errors.New("mailer.smpt: Auth not supported")
)

// loginAuth is an smtp.Auth that implements the LOGIN authentication mechanism.
type loginAuth struct {
	username string
	password string
	host     string
}

func (a *loginAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	if !server.TLS {
		advertised := false
		for _, mechanism := range server.Auth {
			if mechanism == "LOGIN" {
				advertised = true
				break
			}
		}
		if !advertised {
			return "", nil, ErrTLSRequired
		}
	}
	if server.Name != a.host {
		return "", nil, ErrInvalidHost
	}
	return "LOGIN", nil, nil
}

func (a *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
	if !more {
		return nil, nil
	}

	switch {
	case bytes.Equal(fromServer, []byte("Username:")):
		return []byte(a.username), nil
	case bytes.Equal(fromServer, []byte("Password:")):
		return []byte(a.password), nil
	default:
		return nil, fmt.Errorf("gomail: unexpected server challenge: %s", fromServer)
	}
}
