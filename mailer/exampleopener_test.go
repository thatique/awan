package mailer_test

import (
	"context"
	"fmt"
	"log"
	"net/smtp"
	"strings"

	"github.com/emersion/go-message"
	"github.com/thatique/awan/mailer"
	_ "github.com/thatique/awan/mailer/smtp"

	"github.com/ory/dockertest"
)

func ExampleOpenTransport() {
	cleanup, host := prepareSMTPServer()
	defer cleanup()

	ctx := context.Background()
	transport, err := mailer.OpenTransport(ctx, fmt.Sprintf("smtp://foo:secrets@%s", host))
	if err != nil {
		log.Fatal(err)
	}
	defer transport.Close()

	// lets send an email
	h1 := make(message.Header)
	h1.Set("Content-Type", "text/plain")
	e1, _ := message.New(h1, strings.NewReader("this is a test"))

	h2 := make(message.Header)
	h2.Set("Content-Type", "text/html")
	r2 := strings.NewReader("<p>this is a test</p>")
	e2, _ := message.New(h2, r2)

	h := make(message.Header)
	h.Set("Sender", "foo@localhost")
	h.Set("From", "foo@localhost")
	h.Set("To", "bar@localhost")
	h.Set("Subject", "Test URL Opener")
	h.Set("Content-Type", "multipart/alternative; boundary=IMTHEBOUNDARY")
	e, _ := message.NewMultipart(h, []*message.Entity{e1, e2})

	err = transport.SendMessage(ctx, e)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("email sent")
	// Output:
	// email sent
}

func prepareSMTPServer() (func(), string) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatal(err)
	}

	resource, err := pool.Run("mailhog/mailhog", "v1.0.0", []string{})
	if err != nil {
		log.Fatal(err)
	}
	cleanup := func() {
		pool.Purge(resource)
	}
	setup := func() error {
		c, err := smtp.Dial("localhost:1025")
		if err != nil {
			return err
		}
		if err = c.Hello("localhost"); err != nil {
			return err
		}

		return c.Quit()
	}

	if pool.Retry(setup); err != nil {
		cleanup()
		log.Fatal(err)
	}
	return cleanup, fmt.Sprintf("127.0.0.1:%s", resource.GetPort("1025/tcp"))
}