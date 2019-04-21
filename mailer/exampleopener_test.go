package mailer_test

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/emersion/go-message"
	"github.com/thatique/awan/mailer"
	_ "github.com/thatique/awan/mailer/smtp"
)

func ExampleOpenTransport() {
	ctx := context.Background()
	// use mailhog or python's smtp debugging server on port 1025
	transport, err := mailer.OpenTransport(ctx, "smtp://foo:secrets@localhost:1025")
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
