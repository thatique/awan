package mailer

import (
	"context"
	"net/mail"

	"github.com/emersion/go-message"
	"github.com/thatique/awan/internal/trace"
	"github.com/thatique/awan/mailer/driver"
	"github.com/thatique/awan/verr"
)

const pkgName = "github.com/thatique/awan/mailer"

var (
	latencyMeasure = trace.LatencyMeasure(pkgName)

	// OpenCensusViews are predefined views for OpenCensus metrics.
	// The views include counts and latency distributions for API method calls.
	// See the example at https://godoc.org/go.opencensus.io/stats/view for usage.
	OpenCensusViews = trace.Views(pkgName, latencyMeasure)
)

// Transport is transport to send email
type Transport struct {
	transport driver.Transport
	tracer    *trace.Tracer
}

// NewTransport initialize transport
func NewTransport(transport driver.Transport) *Transport {
	return &Transport{
		transport: transport,
		tracer: &trace.Tracer{
			Package:        pkgName,
			Provider:       trace.ProviderName(transport),
			LatencyMeasure: latencyMeasure,
		},
	}
}

// Send send email to provided sender and recipient, the `WriterTo` should write
// well formatted email message.
func (t *Transport) Send(ctx context.Context, from string, to []string, msg driver.WriterTo) (err error) {
	ctx = t.tracer.Start(ctx, "Send")
	defer func() { t.tracer.End(ctx, err) }()

	err = t.transport.Send(ctx, from, to, msg)
	if err != nil {
		err = wrapError(t, err)
	}
	return
}

// SendMessage send `message.Entity`, the sender and recipients is taken from the
// message entity
func (t *Transport) SendMessage(ctx context.Context, msg *message.Entity) (err error) {
	ctx = t.tracer.Start(ctx, "SendMessage")
	defer func() { t.tracer.End(ctx, err) }()

	var (
		headerPrefix string
		fromAddrStr  string
	)

	resent := msg.Header.Get("Resent-Date")
	if resent != "" {
		headerPrefix = "Resent-"
	}

	if sender := msg.Header.Get(headerPrefix + "Sender"); sender != "" {
		fromAddrStr = sender
	} else if sender = msg.Header.Get(headerPrefix + "From"); sender != "" {
		fromAddrStr = sender
	}

	fromAddrs, err := mail.ParseAddressList(fromAddrStr)
	if err != nil || len(fromAddrs) == 0 {
		return err
	}

	var toAddrs []string
	for _, key := range []string{"To", "Bcc", "Cc"} {
		addrList := msg.Header.Get(headerPrefix + key)
		if addrList == "" {
			continue
		}
		addressList, err := mail.ParseAddressList(addrList)
		if err != nil {
			continue
		}
		for _, address := range addressList {
			toAddrs = append(toAddrs, address.Address)
		}
	}

	return t.Send(ctx, fromAddrs[0].Address, toAddrs, msg)
}

// Close the connection
func (t *Transport) Close() (err error) {
	ctx := t.tracer.Start(context.Background(), "Close")
	defer func() { t.tracer.End(ctx, err) }()

	err = t.transport.Close()
	if err != nil {
		err = wrapError(t, err)
	}
	return
}

func wrapError(t *Transport, err error) error {
	if err == nil {
		return nil
	}
	if verr.DoNotWrap(err) {
		return err
	}
	return verr.New(t.transport.ErrorCode(err), err, 2, "mailer")
}
