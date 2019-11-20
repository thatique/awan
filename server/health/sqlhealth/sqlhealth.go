package sqlhealth

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Checker checks the health of a SQL database
type Checker struct {
	cancel context.CancelFunc

	stopped <-chan struct{}
	healthy bool
}

// New starts a new asynchronous ping of the SQL database. Pings will be sent
// until one succeeds or Stop is called, whichever comes first.
func New(db *sql.DB) *Checker {
	// create a context here because we are detaching.
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	c := &Checker{cancel: cancel, stopped: stopped}
	go func() {
		var timer *time.Timer
		defer func() {
			if timer != nil {
				timer.Stop()
			}
			close(stopped)
		}()

		wait := 250 * time.Millisecond
		const maxWait = 30 * time.Second
		for {
			if err := db.PingContext(ctx); err == nil {
				c.healthy = true
				return
			}
			if timer == nil {
				timer = time.NewTimer(wait)
			} else {
				timer.Reset(wait)
			}
			select {
			case <-timer.C:
				if wait < maxWait {
					wait *= 2
					if wait > maxWait {
						wait = maxWait
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return c
}

func (c *Checker) CheckHealth() error {
	select {
	case <-c.stopped:
		if !c.healthy {
			return errors.New("ping stopped before becoming healthy")
		}
		return nil
	default:
		return errors.New("still pinging database")
	}
}

func (c *Checker) Stop() {
	c.cancel()
	<-c.stopped
}
