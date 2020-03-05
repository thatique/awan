package drivertest

import (
	"context"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"github.com/thatique/awan/session"
	"github.com/thatique/awan/session/driver"
)

// RunConformanceTests run all tests for the given storage
func RunConformanceTests(t *testing.T, storage driver.Storage) {
	t.Run("Delete session", func(t *testing.T) {
		testDelete(t, storage)
	})
}

func testDelete(t *testing.T, storage driver.Storage) {
	ctx := context.Background()
	sid := session.GenerateSessionID()

	if err := storage.Delete(ctx, sid); err != nil {
		t.Errorf("deleteSession should not fail for inexistent sessions. %v", err)
	}

	rnd := rand.New(rand.NewSource(0))
	for i := 0; i < 20; i++ {
		sess := generateSession(rnd, false)
		sid = sess.ID

		// verify this session not in storage
		esess, err := storage.Get(ctx, sid)
		if err != nil {
			t.Errorf("storage.Get() should not fail for inexistent session. %v", err)
			break
		}
		if esess != nil {
			t.Errorf("there is already a session with %s ID", sid)
			break
		}

		// insert it
		if err = storage.Insert(ctx, sess); err != nil {
			t.Errorf("failed to insert a session")
			break
		}

		// then get that session
		sess2, err := storage.Get(ctx, sid)
		if err != nil {
			t.Errorf("storage.Get() should not fail for existent session. %v", err)
			break
		}
		if sess2 == nil {
			t.Errorf("storage.Get should return session that already inserted with the given ID")
			break
		}

		// delete it
		if err = storage.Delete(ctx, sid); err != nil {
			t.Errorf("storage.Delete() failed to delete existent session. %v", err)
			break
		}

		// then get should return nothing
		esess, err = storage.Get(ctx, sid)
		if err != nil {
			t.Errorf("storage.Get() should not fail for inexistent session. %v", err)
			break
		}
		if esess != nil {
			t.Errorf("there is already a session with %s ID", sid)
			break
		}
	}
}

func generateSession(rnd *rand.Rand, hashAuthID bool) *driver.Session {
	sid := session.GenerateSessionID()

	var values map[interface{}]interface{}
	for {
		t := reflect.TypeOf(values)
		v, ok := quick.Value(t, rnd)
		if !ok {
			continue
		}

		values = v.Interface().(map[interface{}]interface{})
		break
	}

	authID := ""
	if hashAuthID {
		authID = session.GenerateSessionID()
	}

	sess := driver.NewSession(sid, authID, time.Now().UTC())
	sess.Values = values

	return sess
}
