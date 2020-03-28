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
	t.Run("Insert and get", func(t *testing.T) {
		testInsertGet(t, storage)
	})
	t.Run("Delete session", func(t *testing.T) {
		testDelete(t, storage)
	})
	t.Run("Insert Conflict", func(t *testing.T) {
		insertSessionThrowIfExists(t, storage)
	})
}

func testInsertGet(t *testing.T, storage driver.Storage) {
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(1))

	var (
		err error
	)
	for i := 0; i < 5; i++ {
		sess := generateSession(rnd, true)

		if err = storage.Insert(ctx, sess); err != nil {
			t.Errorf("failed to insert a session: %v", err)
			break
		}

		sess2, err := storage.Get(ctx, sess.ID)
		if err != nil {
			t.Errorf("failed to get inserted session: %v", err)
			break
		}

		if !sess.Equal(sess2) {
			t.Error("getting inserted session is not equals")
			break
		}

		storage.Delete(ctx, sess.ID)
	}
}

func testDelete(t *testing.T, storage driver.Storage) {
	ctx := context.Background()
	sid := session.GenerateSessionID()

	if err := storage.Delete(ctx, sid); err != nil {
		t.Errorf("deleteSession should not fail for inexistent sessions. %v", err)
	}

	rnd := rand.New(rand.NewSource(0))
	for i := 0; i < 5; i++ {
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

func insertSessionThrowIfExists(t *testing.T, storage driver.Storage) {
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(0))
	for i := 0; i < 5; i++ {
		sess1 := generateSession(rnd, false)
		sess2 := generateSession(rnd, false)
		// make this two session share same ID
		sid := sess1.ID
		sess2.ID = sess1.ID

		// verify this session not in storage
		if esess, err := storage.Get(ctx, sid); err != nil || esess != nil {
			t.Errorf("storage.Get return error or there's already existing session: %v", err)
			break
		}

		// insert it
		if err := storage.Insert(ctx, sess1); err != nil {
			t.Errorf("failed to insert a session")
			break
		}

		if esess2, err := storage.Get(ctx, sid); err != nil || esess2 == nil {
			t.Errorf("Storage.Get should return existing session: %v", err)
			break
		}

		// then insert again, this should return error
		err := storage.Insert(ctx, sess2)
		if err == nil {
			t.Error("Storage.Insert should return error for existing session ID")
			break
		}
		verr, ok := err.(driver.SessionAlreadyExists)
		if !ok {
			t.Errorf("storage.Insert should return SessionAlreadyExists in existing session: %v", err)
			break
		}

		if verr.ID != sid {
			t.Errorf("SessionAlreadyExists returned should contains conflicted session ID")
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
