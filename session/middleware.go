package session

import (
	"context"
	"net/http"

	"github.com/gorilla/securecookie"
	"github.com/thatique/awan/httputil"
	"github.com/thatique/awan/session/driver"
)

type sessionResponseWriter struct {
	http.ResponseWriter

	hasWritten bool
	data       map[interface{}]interface{}
	token      *SaveSessionToken
	ss         *ServerSessionState
}

func newSessionResponseWriter(w http.ResponseWriter, token *SaveSessionToken) *sessionResponseWriter {
	return &sessionResponseWriter{
		ResponseWriter: w,
		token:          token,
	}
}

// Middleware provides session to the wrapped http handler
func Middleware(ss *ServerSessionState, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sid := ""
		if c, err := r.Cookie(ss.cookieName); err == nil {
			err = securecookie.DecodeMulti(ss.cookieName, c.Value, &sid, ss.Codecs...)
			if err != nil {
				sid = ""
			}
		}

		data, token, err := ss.Load(r.Context(), sid)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		nw := newSessionResponseWriter(w, token)
		nw.data = data
		nw.ss = ss

		nr := r.WithContext(context.WithValue(r.Context(), sessionContextKey{}, data))

		next.ServeHTTP(nw, nr)
	})
}

func (w *sessionResponseWriter) WriteHeader(code int) {
	if !w.hasWritten {
		if err := w.saveSession(); err != nil {
			panic(err)
		}
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *sessionResponseWriter) Write(b []byte) (int, error) {
	if !w.hasWritten {
		if err := w.saveSession(); err != nil {
			return 0, err
		}
	}
	return w.ResponseWriter.Write(b)
}

func (w *sessionResponseWriter) saveSession() error {
	if w.hasWritten {
		panic("should not call saveSession twice")
	}

	w.hasWritten = true

	var (
		err  error
		sess *driver.Session
	)

	if sess, err = w.ss.Save(context.Background(), w.token, w.data); err != nil {
		return err
	}

	if sess == nil {
		http.SetCookie(w,
			httputil.NewCookieFromOptions(w.ss.cookieName, "", -1, w.ss.CookieOptions))
		return nil
	}

	encoded, err := securecookie.EncodeMulti(w.ss.cookieName, sess.ID,
		w.ss.Codecs...)
	if err != nil {
		return err
	}

	http.SetCookie(w,
		httputil.NewCookieFromOptions(w.ss.cookieName, encoded,
			sess.MaxAge(w.ss.IdleTimeout, w.ss.AbsoluteTimeout, w.token.now), w.ss.CookieOptions))
	return nil
}
