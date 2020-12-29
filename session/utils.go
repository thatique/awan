package session

import (
	"errors"
	"net/http"
)

// Default flashes key.
const flashesKey = "_flash"

type sessionContextKey struct{}

// GetSession get data associated for this request. Make sure call this function after
// `Middleware` run.
func GetSession(r *http.Request) (map[interface{}]interface{}, error) {
	var ctx = r.Context()
	data := ctx.Value(sessionContextKey{})
	if data != nil {
		return data.(map[interface{}]interface{}), nil
	}

	return nil, errors.New("sersan: no session data found in request, perhaps you didn't use Sersan's middleware?")
}

// AddFlash adds a flash message to the session.
func AddFlash(sess map[interface{}]interface{}, value interface{}, vars ...string) {
	key := flashesKey
	if len(vars) > 0 {
		key = vars[0]
	}
	var flashes []interface{}
	if v, ok := sess[key]; ok {
		flashes = v.([]interface{})
	}
	sess[key] = append(flashes, value)
}

// Flashes returns a slice of flash messages from the session.
func Flashes(sess map[interface{}]interface{}, vars ...string) []interface{} {
	var flashes []interface{}
	key := flashesKey
	if len(vars) > 0 {
		key = vars[0]
	}
	if v, ok := sess[key]; ok {
		// Drop the flashes and return it.
		delete(sess, key)
		flashes = v.([]interface{})
	}
	return flashes
}
