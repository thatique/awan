package driver

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
)

// Serializer provide a way to serialize and deserialize session's values into
// bytes[]
type Serializer interface {
	// Serialize session's values to []bytes
	Serialize(s *Session) ([]byte, error)

	// Deserialize []bytes to session's value
	Deserialize(b []byte, s *Session) error
}

// JSONSerializer serialize session values into json
var JSONSerializer Serializer = jsonSerializer{}

type jsonSerializer struct{}

func (j jsonSerializer) Serialize(s *Session) ([]byte, error) {
	m := make(map[string]interface{}, len(s.Values))
	for k, v := range s.Values {
		ks, ok := k.(string)
		if !ok {
			return nil, fmt.Errorf("awan.session Non-string key value, can't serialize session to JSON: %v", k)
		}
		m[ks] = v
	}

	return json.Marshal(m)
}

func (j jsonSerializer) Deserialize(b []byte, s *Session) error {
	m := make(map[string]interface{})
	err := json.Unmarshal(b, &m)

	if err != nil {
		return fmt.Errorf("awan.session JSONSerializer Deserialize operation Error: %v", err)
	}

	if s.Values == nil {
		s.Values = make(map[interface{}]interface{})
	}
	for k, v := range m {
		s.Values[k] = v
	}
	return nil
}

// GobSerializer serialize session values using gob encoding
var GobSerializer Serializer = gobSerializer{}

type gobSerializer struct{}

func (g gobSerializer) Serialize(ss *Session) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(ss.Values)
	if err == nil {
		return buf.Bytes(), nil
	}
	return nil, err
}

func (g gobSerializer) Deserialize(b []byte, ss *Session) error {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	return dec.Decode(&ss.Values)
}
