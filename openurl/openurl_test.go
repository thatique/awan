package openurl

import (
	"testing"
)

func TestSchemeMap(t *testing.T) {
	const foo, bar = "foo value", "bar value"

	tests := []struct {
		url     string
		wantErr bool
		want    interface{}
	}{
		{"invalid url", true, nil},
		{"foo://a/b/c", false, foo},
		{"api+foo://a/b/c", false, foo},
		{"api+type+foo://a/b/c", false, foo},
		{"bar://a?p=v", false, bar},
		{"api+bar://a", false, bar},
		{"api+type+bar://a", false, bar},
		{"typ+bar://a", true, nil},
		{"api+typ+bar://a", true, nil},
	}

	var emptyM, m SchemaMap
	m.Register("api", "Type", "foo", foo)
	m.Register("api", "Type", "bar", bar)

	for _, test := range tests {
		// Empty SchemeMap should always return an error.
		if _, _, err := emptyM.FromString("type", test.url); err == nil {
			t.Errorf("%s: empty SchemeMap got nil error, wanted non-nil error", test.url)
		}

		got, gotURL, gotErr := m.FromString("type", test.url)
		if (gotErr != nil) != test.wantErr {
			t.Errorf("%s: got error %v, want error: %v", test.url, gotErr, test.wantErr)
		}
		if gotErr != nil {
			continue
		}
		if got := gotURL.String(); got != test.url {
			t.Errorf("%s: got URL %q want %v", test.url, got, test.url)
		}
		if got != test.want {
			t.Errorf("%s: got %v want %v", test.url, got, test.want)
		}
	}

}
