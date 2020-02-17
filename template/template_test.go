package template

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

const want = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Awan</title>
	</head>
	<body>
		<div>Test</div><div>Get</div>
	</body>
</html>`

func TestTemplateShare(t *testing.T) {
	factory := NewFactory(&testFinder{name: "index.html"}, nil)
	factory.Share("Title", "Awan")
	tpl, err := factory.Make("index", "index.html")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, M{"Items": []string{"Test", "Get"}}); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != want {
		t.Fatalf("got %q; want %q", got, want)
	}
}

func TestTemplateWithContext(t *testing.T) {
	factory := NewFactory(&testFinder{name: "index.html"}, nil)
	tpl, err := factory.Make("index", "index.html")
	if err != nil {
		t.Fatal(err)
	}
	tpl.With("Title", "Awan")
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, M{"Items": []string{"Test", "Get"}}); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != want {
		t.Fatalf("got %q; want %q", got, want)
	}
}

func TestTemplateAllContext(t *testing.T) {
	factory := NewFactory(&testFinder{name: "index.html"}, nil)
	tpl, err := factory.Make("index", "index.html")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, M{"Title": "Awan", "Items": []string{"Test", "Get"}}); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != want {
		t.Fatalf("got %q; want %q", got, want)
	}
}

func TestTemplateContextOverwrite(t *testing.T) {
	factory := NewFactory(&testFinder{name: "index.html"}, nil)
	factory.Share("Title", "Awan Repository")
	tpl, err := factory.Make("index", "index.html")
	if err != nil {
		t.Fatal(err)
	}
	tpl.With("Title", "Awan Package")
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, M{"Title": "Awan", "Items": []string{"Test", "Get"}}); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != want {
		t.Fatalf("got %q; want %q", got, want)
	}
}

type testFinder struct {
	name string
}

func (tf *testFinder) Find(name string) (string, error) {
	if tf.name != name {
		return "", errors.New(fmt.Sprintf("can't find template %s", name))
	}
	const tpl = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>{{.Title}}</title>
	</head>
	<body>
		{{range .Items}}<div>{{ . }}</div>{{else}}<div><strong>no rows</strong></div>{{end}}
	</body>
</html>`
	return tpl, nil
}
