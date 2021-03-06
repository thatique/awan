package template

import (
	"html/template"
	"io"
	"io/ioutil"
	"path"
	"path/filepath"
)

// M is generic data to be passed template
type M map[string]interface{}

// Finder provides to get template string based their name
type Finder interface {
	// Find finds a template based the given name, returns template string
	// if success otherwise returns empty string and non-nil error
	Find(name string) (string, error)
}

// FileFinder implements Finder that search template in file system
type fileFinder struct {
	basePath string
}

// Get returns template string
func (f *fileFinder) Find(name string) (string, error) {
	assetPath := path.Join(f.basePath, filepath.FromSlash(path.Clean("/"+name)))
	b, err := ioutil.ReadFile(assetPath)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// AssetFinder implements Finder
type assetFinder struct {
	basePath string
	assets   func(string) ([]byte, error)
}

// Get returns template string
func (a *assetFinder) Find(name string) (string, error) {
	assetPath := path.Join(a.basePath, filepath.FromSlash(path.Clean("/"+name)))
	if len(assetPath) > 0 && assetPath[0] == '/' {
		assetPath = assetPath[1:]
	}
	var (
		b   []byte
		err error
	)
	if b, err = a.assets(assetPath); err != nil {
		return "", err
	}
	return string(b), nil
}

// Factory provides an easy way to create Template
type Factory struct {
	finder    Finder
	funcs     template.FuncMap
	templates map[string]*template.Template
	shared    M
}

// NewFileFactory returns new Factory backed with file finder
func NewFileFactory(basePath string, funcs template.FuncMap) *Factory {
	return NewFactory(&fileFinder{basePath: basePath}, funcs)
}

// NewAssetFactory returns new Factory backed with asset finder
func NewAssetFactory(basePath string, assets func(string) ([]byte, error), funcs template.FuncMap) *Factory {
	return NewFactory(&assetFinder{assets: assets, basePath: basePath}, funcs)
}

// NewFactory returns new Factory
func NewFactory(finder Finder, funcs template.FuncMap) *Factory {
	return &Factory{finder: finder, funcs: funcs, templates: make(map[string]*template.Template)}
}

// Make create Template
func (f *Factory) Make(name string, tpls ...string) (*Template, error) {
	if t, ok := f.templates[name]; ok {
		return f.createTemplate(t, name), nil
	}

	var (
		tpl *template.Template
		err error
	)

	tpl = template.New(name).Funcs(f.funcs)
	for _, tn := range tpls {
		if tpl, err = f.parse(tpl, tn); err != nil {
			return nil, err
		}
	}

	f.templates[name] = tpl

	return f.createTemplate(tpl, name), nil
}

// Share add a piece of shared data
func (f *Factory) Share(k string, v interface{}) {
	if f.shared == nil {
		f.shared = M{k: v}
	} else {
		f.shared[k] = v
	}
}

func (f *Factory) createTemplate(t *template.Template, name string) *Template {
	m := M{}
	if f.shared != nil {
		for k, v := range f.shared {
			m[k] = v
		}
	}

	return &Template{name: name, tpl: t, data: m}
}

func (f *Factory) parse(tpl *template.Template, name string) (*template.Template, error) {
	s, err := f.finder.Find(name)
	if err != nil {
		return nil, err
	}
	return tpl.Parse(s)
}

// Template provides a way to compose data
type Template struct {
	name string
	tpl  *template.Template
	data M // always non nil
}

// With Add a piece of data to the view.
func (t *Template) With(key string, value interface{}) {
	t.data[key] = value
}

// Execute applies a parsed template to the specified data object, writing the output to w
func (t *Template) Execute(w io.Writer, data M) error {
	final := M{}
	for k, v := range t.data {
		final[k] = v
	}
	if data != nil {
		for k, v := range data {
			final[k] = v
		}
	}
	return t.tpl.Execute(w, final)
}

// GetName returns the name of Template
func (t *Template) GetName() string {
	return t.name
}
