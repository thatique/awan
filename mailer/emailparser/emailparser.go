package emailparser

import (
	"errors"
	"fmt"
	"mime"
	"strings"
	"unicode/utf8"
)

type Email struct {
	local  string
	domain string
}

func NewEmail(s string) (*Email, error) {
	p := newEmailParser(s)
	return p.parse()
}

func IsValidEmail(s string) bool {
	_, err := NewEmail(s)
	return err == nil
}

func (e *Email) Local() string {
	return e.local
}

func (e *Email) Domain() string {
	return e.domain
}

func (e *Email) String() string {
	local := e.local
	quotelocal := false
	for i, r := range local {
		if isAtext(r, false, false) {
			continue
		}
		if r == '.' {
			// Dots are okay if they are surrounded by atext.
			// We only need to check that the previous byte is
			// not a dot, and this isn't the end of the string.
			if i > 0 && local[i-1] != '.' && i < len(local)-1 {
				continue
			}
		}
		quotelocal = true
		break
	}

	if quotelocal {
		local = quoteString(local)
	}

	return local + "@" + e.domain
}

type emailParser struct {
	s           string
	WordDecoder *mime.WordDecoder
}

func newEmailParser(s string) *emailParser {
	return &emailParser{
		s:           s,
		WordDecoder: new(mime.WordDecoder),
	}
}

func (p *emailParser) parse() (spec *Email, err error) {
	orig := *p
	defer func() {
		if err != nil {
			*p = orig
		}
	}()

	// local-part = dot-atom / quoted-string
	var localPart string
	p.skipSpace()
	if p.empty() {
		return nil, errors.New("mail: no addr-spec")
	}

	if p.peek() == '"' {
		// quoted-string
		localPart, err = p.consumeQuotedString()
		if localPart == "" {
			err = errors.New("mail: empty quoted string in addr-spec")
		}
	} else {
		// dot-atom
		localPart, err = p.consumeAtom(true, false)
	}

	if err != nil {
		return nil, err
	}

	if !p.consume('@') {
		return nil, errors.New("mail: missing @ in addr-spec")
	}

	// domain = dot-atom / domain-literal
	var domain string
	p.skipSpace()
	if p.empty() {
		return nil, errors.New("mail: no domain in addr-spec")
	}

	p2 := *p
	domain, err = p.consumeAtom(true, false)
	if err == nil {
		return &Email{local: localPart, domain: domain}, nil
	}

	var err2 error
	domain, err2 = p2.consumeDomainLiteral()
	if err2 != nil {
		return nil, err2
	}
	if err != nil {
		return nil, err
	}

	return &Email{local: localPart, domain: domain}, nil
}

// consumeAtom parses an RFC 5322 atom at the start of p.
// If dot is true, consumeAtom parses an RFC 5322 dot-atom instead.
// If permissive is true, consumeAtom will not fail on:
// - leading/trailing/double dots in the atom (see golang.org/issue/4938)
// - special characters (RFC 5322 3.2.3) except '<', '>', ':' and '"' (see golang.org/issue/21018)
func (p *emailParser) consumeAtom(dot bool, permissive bool) (atom string, err error) {
	i := 0

Loop:
	for {
		r, size := utf8.DecodeRuneInString(p.s[i:])
		switch {
		case size == 1 && r == utf8.RuneError:
			return "", fmt.Errorf("mail: invalid utf-8 in address: %q", p.s)

		case size == 0 || !isAtext(r, dot, permissive):
			break Loop

		default:
			i += size

		}
	}

	if i == 0 {
		return "", errors.New("mail: invalid string")
	}
	atom, p.s = p.s[:i], p.s[i:]
	if !permissive {
		if strings.HasPrefix(atom, ".") {
			return "", errors.New("mail: leading dot in atom")
		}
		if strings.Contains(atom, "..") {
			return "", errors.New("mail: double dot in atom")
		}
		if strings.HasSuffix(atom, ".") {
			return "", errors.New("mail: trailing dot in atom")
		}
	}
	return atom, nil
}

// consumeQuotedString parses the quoted string at the start of p.
func (p *emailParser) consumeQuotedString() (qs string, err error) {
	// Assume first byte is '"'.
	i := 1
	qsb := make([]rune, 0, 10)

	escaped := false

Loop:
	for {
		r, size := utf8.DecodeRuneInString(p.s[i:])

		switch {
		case size == 0:
			return "", errors.New("mail: unclosed quoted-string")

		case size == 1 && r == utf8.RuneError:
			return "", fmt.Errorf("mail: invalid utf-8 in quoted-string: %q", p.s)

		case escaped:
			//  quoted-pair = ("\" (VCHAR / WSP))

			if !isVchar(r) && !isWSP(r) {
				return "", fmt.Errorf("mail: bad character in quoted-string: %q", r)
			}

			qsb = append(qsb, r)
			escaped = false

		case isQtext(r) || isWSP(r):
			// qtext (printable US-ASCII excluding " and \), or
			// FWS (almost; we're ignoring CRLF)
			qsb = append(qsb, r)

		case r == '"':
			break Loop

		case r == '\\':
			escaped = true

		default:
			return "", fmt.Errorf("mail: bad character in quoted-string: %q", r)

		}

		i += size
	}
	p.s = p.s[i+1:]
	return string(qsb), nil
}

func (p *emailParser) consumeDomainLiteral() (dl string, err error) {
	p.skipCFWS()
	if p.empty() {
		return "", errors.New("mail: no addr-spec")
	}
	if !p.consume('[') {
		return "", errors.New("mail: missing [")
	}
	i := 0
	qdl := make([]rune, 0, 10)
Loop:
	for {
		r, size := utf8.DecodeRuneInString(p.s[i:])
		switch {
		case size == 0:
			break Loop

		case size == 1 && r == utf8.RuneError:
			return "", errors.New("invalid chars")

		case isWSP(r):
			i += size
			continue Loop

		case isDomainText(r):
			qdl = append(qdl, r)

		default:
			break Loop
		}
		i += size
	}

	p.s = p.s[i:]

	if !p.consume(']') {
		return "", errors.New("mail: missing ]")
	}

	p.skipCFWS()

	return "[" + string(qdl) + "]", nil
}

// skipCFWS skips CFWS as defined in RFC5322.
func (p *emailParser) skipCFWS() bool {
	p.skipSpace()

	for {
		if !p.consume('(') {
			break
		}

		if _, ok := p.consumeComment(); !ok {
			return false
		}

		p.skipSpace()
	}

	return true
}

func (p *emailParser) consumeComment() (string, bool) {
	// '(' already consumed.
	depth := 1

	var comment string
	for {
		if p.empty() || depth == 0 {
			break
		}

		if p.peek() == '\\' && p.len() > 1 {
			p.s = p.s[1:]
		} else if p.peek() == '(' {
			depth++
		} else if p.peek() == ')' {
			depth--
		}
		if depth > 0 {
			comment += p.s[:1]
		}
		p.s = p.s[1:]
	}

	return comment, depth == 0
}

func (p *emailParser) consume(c byte) bool {
	if p.empty() || p.peek() != c {
		return false
	}
	p.s = p.s[1:]
	return true
}

// skipSpace skips the leading space and tab characters.
func (p *emailParser) skipSpace() {
	p.s = strings.TrimLeft(p.s, " \t")
}

func (p *emailParser) peek() byte {
	return p.s[0]
}

func (p *emailParser) empty() bool {
	return p.len() == 0
}

func (p *emailParser) len() int {
	return len(p.s)
}

func quoteString(s string) string {
	var buf strings.Builder
	buf.WriteByte('"')
	for _, r := range s {
		if isQtext(r) || isWSP(r) {
			buf.WriteRune(r)
		} else if isVchar(r) {
			buf.WriteByte('\\')
			buf.WriteRune(r)
		}
	}
	buf.WriteByte('"')
	return buf.String()
}

func isDomainText(r rune) bool {
	return (r >= '!' && r <= 'Z') || (r >= '^' && r <= '~') || isObsNoWsCtl(r)
}

func isObsNoWsCtl(r rune) bool {
	return (r >= 1 && r <= 8) || (r >= 14 && r <= 31) || r == 11 || r == 12 || r == 127
}

// isAtext reports whether r is an RFC 5322 atext character.
// If dot is true, period is included.
// If permissive is true, RFC 5322 3.2.3 specials is included,
// except '<', '>', ':' and '"'.
func isAtext(r rune, dot, permissive bool) bool {
	switch r {
	case '.':
		return dot

	// RFC 5322 3.2.3. specials
	case '(', ')', '[', ']', ';', '@', '\\', ',':
		return permissive

	case '<', '>', '"', ':':
		return false
	}
	return isVchar(r)
}

// isQtext reports whether r is an RFC 5322 qtext character.
func isQtext(r rune) bool {
	// Printable US-ASCII, excluding backslash or quote.
	if r == '\\' || r == '"' {
		return false
	}
	return isVchar(r)
}

// isVchar reports whether r is an RFC 5322 VCHAR character.
func isVchar(r rune) bool {
	// Visible (printing) characters.
	return '!' <= r && r <= '~' || isMultibyte(r)
}

// isWSP reports whether r is a WSP (white space).
// WSP is a space or horizontal tab (RFC 5234 Appendix B).
func isWSP(r rune) bool {
	return r == ' ' || r == '\t'
}

// isMultibyte reports whether r is a multi-byte UTF-8 character
// as supported by RFC 6532
func isMultibyte(r rune) bool {
	return r >= utf8.RuneSelf
}