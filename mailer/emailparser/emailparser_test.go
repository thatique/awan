package emailparser

import (
	"testing"
)

var tests = []struct {
	s     string
	valid bool
	email *Email
}{
	{
		s:     `luci@machine.example`,
		valid: true,
		email: &Email{
			local:  "luci",
			domain: "machine.example",
		},
	},
	{
		s:     `john.q.public@example.com`,
		valid: true,
		email: &Email{
			local:  `john.q.public`,
			domain: `example.com`,
		},
	},
	{
		s:     `nami@pub.example.com`,
		valid: true,
		email: &Email{
			local:  `nami`,
			domain: `pub.example.com`,
		},
	},
	{
		s:     `"my@strange@address"@example.com`,
		valid: true,
		email: &Email{
			local:  `my@idiot@address`,
			domain: `example.com`,
		},
	},
	{
		s:     `"first last"@example.com`,
		valid: true,
		email: &Email{
			local:  `first last`,
			domain: `example.com`,
		},
	},
}

func TestEmailParser(t *testing.T) {
	var (
		p   *emailParser
		err error
		em  *Email
	)
	for _, test := range tests {
		p = newEmailParser(test.s)
		em, err = p.parse()
		if err != nil && test.valid {
			t.Errorf("Failed to parse valid email address: %s, %v", test.s, err)
			continue
		}
		if em.String() != test.s {
			t.Errorf("Email.String() is not equal to %s", test.s)
		}
	}
}

func TestIsValidEmail(t *testing.T) {
	var result bool
	for _, test := range tests {
		result = IsValidEmail(test.s)
		if test.valid != result {
			t.Errorf(
				"Expected %s to be a valid email address: %t, but `IsValidEmail` return %t",
				test.s,
				test.valid,
				result,
			)
		}
	}
}
