package bearertoken

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/thatique/awan/auth/authenticator"
	"github.com/thatique/awan/auth/user"
)

func TestAuthenticateRequest(t *testing.T) {
	auth := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
		if token != "token" {
			t.Errorf("unexpected token: %s", token)
		}
		return &authenticator.Response{User: &user.DefaultInfo{Name: "user"}}, true, nil
	}))
	resp, ok, err := auth.AuthenticateRequest(&http.Request{
		Header: http.Header{"Authorization": []string{"Bearer token"}},
	})
	if !ok || resp == nil || err != nil {
		t.Errorf("expected valid user")
	}
}

func TestAuthenticateRequestTokenInvalid(t *testing.T) {
	auth := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
		return nil, false, nil
	}))
	resp, ok, err := auth.AuthenticateRequest(&http.Request{
		Header: http.Header{"Authorization": []string{"Bearer token"}},
	})
	if ok || resp != nil {
		t.Errorf("expected not authenticated user")
	}
	if err != InvalidToken {
		t.Errorf("expected invalidToken error, got %v", err)
	}
}

func TestAuthenticateRequestTokenInvalidCustomError(t *testing.T) {
	customError := errors.New("custom")
	auth := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
		return nil, false, customError
	}))
	resp, ok, err := auth.AuthenticateRequest(&http.Request{
		Header: http.Header{"Authorization": []string{"Bearer token"}},
	})
	if ok || resp != nil {
		t.Errorf("expected not authenticated user")
	}
	if err != customError {
		t.Errorf("expected custom error, got %v", err)
	}
}

func TestAuthenticateRequestBadValue(t *testing.T) {
	testCases := []struct {
		Req *http.Request
	}{
		{Req: &http.Request{}},
		{Req: &http.Request{Header: http.Header{"Authorization": []string{"Bearer"}}}},
		{Req: &http.Request{Header: http.Header{"Authorization": []string{"bear token"}}}},
		{Req: &http.Request{Header: http.Header{"Authorization": []string{"Bearer: token"}}}},
	}
	for i, testCase := range testCases {
		auth := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
			t.Errorf("authentication should not have been called")
			return nil, false, nil
		}))
		user, ok, err := auth.AuthenticateRequest(testCase.Req)
		if ok || user != nil || err != nil {
			t.Errorf("%d: expected not authenticated (no token)", i)
		}
	}
}

func TestBearerToken(t *testing.T) {
	tests := map[string]struct {
		AuthorizationHeaders []string
		TokenAuth            authenticator.Token

		ExpectedUserName             string
		ExpectedOK                   bool
		ExpectedErr                  bool
		ExpectedAuthorizationHeaders []string
	}{
		"no header": {
			AuthorizationHeaders:         nil,
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  false,
			ExpectedAuthorizationHeaders: nil,
		},
		"empty header": {
			AuthorizationHeaders:         []string{""},
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  false,
			ExpectedAuthorizationHeaders: []string{""},
		},
		"non-bearer header": {
			AuthorizationHeaders:         []string{"Basic 123"},
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  false,
			ExpectedAuthorizationHeaders: []string{"Basic 123"},
		},
		"empty bearer token": {
			AuthorizationHeaders:         []string{"Bearer "},
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  false,
			ExpectedAuthorizationHeaders: []string{"Bearer "},
		},
		"valid bearer token removing header": {
			AuthorizationHeaders: []string{"Bearer 123"},
			TokenAuth: authenticator.TokenFunc(func(ctx context.Context, t string) (*authenticator.Response, bool, error) {
				return &authenticator.Response{User: &user.DefaultInfo{Name: "myuser"}}, true, nil
			}),
			ExpectedUserName:             "myuser",
			ExpectedOK:                   true,
			ExpectedErr:                  false,
			ExpectedAuthorizationHeaders: nil,
		},
		"invalid bearer token": {
			AuthorizationHeaders:         []string{"Bearer 123"},
			TokenAuth:                    authenticator.TokenFunc(func(ctx context.Context, t string) (*authenticator.Response, bool, error) { return nil, false, nil }),
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  true,
			ExpectedAuthorizationHeaders: []string{"Bearer 123"},
		},
		"error bearer token": {
			AuthorizationHeaders: []string{"Bearer 123"},
			TokenAuth: authenticator.TokenFunc(func(ctx context.Context, t string) (*authenticator.Response, bool, error) {
				return nil, false, errors.New("error")
			}),
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  true,
			ExpectedAuthorizationHeaders: []string{"Bearer 123"},
		},
	}

	for k, tc := range tests {
		req, _ := http.NewRequest("GET", "/", nil)
		for _, h := range tc.AuthorizationHeaders {
			req.Header.Add("Authorization", h)
		}

		bearerAuth := New(tc.TokenAuth)
		resp, ok, err := bearerAuth.AuthenticateRequest(req)
		if tc.ExpectedErr != (err != nil) {
			t.Errorf("%s: Expected err=%v, got %v", k, tc.ExpectedErr, err)
			continue
		}
		if ok != tc.ExpectedOK {
			t.Errorf("%s: Expected ok=%v, got %v", k, tc.ExpectedOK, ok)
			continue
		}
		if ok && resp.User.GetUsername() != tc.ExpectedUserName {
			t.Errorf("%s: Expected username=%v, got %v", k, tc.ExpectedUserName, resp.User.GetUsername())
			continue
		}
		if !reflect.DeepEqual(req.Header["Authorization"], tc.ExpectedAuthorizationHeaders) {
			t.Errorf("%s: Expected headers=%#v, got %#v", k, tc.ExpectedAuthorizationHeaders, req.Header["Authorization"])
			continue
		}
	}
}
