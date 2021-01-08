package condition

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestKeyMarshalJSON(t *testing.T) {
	testCases := []struct {
		key            Key
		expectedResult []byte
		expectErr      bool
	}{
		{Key("aws:Referer"), []byte(`"aws:Referer"`), false},
		{Key(""), nil, true},
		{Key("aa\xe2"), nil, true},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.key)
		expectErr := (err != nil)

		if testCase.expectErr != expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: key: expected: %v, got: %v\n", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func TestKeyGetName(t *testing.T) {
	testCases := []struct {
		key            Key
		expectedResult string
	}{
		{Key("aws:Referer"), "Referer"},
		{Key("private-message"), "private-message"},
		{Key("aws:s3:BucketName"), "s3:BucketName"},
	}

	for i, testCase := range testCases {
		result := testCase.key.Name()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: keyname: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}
