package policy

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestResourceSetMatch(t *testing.T) {
	testCases := []struct {
		resourceSet    ResourceSet
		resource       string
		expectedResult bool
	}{
		{NewResourceSet(NewResource("*", "")), "mybucket", true},
		{NewResourceSet(NewResource("*", "")), "mybucket/myobject", true},
		{NewResourceSet(NewResource("mybucket*", "")), "mybucket", true},
		{NewResourceSet(NewResource("mybucket*", "")), "mybucket/myobject", true},
		{NewResourceSet(NewResource("", "*")), "/myobject", true},
		{NewResourceSet(NewResource("*", "*")), "mybucket/myobject", true},
		{NewResourceSet(NewResource("mybucket", "*")), "mybucket/myobject", true},
		{NewResourceSet(NewResource("mybucket*", "/myobject")), "mybucket/myobject", true},
		{NewResourceSet(NewResource("mybucket*", "/myobject")), "mybucket100/myobject", true},
		{NewResourceSet(NewResource("mybucket?0", "/2010/photos/*")), "mybucket20/2010/photos/1.jpg", true},
		{NewResourceSet(NewResource("mybucket", "")), "mybucket", true},
		{NewResourceSet(NewResource("mybucket?0", "")), "mybucket30", true},
		{NewResourceSet(NewResource("mybucket?0", "/2010/photos/*"),
			NewResource("mybucket", "/2010/photos/*")), "mybucket/2010/photos/1.jpg", true},
		{NewResourceSet(NewResource("", "*")), "mybucket/myobject", false},
		{NewResourceSet(NewResource("*", "*")), "mybucket", false},
		{NewResourceSet(NewResource("mybucket", "*")), "mybucket10/myobject", false},
		{NewResourceSet(NewResource("mybucket", "")), "mybucket/myobject", false},
		{NewResourceSet(), "mybucket/myobject", false},
	}

	for i, testCase := range testCases {
		result := testCase.resourceSet.Match(testCase.resource, nil)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestResourceSetUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult ResourceSet
		expectErr      bool
	}{
		{[]byte(`"mybucket/myobject*"`),
			NewResourceSet(NewResource("mybucket", "/myobject*")), false},
		{[]byte(`"mybucket/photos/myobject*"`),
			NewResourceSet(NewResource("mybucket", "/photos/myobject*")), false},
		{[]byte(`"mybucket"`), NewResourceSet(NewResource("mybucket", "")), false},
		{[]byte(`""`), nil, true},
	}

	for i, testCase := range testCases {
		var result ResourceSet
		err := json.Unmarshal(testCase.data, &result)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}
