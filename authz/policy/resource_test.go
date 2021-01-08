package policy

import "testing"

func TestResourceIsBucketPattern(t *testing.T) {
	testCases := []struct {
		resource       Resource
		expectedResult bool
	}{
		{NewResource("*", ""), true},
		{NewResource("mybucket", ""), true},
		{NewResource("mybucket*", ""), true},
		{NewResource("mybucket?0", ""), true},
		{NewResource("", "*"), false},
		{NewResource("*", "*"), false},
		{NewResource("mybucket", "*"), false},
		{NewResource("mybucket*", "/myobject"), false},
		{NewResource("mybucket?0", "/2010/photos/*"), false},
	}

	for i, testCase := range testCases {
		result := testCase.resource.IsResourcePattern()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestResourceIsObjectPattern(t *testing.T) {
	testCases := []struct {
		resource       Resource
		expectedResult bool
	}{
		{NewResource("*", ""), true},
		{NewResource("mybucket*", ""), true},
		{NewResource("", "*"), true},
		{NewResource("*", "*"), true},
		{NewResource("mybucket", "*"), true},
		{NewResource("mybucket*", "/myobject"), true},
		{NewResource("mybucket?0", "/2010/photos/*"), true},
		{NewResource("mybucket", ""), false},
		{NewResource("mybucket?0", ""), false},
	}

	for i, testCase := range testCases {
		result := testCase.resource.IsObjectPattern()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestResourceIsValid(t *testing.T) {
	testCases := []struct {
		resource       Resource
		expectedResult bool
	}{
		{NewResource("*", ""), true},
		{NewResource("mybucket*", ""), true},
		{NewResource("*", "*"), true},
		{NewResource("mybucket", "*"), true},
		{NewResource("mybucket*", "/myobject"), true},
		{NewResource("mybucket?0", "/2010/photos/*"), true},
		{NewResource("mybucket", ""), true},
		{NewResource("mybucket?0", ""), true},
		{NewResource("", ""), false},
		{NewResource("", "*"), false},
	}

	for i, testCase := range testCases {
		result := testCase.resource.IsValid()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}
