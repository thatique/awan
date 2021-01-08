package policy

import (
	"net"
	"testing"

	"github.com/thatique/awan/auth/user"
	"github.com/thatique/awan/authz/authorizer"
	"github.com/thatique/awan/authz/policy/condition"
)

func TestStatementIsAllowed(t *testing.T) {
	case1Statement := NewStatement(
		Allow,
		NewPrincipal("*"),
		NewActionSet("GetBucketLocationAction", "PutObjectAction"),
		NewResourceSet(NewResource("*", "")),
		condition.NewFunctions(),
	)

	case2Statement := NewStatement(
		Allow,
		NewPrincipal("*"),
		NewActionSet("GetObjectAction", "PutObjectAction"),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(),
	)

	_, IPNet1, err := net.ParseCIDR("192.168.1.0/24")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	func1, err := condition.NewIPAddressFunc(
		condition.AWSSourceIP,
		IPNet1,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Statement := NewStatement(
		Allow,
		NewPrincipal("*"),
		NewActionSet("GetObjectAction", "PutObjectAction"),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(func1),
	)

	case4Statement := NewStatement(
		Deny,
		NewPrincipal("*"),
		NewActionSet("GetObjectAction", "PutObjectAction"),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(func1),
	)

	anonGetBucketLocationArgs := authorizer.Args{
		User: &user.DefaultInfo{
			Name: "Q3AM3UQ867SPQQA43P2F",
		},
		Action:          "GetBucketLocationAction",
		Resource:        "mybucket",
		ConditionValues: map[string][]string{},
	}

	getBucketLocationArgs := authorizer.Args{
		User: &user.DefaultInfo{
			Name: "Q3AM3UQ867SPQQA43P2F",
		},
		Action:          "GetBucketLocationAction",
		Resource:        "mybucket",
		ConditionValues: map[string][]string{},
		IsOwner:         true,
	}

	anonPutObjectActionArgs := authorizer.Args{
		User: &user.DefaultInfo{
			Name: "Q3AM3UQ867SPQQA43P2F",
		},
		Action: "PutObjectAction",
		ConditionValues: map[string][]string{
			"x-amz-copy-source": {"mybucket/myobject"},
			"SourceIp":          {"192.168.1.10"},
		},
		Resource: "mybucket",
		Object:   "myobject",
	}

	putObjectActionArgs := authorizer.Args{
		User: &user.DefaultInfo{
			Name: "Q3AM3UQ867SPQQA43P2F",
		},
		Action:   "PutObjectAction",
		Resource: "mybucket",
		ConditionValues: map[string][]string{
			"x-amz-copy-source": {"mybucket/myobject"},
			"SourceIp":          {"192.168.1.10"},
		},
		IsOwner: true,
		Object:  "myobject",
	}

	getObjectActionArgs := authorizer.Args{
		User: &user.DefaultInfo{
			Name: "Q3AM3UQ867SPQQA43P2F",
		},
		Action:          "GetObjectAction",
		Resource:        "mybucket",
		ConditionValues: map[string][]string{},
		IsOwner:         true,
		Object:          "myobject",
	}

	anonGetObjectActionArgs := authorizer.Args{
		User: &user.DefaultInfo{
			Name: "Q3AM3UQ867SPQQA43P2F",
		},
		Action:          "GetObjectAction",
		ConditionValues: map[string][]string{},
		Resource:        "mybucket",
		Object:          "myobject",
	}

	testCases := []struct {
		statement      Statement
		args           authorizer.Args
		expectedResult bool
	}{
		{case1Statement, anonGetBucketLocationArgs, true},
		{case1Statement, anonPutObjectActionArgs, true},
		{case1Statement, anonGetObjectActionArgs, false},
		{case1Statement, getBucketLocationArgs, true},
		{case1Statement, putObjectActionArgs, true},
		{case1Statement, getObjectActionArgs, false},

		{case2Statement, anonGetBucketLocationArgs, false},
		{case2Statement, anonPutObjectActionArgs, true},
		{case2Statement, anonGetObjectActionArgs, true},
		{case2Statement, getBucketLocationArgs, false},
		{case2Statement, putObjectActionArgs, true},
		{case2Statement, getObjectActionArgs, true},

		{case3Statement, anonGetBucketLocationArgs, false},
		{case3Statement, anonPutObjectActionArgs, true},
		{case3Statement, anonGetObjectActionArgs, false},
		{case3Statement, getBucketLocationArgs, false},
		{case3Statement, putObjectActionArgs, true},
		{case3Statement, getObjectActionArgs, false},

		{case4Statement, anonGetBucketLocationArgs, true},
		{case4Statement, anonPutObjectActionArgs, false},
		{case4Statement, anonGetObjectActionArgs, true},
		{case4Statement, getBucketLocationArgs, true},
		{case4Statement, putObjectActionArgs, false},
		{case4Statement, getObjectActionArgs, true},
	}

	for i, testCase := range testCases {
		result := testCase.statement.IsAllowed(testCase.args)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}
