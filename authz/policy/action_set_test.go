package policy

import "testing"

func TestActionSetMatch(t *testing.T) {
	actions := NewActionSet("GetBucket", "PutBucket")
	if !actions.Match("GetBucket") {
		t.Error("matching action should match")
	}
	if actions.Match("DeleteBucket") {
		t.Error("no existent actions should not match")
	}
}
