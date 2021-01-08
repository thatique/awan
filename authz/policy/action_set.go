package policy

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/pkg/wildcard"
	"github.com/thatique/awan/authz/authorizer"
)

// ActionSet - set of actions.
type ActionSet map[authorizer.Action]struct{}

// Add add an action to action set
func (actionSet ActionSet) Add(action authorizer.Action) {
	actionSet[action] = struct{}{}
}

// Match - matches object name with anyone of action pattern in action set.
func (actionSet ActionSet) Match(action authorizer.Action) bool {
	for r := range actionSet {
		if wildcard.Match(string(r), string(action)) {
			return true
		}
	}

	return false
}

// Intersection - returns actions available in both ActionSet.
func (actionSet ActionSet) Intersection(sset ActionSet) ActionSet {
	nset := NewActionSet()
	for k := range actionSet {
		if _, ok := sset[k]; ok {
			nset.Add(k)
		}
	}

	return nset
}

// MarshalJSON - encodes ActionSet to JSON data.
func (actionSet ActionSet) MarshalJSON() ([]byte, error) {
	if len(actionSet) == 0 {
		return nil, fmt.Errorf("empty action set")
	}

	return json.Marshal(actionSet.ToSlice())
}

func (actionSet ActionSet) String() string {
	actions := []string{}
	for action := range actionSet {
		actions = append(actions, string(action))
	}
	sort.Strings(actions)

	return fmt.Sprintf("%v", actions)
}

// ToSlice - returns slice of actions from the action set.
func (actionSet ActionSet) ToSlice() []authorizer.Action {
	actions := []authorizer.Action{}
	for action := range actionSet {
		actions = append(actions, action)
	}

	return actions
}

// UnmarshalJSON - decodes JSON data to ActionSet.
func (actionSet *ActionSet) UnmarshalJSON(data []byte) error {
	var sset set.StringSet
	if err := json.Unmarshal(data, &sset); err != nil {
		return err
	}

	if len(sset) == 0 {
		return fmt.Errorf("empty action set")
	}

	*actionSet = make(ActionSet)
	for _, s := range sset.ToSlice() {
		actionSet.Add(authorizer.Action(s))
	}

	return nil
}

// NewActionSet - creates new action set.
func NewActionSet(actions ...authorizer.Action) ActionSet {
	actionSet := make(ActionSet)
	for _, action := range actions {
		actionSet.Add(action)
	}

	return actionSet
}
