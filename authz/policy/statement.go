package policy

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/thatique/awan/authz/authorizer"
	"github.com/thatique/awan/authz/policy/condition"
)

// Statement contains information about a single permission
type Statement struct {
	SID        string              `json:"SID,omitempty"`
	Principal  Principal           `json:"Principal"`
	Effect     Effect              `json:"Effect"`
	Actions    ActionSet           `json:"Action"`
	Resources  ResourceSet         `json:"Resource,omitempty"`
	Conditions condition.Functions `json:"Condition,omitempty"`
}

// IsAllowed check if this statement allowed
func (statement Statement) IsAllowed(args authorizer.Args) bool {
	check := func() bool {
		if !statement.Principal.Match(args.User.GetUsername()) {
			return false
		}
		if !statement.Actions.Match(args.Action) {
			return false
		}

		resource := args.Resource
		if args.Object != "" {
			if !strings.HasPrefix(args.Resource, "/") {
				resource += "/"
			}

			resource += args.Object
		}

		if !statement.Resources.Match(resource, args.ConditionValues) {
			return false
		}

		return statement.Conditions.Evaluate(args.ConditionValues)
	}

	return statement.Effect.IsAllowed(check())
}

// IsValid - checks whether statement is valid or not.
func (statement Statement) IsValid() error {
	if !statement.Effect.IsValid() {
		return fmt.Errorf("invalid Effect %v", statement.Effect)
	}

	if len(statement.Actions) == 0 {
		return fmt.Errorf("Action must not be empty")
	}

	if len(statement.Resources) == 0 {
		return fmt.Errorf("Resource must not be empty")
	}

	return nil
}

// MarshalJSON - encodes JSON data to Statement.
func (statement Statement) MarshalJSON() ([]byte, error) {
	if err := statement.IsValid(); err != nil {
		return nil, err
	}

	// subtype to avoid recursive call to MarshalJSON()
	type subStatement Statement
	ss := subStatement(statement)
	return json.Marshal(ss)
}

// UnmarshalJSON - decodes JSON data to Statement.
func (statement *Statement) UnmarshalJSON(data []byte) error {
	// subtype to avoid recursive call to UnmarshalJSON()
	type subStatement Statement
	var ss subStatement

	if err := json.Unmarshal(data, &ss); err != nil {
		return err
	}

	s := Statement(ss)
	if err := s.IsValid(); err != nil {
		return err
	}

	*statement = s

	return nil
}

// NewStatement - creates new statement.
// NewStatement - creates new statement.
func NewStatement(effect Effect, principal Principal, actionSet ActionSet, resourceSet ResourceSet, conditions condition.Functions) Statement {
	return Statement{
		Effect:     effect,
		Principal:  principal,
		Actions:    actionSet,
		Resources:  resourceSet,
		Conditions: conditions,
	}
}
