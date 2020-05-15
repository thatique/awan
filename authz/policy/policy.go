package policy

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/thatique/awan/authz/authorizer"
)

// Policy represents an access control policy which is used to either grant or deny a
// principal (users/groups/roles/etc) actions on specific resources
type Policy struct {
	ID         string      `json:"ID,omitempty"`
	Name       string      `json:"Name,omitempty"`
	Statements []Statement `json:"Statements"`
}

// IsAllowed evaluate policy statement for the give args
func (policy Policy) IsAllowed(args authorizer.Args) bool {
	// Check all deny statements. If any one statement denies, return false.
	for _, statement := range policy.Statements {
		if statement.Effect == Deny {
			if !statement.IsAllowed(args) {
				return false
			}
		}
	}

	// For owner, its allowed by default.
	if args.IsOwner {
		return true
	}

	// Check all allow statements. If any one statement allows, return true.
	for _, statement := range policy.Statements {
		if statement.Effect == Allow {
			if statement.IsAllowed(args) {
				return true
			}
		}
	}

	return false
}

// IsValid check if the policy is valid
func (policy Policy) IsValid() error {
	for _, statement := range policy.Statements {
		if err := statement.IsValid(); err != nil {
			return err
		}
	}

	for i := range policy.Statements {
		for _, statement := range policy.Statements[i+1:] {
			actions := policy.Statements[i].Actions.Intersection(statement.Actions)
			if len(actions) == 0 {
				continue
			}

			resources := policy.Statements[i].Resources.Intersection(statement.Resources)
			if len(resources) == 0 {
				continue
			}

			return fmt.Errorf("duplicate actions %v, resources %v found in statements %v, %v",
				actions, resources, policy.Statements[i], statement)
		}
	}

	return nil
}

// MarshalJSON - encodes Policy to JSON data.
func (policy Policy) MarshalJSON() ([]byte, error) {
	if err := policy.IsValid(); err != nil {
		return nil, err
	}

	// subtype to avoid recursive call to MarshalJSON()
	type subPolicy Policy
	return json.Marshal(subPolicy(policy))
}

// UnmarshalJSON - decodes JSON data to Iamp.
func (policy *Policy) UnmarshalJSON(data []byte) error {
	// subtype to avoid recursive call to UnmarshalJSON()
	type subPolicy Policy
	var sp subPolicy
	if err := json.Unmarshal(data, &sp); err != nil {
		return err
	}

	p := Policy(sp)
	if err := p.IsValid(); err != nil {
		return err
	}

	*policy = p

	return nil
}

// ParseConfig - parses data in given reader to Iamp.
func ParseConfig(reader io.Reader) (*Policy, error) {
	var policy Policy

	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil {
		return nil, err
	}

	return &policy, policy.IsValid()
}
