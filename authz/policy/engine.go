package policy

import (
	"github.com/thatique/awan/auth/user"
	"github.com/thatique/awan/authz/authorizer"
)

// NewAuthorizer create new authorizer based on policy
func NewAuthorizer(lister Lister) authorizer.Authorizer {
	return &engine{lister: lister}
}

// Lister get policies for the given user
type Lister interface {
	GetPoliciesForUser(user user.Info) (policies []Policy, err error)
}

type engine struct {
	lister Lister
}

func (e *engine) Authorize(args authorizer.Args) (authorized authorizer.Decision, err error) {
	policies, err := e.lister.GetPoliciesForUser(args.User)
	if err != nil {
		return authorizer.DecisionDeny, err
	}

	// Deny by default
	if len(policies) == 0 {
		return authorizer.DecisionDeny, nil
	}

	for _, policy := range policies {
		if policy.IsAllowed(args) {
			return authorizer.DecisionAllow, nil
		}
	}

	return authorizer.DecisionDeny, nil
}
