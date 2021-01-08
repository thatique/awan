package authorizer

import (
	"github.com/thatique/awan/auth/user"
)

// Action being performed
type Action string

// Args is argument to be passed to Authorized
type Args struct {
	// the user
	User user.Info
	// Action to performed
	Action Action
	// The resources being acted upon
	Resource string
	// The object
	Object string

	// is the current user owner of this resource
	IsOwner bool

	// the condition values
	ConditionValues map[string][]string
}

// Authorizer authorize based on Args
type Authorizer interface {
	Authorize(args Args) (authorized Decision, err error)
}

// Func function to implement Auhorizer interface
type Func func(args Args) (Decision, error)

// Authorize implments Authorizer interface
func (f Func) Authorize(args Args) (authorized Decision, err error) {
	return f(args)
}

// Decision returned by an authorizer
type Decision int32

const (
	// DecisionDeny Deny the request
	DecisionDeny Decision = iota
	// DecisionAllow Allow the request
	DecisionAllow
)
