package policy

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/minio/minio/pkg/wildcard"
	"github.com/thatique/awan/authz/policy/condition"
)

// Resource in policy statement
type Resource struct {
	ResourceName string
	Pattern      string
}

// IsResourcePattern check if the resource is resource pattern
func (r Resource) IsResourcePattern() bool {
	return !strings.Contains(r.Pattern, "/") || r.Pattern == "*"
}

// IsObjectPattern check if the policy is an object pattern
func (r Resource) IsObjectPattern() bool {
	return strings.Contains(r.Pattern, "/") || strings.Contains(r.ResourceName, "*") || r.Pattern == "*/*"
}

// IsValid - checks whether Resource is valid or not.
func (r Resource) IsValid() bool {
	return r.ResourceName != "" && r.Pattern != ""
}

func (r Resource) Match(resource string, conditionValues map[string][]string) bool {
	pattern := r.Pattern
	for _, key := range condition.CommonKeys {
		if rvalues, ok := conditionValues[key.Name()]; ok && rvalues[0] != "" {
			pattern = strings.Replace(pattern, key.VarName(), rvalues[0], -1)
		}
	}

	return wildcard.Match(pattern, resource)
}

func (r Resource) String() string {
	return r.Pattern
}

// UnmarshalJSON - decodes JSON data to Resource.
func (r *Resource) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parsedResource, err := parseResource(s)
	if err != nil {
		return err
	}

	*r = parsedResource

	return nil
}

// Validate - validates Resource is for given bucket or not.
func (r Resource) Validate(objectName string) error {
	if !r.IsValid() {
		return fmt.Errorf("invalid resource")
	}

	if !wildcard.Match(r.ResourceName, objectName) {
		return fmt.Errorf("bucket name does not match")
	}

	return nil
}

// MarshalJSON - encodes Resource to JSON data.
func (r Resource) MarshalJSON() ([]byte, error) {
	if !r.IsValid() {
		return nil, fmt.Errorf("invalid resource %v", r)
	}

	return json.Marshal(r.String())
}

func parseResource(s string) (Resource, error) {
	tokens := strings.SplitN(s, "/", 2)
	objectName := tokens[0]
	if objectName == "" {
		return Resource{}, fmt.Errorf("invalid resource format '%v'", s)
	}

	return Resource{
		ResourceName: objectName,
		Pattern:      s,
	}, nil
}

// NewResource - creates new resource.
func NewResource(objectName, keyName string) Resource {
	pattern := objectName
	if keyName != "" {
		if !strings.HasPrefix(keyName, "/") {
			pattern += "/"
		}

		pattern += keyName
	}

	return Resource{
		ResourceName: objectName,
		Pattern:      pattern,
	}
}
