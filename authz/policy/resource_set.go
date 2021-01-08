package policy

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/minio/minio-go/v7/pkg/set"
)

// ResourceSet set of resources in policy statement.
type ResourceSet map[Resource]struct{}

// Add a resource to ResourceSet
func (resourceSet ResourceSet) Add(resource Resource) {
	resourceSet[resource] = struct{}{}
}

// Match - matches object name with anyone of action pattern in action set.
func (resourceSet ResourceSet) Match(resource string, conditionValues map[string][]string) bool {
	for r := range resourceSet {
		if r.Match(resource, conditionValues) {
			return true
		}
	}

	return false
}

// Intersection - returns actions available in both ResourceSet.
func (resourceSet ResourceSet) Intersection(sset ResourceSet) ResourceSet {
	nset := NewResourceSet()
	for k := range resourceSet {
		if _, ok := sset[k]; ok {
			nset.Add(k)
		}
	}

	return nset
}

func (resourceSet ResourceSet) String() string {
	resources := []string{}
	for resource := range resourceSet {
		resources = append(resources, resource.String())
	}
	sort.Strings(resources)

	return fmt.Sprintf("%v", resources)
}

// MarshalJSON - encodes ResourceSet to JSON data.
func (resourceSet ResourceSet) MarshalJSON() ([]byte, error) {
	if len(resourceSet) == 0 {
		return nil, errors.New("empty resource set")
	}

	resources := []Resource{}
	for resource := range resourceSet {
		resources = append(resources, resource)
	}

	return json.Marshal(resources)
}

// UnmarshalJSON - decodes JSON data to ResourceSet.
func (resourceSet *ResourceSet) UnmarshalJSON(data []byte) error {
	var sset set.StringSet
	if err := json.Unmarshal(data, &sset); err != nil {
		return err
	}

	*resourceSet = make(ResourceSet)
	for _, s := range sset.ToSlice() {
		resource, err := parseResource(s)
		if err != nil {
			return err
		}

		if _, found := (*resourceSet)[resource]; found {
			return fmt.Errorf("duplicate resource '%v' found", s)
		}

		resourceSet.Add(resource)
	}

	return nil
}

// NewResourceSet - creates new action set.
func NewResourceSet(resources ...Resource) ResourceSet {
	resourceSet := make(ResourceSet)
	for _, resource := range resources {
		resourceSet.Add(resource)
	}

	return resourceSet
}
