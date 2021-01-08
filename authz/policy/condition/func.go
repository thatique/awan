package condition

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Function - condition function interface.
type Function interface {
	// evaluate() - evaluates this condition function with given values.
	evaluate(values map[string][]string) bool

	// key() - returns condition key used in this function.
	key() Key

	// name() - returns condition name of this function.
	name() name

	// String() - returns string representation of function.
	String() string

	// toMap - returns map representation of this function.
	toMap() map[Key]ValueSet
}

// Functions - list of functions.
type Functions []Function

// Evaluate - evaluate list of functions, return true if all functions return
// true
func (fns Functions) Evaluate(values map[string][]string) bool {
	for _, fn := range fns {
		if !fn.evaluate(values) {
			return false
		}
	}

	return true
}

// Keys - returns list of keys used in all functions.
func (fns Functions) Keys() KeySet {
	keySet := NewKeySet()

	for _, fn := range fns {
		keySet.Add(fn.key())
	}

	return keySet
}

// MarshalJSON - encodes Functions to  JSON data.
func (fns Functions) MarshalJSON() ([]byte, error) {
	nm := make(map[name]map[Key]ValueSet)

	for _, fn := range fns {
		if _, ok := nm[fn.name()]; ok {
			for k, v := range fn.toMap() {
				nm[fn.name()][k] = v
			}
		} else {
			nm[fn.name()] = fn.toMap()
		}
	}

	return json.Marshal(nm)
}

func (fns Functions) String() string {
	funcStrings := []string{}
	for _, fn := range fns {
		s := fmt.Sprintf("%v", fn)
		funcStrings = append(funcStrings, s)
	}
	sort.Strings(funcStrings)

	return fmt.Sprintf("%v", funcStrings)
}

var conditionFuncMap = map[name]func(Key, ValueSet) (Function, error){
	stringEquals:              newStringEqualsFunc,
	stringNotEquals:           newStringNotEqualsFunc,
	stringEqualsIgnoreCase:    newStringEqualsIgnoreCaseFunc,
	stringNotEqualsIgnoreCase: newStringNotEqualsIgnoreCaseFunc,
	binaryEquals:              newBinaryEqualsFunc,
	stringLike:                newStringLikeFunc,
	stringNotLike:             newStringNotLikeFunc,
	ipAddress:                 newIPAddressFunc,
	notIPAddress:              newNotIPAddressFunc,
	null:                      newNullFunc,
	boolean:                   newBooleanFunc,
	numericEquals:             newNumericEqualsFunc,
	numericNotEquals:          newNumericNotEqualsFunc,
	numericLessThan:           newNumericLessThanFunc,
	numericLessThanEquals:     newNumericLessThanEqualsFunc,
	numericGreaterThan:        newNumericGreaterThanFunc,
	numericGreaterThanEquals:  newNumericGreaterThanEqualsFunc,
	dateEquals:                newDateEqualsFunc,
	dateNotEquals:             newDateNotEqualsFunc,
	dateLessThan:              newDateLessThanFunc,
	dateLessThanEquals:        newDateLessThanEqualsFunc,
	dateGreaterThan:           newDateGreaterThanFunc,
	dateGreaterThanEquals:     newDateGreaterThanEqualsFunc,
	// Add new conditions here.
}

// UnmarshalJSON - decodes JSON data to Functions.
func (functions *Functions) UnmarshalJSON(data []byte) error {
	// As string kind, int kind then json.Unmarshaler is checked at
	// https://github.com/golang/go/blob/master/src/encoding/json/decode.go#L618
	// UnmarshalJSON() is not called for types extending string
	// see https://play.golang.org/p/HrSsKksHvrS, better way to do is
	// https://play.golang.org/p/y9ElWpBgVAB
	//
	// Due to this issue, name and Key types cannot be used as map keys below.
	nm := make(map[string]map[string]ValueSet)
	if err := json.Unmarshal(data, &nm); err != nil {
		return err
	}

	if len(nm) == 0 {
		return fmt.Errorf("condition must not be empty")
	}

	funcs := []Function{}
	for nameString, args := range nm {
		n, err := parseName(nameString)
		if err != nil {
			return err
		}

		for keyString, values := range args {
			key, err := parseKey(keyString)
			if err != nil {
				return err
			}

			vfn, ok := conditionFuncMap[n]
			if !ok {
				return fmt.Errorf("condition %v is not handled", n)
			}

			f, err := vfn(key, values)
			if err != nil {
				return err
			}

			funcs = append(funcs, f)
		}
	}

	*functions = funcs

	return nil
}

// NewFunctions - returns new Functions with given function list.
func NewFunctions(functions ...Function) Functions {
	return Functions(functions)
}
