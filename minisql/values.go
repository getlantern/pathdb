package minisql

import (
	"fmt"
	"reflect"
)

const (
	ValueTypeBytes  = 0
	ValueTypeString = 1
	ValueTypeInt    = 2
)

type Value struct {
	Type   int
	String *string
	Int    *int
	Bytes  *[]byte
}

func NewValue(i interface{}) *Value {
	switch v := i.(type) {
	case []byte:
		return &Value{Type: ValueTypeBytes, Bytes: &v}
	case string:
		return &Value{Type: ValueTypeString, String: &v}
	case int:
		return &Value{Type: ValueTypeInt, Int: &v}
	}
	return nil
}

func valueFromPointer(i interface{}) *Value {
	switch t := i.(type) {
	case *[]byte:
		return &Value{Type: ValueTypeBytes, Bytes: t}
	case *string:
		return &Value{Type: ValueTypeString, String: t}
	case *int:
		return &Value{Type: ValueTypeInt, Int: t}
	default:
		panic(fmt.Errorf("type can't be used to initialize value from pointer: %v", reflect.TypeOf(i)))
	}
}

func (a *Value) value() interface{} {
	switch a.Type {
	case ValueTypeBytes:
		return *a.Bytes
	case ValueTypeString:
		return *a.String
	case ValueTypeInt:
		return *a.Int
	default:
		return nil
	}
}

func (a *Value) pointerValue() interface{} {
	switch a.Type {
	case ValueTypeBytes:
		return a.Bytes
	case ValueTypeString:
		return a.String
	case ValueTypeInt:
		return a.Int
	default:
		return nil
	}
}

type Values interface {
	Len() int
	Get(index int) *Value
	Set(index int, value *Value)
}

func NewValues(vals []interface{}) Values {
	values := make(valueArray, len(vals))
	for i, v := range vals {
		values[i] = NewValue(v)
	}
	return &valueArrayWrapper{values}
}

type valueArray []*Value

type valueArrayWrapper struct {
	values valueArray
}

func (vaw *valueArrayWrapper) Len() int {
	return len(vaw.values)
}

func (vaw *valueArrayWrapper) Get(index int) *Value {
	return vaw.values[index]
}

func (vaw *valueArrayWrapper) Set(index int, value *Value) {
	vaw.values[index] = value
}
