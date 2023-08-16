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
	string *string
	int    *int
	bytes  *[]byte
}

func (v *Value) String() string {
	if v.string == nil {
		return ""
	}
	return *v.string
}

func (v *Value) SetString(s string) {
	*v.string = s
}

func (v *Value) Int() int {
	if v.int == nil {
		return 0
	}
	return *v.int
}

func (v *Value) SetInt(i int) {
	*v.int = i
}

func (v *Value) Bytes() []byte {
	if v.bytes == nil {
		return nil
	}
	return *v.bytes
}
func (v *Value) SetBytes(b []byte) {
	*v.bytes = b
}

func NewValue(i interface{}) *Value {
	switch v := i.(type) {
	case []byte:
		return NewValueBytes(v)
	case string:
		return NewValueString(v)
	case int:
		return NewValueInt(v)
	}
	return nil
}

func NewValueBytes(i []byte) *Value {
	return &Value{Type: ValueTypeBytes, bytes: &i}
}

func NewValueString(i string) *Value {
	return &Value{Type: ValueTypeString, string: &i}
}

func NewValueInt(i int) *Value {
	return &Value{Type: ValueTypeInt, int: &i}
}

func valueFromPointer(i interface{}) *Value {
	switch t := i.(type) {
	case *[]byte:
		return &Value{Type: ValueTypeBytes, bytes: t}
	case *string:
		return &Value{Type: ValueTypeString, string: t}
	case *int:
		return &Value{Type: ValueTypeInt, int: t}
	default:
		panic(fmt.Errorf("type can't be used to initialize value from pointer: %v", reflect.TypeOf(i)))
	}
}

func (v *Value) value() interface{} {
	switch v.Type {
	case ValueTypeBytes:
		return *v.bytes
	case ValueTypeString:
		return *v.string
	case ValueTypeInt:
		return *v.int
	default:
		return nil
	}
}

func (v *Value) set(i interface{}) {
	if i != nil {
		switch v.Type {
		case ValueTypeBytes:
			v.SetBytes(*i.(*[]byte))
		case ValueTypeString:
			v.SetString(*i.(*string))
		case ValueTypeInt:
			v.SetInt(*i.(*int))
		}
	}
}

func (v *Value) pointerToEmptyValue() interface{} {
	switch v.Type {
	case ValueTypeBytes:
		return &[]byte{}
	case ValueTypeString:
		str := ""
		return &str
	case ValueTypeInt:
		i := 0
		return &i
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
