package minisql

import (
	"fmt"
	"reflect"
)

const (
	valueTypeBytes  = 0
	valueTypeString = 1
	valueTypeInt    = 2
	valueTypeBool   = 3
)

type Value struct {
	Type   int
	string *string
	int    *int
	bool   *bool
	bytes  *[]byte
}

func (v *Value) Bool() bool {
	if v.bool == nil {
		return false
	}
	return *v.bool
}

func (v *Value) SetBool(s bool) {
	v.Type = valueTypeBool
	*v.bool = s
}

func (v *Value) String() string {
	if v.string == nil {
		return ""
	}
	return *v.string
}

func (v *Value) SetString(s string) {
	v.Type = valueTypeString
	*v.string = s
}

func (v *Value) Int() int {
	if v.int == nil {
		return 0
	}
	return *v.int
}

func (v *Value) SetInt(i int) {
	v.Type = valueTypeInt
	*v.int = i
}

func (v *Value) Bytes() []byte {
	if v.bytes == nil {
		return nil
	}
	return *v.bytes
}

func (v *Value) SetBytes(i []byte) {
	v.Type = valueTypeBytes
	// Copy the bytes since the ones passed in are unsafe
	var b = make([]byte, len(i))
	copy(b, i)
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
	case bool:
		return NewValueBool(v)
	}
	return nil
}

func NewValueBytes(i []byte) *Value {
	// Copy the bytes since the ones passed in are unsafe
	var b = make([]byte, len(i))
	copy(b, i)
	return &Value{Type: valueTypeBytes, bytes: &b}
}

func NewValueString(i string) *Value {
	return &Value{Type: valueTypeString, string: &i}
}

func NewValueInt(i int) *Value {
	return &Value{Type: valueTypeInt, int: &i}
}
func NewValueBool(i bool) *Value {
	return &Value{Type: valueTypeBool, bool: &i}
}

func valueFromPointer(i interface{}) *Value {
	switch t := i.(type) {
	case *[]byte:
		return &Value{Type: valueTypeBytes, bytes: t}
	case *string:
		return &Value{Type: valueTypeString, string: t}
	case *int:
		return &Value{Type: valueTypeInt, int: t}
	case *bool:
		return &Value{Type: valueTypeBool, bool: t}
	default:
		panic(fmt.Errorf("type can't be used to initialize value from pointer: %v", reflect.TypeOf(i)))
	}
}

func (v *Value) value() interface{} {
	switch v.Type {
	case valueTypeBytes:
		return *v.bytes
	case valueTypeString:
		return *v.string
	case valueTypeInt:
		return *v.int
	case valueTypeBool:
		return *v.bool
	default:
		return nil
	}
}

func (v *Value) set(i interface{}) {
	if i != nil {
		switch v.Type {
		case valueTypeBytes:
			v.SetBytes(*i.(*[]byte))
		case valueTypeString:
			v.SetString(*i.(*string))
		case valueTypeInt:
			v.SetInt(*i.(*int))
		case valueTypeBool:
			v.SetBool(*i.(*bool))
		}
	}
}

func (v *Value) pointerToEmptyValue() interface{} {
	switch v.Type {
	case valueTypeBytes:
		return &[]byte{}
	case valueTypeString:
		str := ""
		return &str
	case valueTypeInt:
		i := 0
		return &i
	case valueTypeBool:
		i := false
		return &i
	default:
		return nil
	}
}

type Values interface {
	Len() int
	Get(index int) *Value
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
