package minisql

import (
	"fmt"
	"strconv"
)

const (
	ValueTypeBytes  = 0
	ValueTypeString = 1
	ValueTypeInt    = 2
)

type Value struct {
	Type   int
	String string
	Int    int
	Bytes  string
}

func NewValueBytes(val []byte) *Value {
	return &Value{Type: ValueTypeBytes, Bytes: string(val)}
}

func NewValueString(val string) *Value {
	return &Value{Type: ValueTypeString, String: val}
}

func NewValueInt(val int) *Value {
	return &Value{Type: ValueTypeInt, Int: val}
}

func NewValues(i []interface{}) Values {
	values := make(valueArray, len(i))
	for index, v := range i {
		switch t := v.(type) {
		case string:
			values[index] = NewValueString(t)
		case int:
			values[index] = NewValueInt(t)
		case []byte:
			values[index] = NewValueBytes(t)
		default:
			panic(fmt.Errorf("unsupported type provided: %T", t))
		}
	}
	return &valueArrayWrapper{values}
}

func (a *Value) valueAsString() string {
	switch a.Type {
	case ValueTypeBytes:
		return a.Bytes
	case ValueTypeString:
		return a.String
	case ValueTypeInt:
		return strconv.Itoa(a.Int)
	default:
		return ""
	}
}

type Values interface {
	Len() int
	Get(index int) *Value
	Set(index int, value *Value)
}

// Removed generic interface{} type for value array creation
// Instead provided concrete type creation methods like above

type valueArray []*Value

type valueArrayWrapper struct {
	values valueArray
}

func (vaw *valueArrayWrapper) Len() int {
	return len(vaw.values)
}

func (vaw *valueArrayWrapper) Get(index int) *Value {
	fmt.Sprintf("Go side Get Index")
	if index < 0 || index >= len(vaw.values) {
		return nil
	}
	return vaw.values[index]
}

func (vaw *valueArrayWrapper) Set(index int, value *Value) {
	fmt.Sprintf("Go side Set Index")
	if index < 0 || index >= len(vaw.values) {
		return
	}
	vaw.values[index] = value
}

// package minisql

// import (
// 	"fmt"
// 	"reflect"
// )

// const (
// 	ValueTypeBytes  = 0
// 	ValueTypeString = 1
// 	ValueTypeInt    = 2
// )

// type Value struct {
// 	Type   int
// 	String *string
// 	Int    *int
// 	Bytes  *[]byte
// }

// func NewValue(i interface{}) *Value {
// 	switch v := i.(type) {
// 	case []byte:
// 		return &Value{Type: ValueTypeBytes, Bytes: &v}
// 	case string:
// 		return &Value{Type: ValueTypeString, String: &v}
// 	case int:
// 		return &Value{Type: ValueTypeInt, Int: &v}
// 	}
// 	return nil
// }

// func valueFromPointer(i interface{}) *Value {
// 	switch t := i.(type) {
// 	case *[]byte:
// 		return &Value{Type: ValueTypeBytes, Bytes: t}
// 	case *string:
// 		return &Value{Type: ValueTypeString, String: t}
// 	case *int:
// 		return &Value{Type: ValueTypeInt, Int: t}
// 	default:
// 		panic(fmt.Errorf("type can't be used to initialize value from pointer: %v", reflect.TypeOf(i)))
// 	}
// }

// func (a *Value) value() interface{} {
// 	switch a.Type {
// 	case ValueTypeBytes:
// 		return *a.Bytes
// 	case ValueTypeString:
// 		return *a.String
// 	case ValueTypeInt:
// 		return *a.Int
// 	default:
// 		return nil
// 	}
// }

// func (a *Value) pointerValue() interface{} {
// 	switch a.Type {
// 	case ValueTypeBytes:
// 		return a.Bytes
// 	case ValueTypeString:
// 		return a.String
// 	case ValueTypeInt:
// 		return a.Int
// 	default:
// 		return nil
// 	}
// }

// type Values interface {
// 	Len() int
// 	Get(index int) *Value
// 	Set(index int, value *Value)
// }

// func NewValues(vals []interface{}) Values {
// 	values := make(valueArray, len(vals))
// 	for i, v := range vals {
// 		values[i] = NewValue(v)
// 	}
// 	return &valueArrayWrapper{values}
// }

// type valueArray []*Value

// type valueArrayWrapper struct {
// 	values valueArray
// }

// func (vaw *valueArrayWrapper) Len() int {
// 	return len(vaw.values)
// }

// func (vaw *valueArrayWrapper) Get(index int) *Value {
// 	return vaw.values[index]
// }

// func (vaw *valueArrayWrapper) Set(index int, value *Value) {
// 	vaw.values[index] = value
// }
