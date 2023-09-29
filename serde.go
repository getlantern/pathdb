package pathdb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"reflect"

	"google.golang.org/protobuf/proto"
)

const (
	TEXT           = 'T'
	BYTEARRAY      = 'A'
	BYTE           = '2'
	BOOLEAN        = 'B'
	SHORT          = 'S'
	INT            = 'I'
	LONG           = 'L'
	FLOAT          = 'F'
	DOUBLE         = 'D'
	PROTOCOLBUFFER = 'P'
	JSON           = 'J'
)

var (
	byteorder = binary.LittleEndian

	ErrUnregisteredProtobufType = errors.New("unregistered protocol buffer type")
	ErrUnregisteredJSONType     = errors.New("unregistered json type")
	ErrUnkownDataType           = errors.New("unknown data type")
)

type serde struct {
	registeredProtocolBufferTypes   map[reflect.Type]int16
	registeredProtocolBufferTypeIDs map[int16]reflect.Type
	registeredJSONTypes             map[reflect.Type]int16
	registeredJSONTypeIDs           map[int16]reflect.Type
}

func newSerde() *serde {
	return &serde{
		registeredProtocolBufferTypes:   make(map[reflect.Type]int16, 0),
		registeredProtocolBufferTypeIDs: make(map[int16]reflect.Type, 0),
		registeredJSONTypes:             make(map[reflect.Type]int16, 0),
		registeredJSONTypeIDs:           make(map[int16]reflect.Type, 0),
	}
}

func (s *serde) register(id int16, example interface{}) {
	t := reflect.TypeOf(example)
	_, isProtobuf := example.(proto.Message)
	if isProtobuf {
		s.registeredProtocolBufferTypes[t] = id
		s.registeredProtocolBufferTypeIDs[id] = t
	} else {
		s.registeredJSONTypes[t] = id
		s.registeredJSONTypeIDs[id] = t
	}
}

func (s *serde) serialize(data interface{}) (result []byte, err error) {
	switch v := data.(type) {
	case string:
		result = make([]byte, 1+len(v))
		result[0] = TEXT
		copy(result[1:], v)
	case []byte:
		result = make([]byte, 1+len(v))
		result[0] = BYTEARRAY
		copy(result[1:], v)
	case byte:
		result = []byte{BYTE, v}
	case bool:
		b := byte(0)
		if v {
			b = 1
		}
		result = []byte{BOOLEAN, b}
	case int16:
		result = make([]byte, 3)
		result[0] = SHORT
		byteorder.PutUint16(result[1:], uint16(v))
	case int32:
		result = make([]byte, 5)
		result[0] = INT
		byteorder.PutUint32(result[1:], uint32(v))
	case int:
		result = make([]byte, 9)
		result[0] = LONG
		byteorder.PutUint64(result[1:], uint64(v))
	case int64:
		result = make([]byte, 9)
		result[0] = LONG
		byteorder.PutUint64(result[1:], uint64(v))
	case float32:
		result = make([]byte, 5)
		result[0] = FLOAT
		bits := math.Float32bits(v)
		byteorder.PutUint32(result[1:], bits)
	case float64:
		result = make([]byte, 9)
		result[0] = DOUBLE
		bits := math.Float64bits(v)
		byteorder.PutUint64(result[1:], bits)
	case proto.Message:
		pbType, foundPBType := s.registeredProtocolBufferTypes[reflect.TypeOf(v)]
		if !foundPBType {
			err = ErrUnregisteredProtobufType
		} else {
			var b []byte
			b, err = proto.Marshal(v)
			if err == nil {
				result = make([]byte, 3+len(b))
				result[0] = PROTOCOLBUFFER
				byteorder.PutUint16(result[1:], uint16(pbType))
				copy(result[3:], b)
			}
		}
	default:
		jsonType, foundJSONType := s.registeredJSONTypes[reflect.TypeOf(v)]
		if !foundJSONType {
			err = ErrUnregisteredJSONType
		} else {
			var b []byte
			b, err = json.Marshal(v)
			if err == nil {
				result = make([]byte, 3+len(b))
				result[0] = JSON
				byteorder.PutUint16(result[1:], uint16(jsonType))
				copy(result[3:], b)
			}
		}
	}

	return
}

func (s *serde) deserialize(b []byte) (result interface{}, err error) {
	switch b[0] {
	case TEXT:
		result = string(b[1:])
	case BYTEARRAY:
		result = b[1:]
	case BYTE:
		result = b[1]
	case BOOLEAN:
		result = b[1] == 1
	case SHORT:
		result = int16(byteorder.Uint16(b[1:]))
	case INT:
		result = int32(byteorder.Uint32(b[1:]))
	case LONG:
		result = int64(byteorder.Uint64(b[1:]))
	case FLOAT:
		result = math.Float32frombits(byteorder.Uint32(b[1:]))
	case DOUBLE:
		result = math.Float64frombits(byteorder.Uint64(b[1:]))
	case PROTOCOLBUFFER:
		pbType, foundPBType := s.registeredProtocolBufferTypeIDs[int16(byteorder.Uint16(b[1:]))]
		if !foundPBType {
			err = ErrUnregisteredProtobufType
		} else {
			pb := reflect.New(pbType.Elem()).Interface().(proto.Message)
			err = proto.Unmarshal(b[3:], pb)
			if err == nil {
				result = pb
			}
		}
	case JSON:
		jsonType, foundJSONType := s.registeredJSONTypeIDs[int16(byteorder.Uint16(b[1:]))]
		if !foundJSONType {
			err = ErrUnregisteredJSONType
		} else {
			jo := reflect.New(jsonType.Elem()).Interface()
			err = json.Unmarshal(b[3:], jo)
			if err == nil {
				result = jo
			}
		}
	default:
		err = ErrUnkownDataType
	}

	return
}
