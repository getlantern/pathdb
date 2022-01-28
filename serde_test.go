package dbgo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSerdePrimitiveTypes(t *testing.T) {
	s := newSerde()
	testPrimitive(t, s, "", "empty string")
	testPrimitive(t, s, "bubba", "string")
	// testRoundTrip(t, s, []byte{0, 1, 2, 3}, "byte array")
	testPrimitive(t, s, byte(10), "byte")
	testPrimitive(t, s, false, "false")
	testPrimitive(t, s, true, "true")
	testPrimitive(t, s, int16(1), "positive int16")
	testPrimitive(t, s, int16(-1), "negative int16")
	testPrimitive(t, s, int32(1), "positive int32")
	testPrimitive(t, s, int32(-1), "negative int32")
	testPrimitive(t, s, int64(1), "positive int64")
	testPrimitive(t, s, int64(-1), "negative int64")
	testPrimitive(t, s, float32(1), "positive float32")
	testPrimitive(t, s, float32(-1), "negative float32")
	testPrimitive(t, s, float64(1), "positive float64")
	testPrimitive(t, s, float64(-1), "negative float64")
}

func testPrimitive(t *testing.T, s *serde, value interface{}, name string) {
	rt := roundTrip(t, s, value)
	require.Equal(t, value, rt, name)
}

func TestSerdeInt(t *testing.T) {
	rt := roundTrip(t, newSerde(), 1)
	require.Equal(t, int64(1), rt)
}

type JSONObject struct {
	A string
	B float64
}

func TestSerdePBUF(t *testing.T) {
	s := newSerde()
	o := &PBUFObject{
		A: "a",
		B: 5,
	}
	_, err := s.serialize(o)
	require.Equal(t, ErrUnregisteredProtobufType, err, "attempt to serialize unregistered type")
	s.register(1, &PBUFObject{})
	serialized, err := s.serialize(o)
	require.NoError(t, err)
	deserialized, err := s.deserialize(serialized)
	require.NoError(t, err)
	require.Equal(t, o.A, deserialized.(*PBUFObject).A)
	require.Equal(t, o.B, deserialized.(*PBUFObject).B)

	s2 := newSerde()
	_, err = s2.deserialize(serialized)
	require.Equal(t, ErrUnregisteredProtobufType, err, "attempt to deserialize unregistered type")
}

func TestSerdeJSON(t *testing.T) {
	s := newSerde()
	o := &JSONObject{
		A: "a",
		B: 5,
	}
	_, err := s.serialize(o)
	require.Equal(t, ErrUnregisteredJSONType, err, "attempt to serialize unregistered type")
	s.register(1, &JSONObject{})
	serialized, err := s.serialize(o)
	require.NoError(t, err)
	deserialized, err := s.deserialize(serialized)
	require.NoError(t, err)
	require.EqualValues(t, o, deserialized)

	s2 := newSerde()
	_, err = s2.deserialize(serialized)
	require.Equal(t, ErrUnregisteredJSONType, err, "attempt to deserialize unregistered type")
}

func roundTrip(t *testing.T, s *serde, value interface{}) interface{} {
	serialized, err := s.serialize(value)
	require.NoError(t, err)
	deserialized, err := s.deserialize(serialized)
	require.NoError(t, err)
	return deserialized
}
