package pathdb

func UnloadedRaw[T any](db DB, value T) *Raw[T] {
	serde := db.getSerde()
	bytes, err := serde.serialize(value)
	if err != nil {
		panic(err)
	}
	return &Raw[T]{
		serde: serde,
		Bytes: bytes,
	}
}

func LoadedRaw[T any](db DB, value T) *Raw[T] {
	serde := db.getSerde()
	bytes, err := serde.serialize(value)
	if err != nil {
		panic(err)
	}
	return &Raw[T]{
		serde:  serde,
		Bytes:  bytes,
		value:  value,
		loaded: true,
	}
}
