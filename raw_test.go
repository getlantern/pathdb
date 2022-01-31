package pathdb

func unloadedRaw[T any](serde *serde, value T) *Raw[T] {
	bytes, err := serde.serialize(value)
	if err != nil {
		panic(err)
	}
	return &Raw[T]{
		serde: serde,
		Bytes: bytes,
	}
}

func loadedRaw[T any](serde *serde, value T) *Raw[T] {
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
