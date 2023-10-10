package pathdb

type Raw[T any] struct {
	serde  *serde
	Bytes  []byte
	loaded bool
	value  T
	err    error
}

func (r *Raw[T]) Value() (T, error) {
	if !r.loaded {
		v, e := r.serde.deserialize(r.Bytes)
		r.err = e
		if e == nil {
			r.value = v.(T)
		}
		r.loaded = true
	}
	return r.value, r.err
}

func (r *Raw[T]) ValueOrProtoBytes() (interface{}, error) {
	if r.serde.isProtocolBuffer(r.Bytes) {
		return r.serde.stripHeader(r.Bytes), nil
	}
	return r.Value()
}
