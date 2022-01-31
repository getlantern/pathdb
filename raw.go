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
