package pathdb

type PathAndValue[T any] struct {
	path  string
	value T
}

func Mutate(d DB, fn func(TX) error) error {
	t, err := d.begin()
	if err != nil {
		return err
	}

	err = fn(t)
	if err == nil {
		t.commit()
	} else {
		t.rollback()
	}

	return err
}

func Get[T any](q Queryable, path string) (result T, err error) {
	var _result *Raw[T]
	_result, err = RGet[T](q, path)
	if err != nil {
		return
	}
	if _result != nil {
		result, err = _result.Value()
	}
	return
}

func RGet[T any](q Queryable, path string) (result *Raw[T], err error) {
	var b []byte
	b, err = q.get(path)
	if err != nil {
		return
	}
	if len(b) > 0 {
		result = &Raw[T]{
			serde: q.getSerde(),
			Bytes: b,
		}
	}
	return
}

func List[T any](q Queryable, query *Query, search *Search) (result []*PathAndValue[T], err error) {
	var _result []*pathAndValue
	_result, err = q.list(query, search)
	if err != nil {
		return
	}

	serde := q.getSerde()
	result = make([]*PathAndValue[T], 0, len(_result))
	for _, pv := range _result {
		var _value interface{}
		_value, err = serde.deserialize(pv.value)
		if err != nil {
			return
		}
		result = append(result, &PathAndValue[T]{
			path:  pv.path,
			value: _value.(T),
		})
	}
	return
}

func RList[T any](q Queryable, query *Query, search *Search) (result []*PathAndValue[*Raw[T]], err error) {
	var _result []*pathAndValue
	_result, err = q.list(query, search)
	if err != nil {
		return
	}

	serde := q.getSerde()
	result = make([]*PathAndValue[*Raw[T]], 0, len(_result))
	for _, pv := range _result {
		if err != nil {
			return
		}
		result = append(result, &PathAndValue[*Raw[T]]{
			path: pv.path,
			value: &Raw[T]{
				serde: serde,
				Bytes: pv.value,
			},
		})
	}
	return
}

func PutAll[T any](t TX, values map[string]T) error {
	for path, value := range values {
		err := Put(t, path, value, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func Put[T any](t TX, path string, value T, fullText string) error {
	return t.put(path, value, fullText, true)
}

func PutIfAbsent[T any](t TX, path string, value T, fullText string) error {
	return t.put(path, value, fullText, false)
}

func GetOrPut[T any](t TX, path string, value T, fullText string) (result T, err error) {
	var _result interface{}
	_result, err = t.get(path)
	if err != nil {
		return
	}
	if _result != nil {
		result = _result.(T)
		return
	}
	result = value
	err = Put(t, path, value, fullText)
	return
}

func Delete(t TX, path string) error {
	return t.delete(path)
}
