package pathdb

func Mutate(d *db, fn func(TX) error) error {
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