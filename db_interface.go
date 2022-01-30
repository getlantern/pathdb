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
	var _result interface{}
	_result, err = q.get(path)
	if err != nil {
		return
	}
	if _result != nil {
		result = _result.(T)
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