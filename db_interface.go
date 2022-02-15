package pathdb

import (
	"errors"

	"github.com/mattn/go-sqlite3"
)

type Item[T any] struct {
	Path       string
	DetailPath string
	Value      T
}

type SearchResult[T any] struct {
	Item[T]
	Snippet string
}

func Mutate(d DB, fn func(TX) error) error {
	t, err := d.begin()
	if err != nil {
		return err
	}

	err = fn(t)
	if err == nil {
		return t.commit()
	} else {
		rollbackErr := t.rollback()
		if rollbackErr != nil {
			return rollbackErr
		}
	}

	return err
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
	return t.put(path, value, nil, fullText, true)
}

func PutRaw[T any](t TX, path string, value *Raw[T], fullText string) error {
	return t.put(path, nil, value.Bytes, fullText, true)
}

func PutIfAbsent[T any](t TX, path string, value T, fullText string) (bool, error) {
	err := t.put(path, value, nil, fullText, false)
	if err != nil {
		sqlErr, ok := err.(sqlite3.Error)
		if ok && errors.Is(sqlErr.Code, sqlite3.ErrConstraint) {
			// this means there was already a value at that path
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func GetOrPut[T any](t TX, path string, value T, fullText string) (result T, err error) {
	var b []byte
	b, err = t.get(path)
	if err != nil {
		return
	}
	if b != nil {
		var _result interface{}
		_result, err = t.getSerde().deserialize(b)
		if err == nil {
			result = _result.(T)
		}
		return
	}
	result = value
	err = Put(t, path, value, fullText)
	return
}

func Delete(t TX, path string) error {
	return t.delete(path)
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

func List[T any](q Queryable, query *QueryParams) (result []*Item[T], err error) {
	serde := q.getSerde()
	return doSearch(q, query, nil, func(i *item) (*Item[T], error) {
		return newItem[T](serde, i)
	})
}

func RList[T any](q Queryable, query *QueryParams) (result []*Item[*Raw[T]], err error) {
	serde := q.getSerde()
	return doSearch(q, query, nil, func(i *item) (*Item[*Raw[T]], error) {
		return newRawItem[T](serde, i)
	})
}

func ListPaths(q Queryable, query *QueryParams) (result []string, err error) {
	return doSearch(q, query, nil, func(i *item) (string, error) {
		return i.path, nil
	})
}

func Search[T any](q Queryable, query *QueryParams, search *SearchParams) (result []*SearchResult[T], err error) {
	serde := q.getSerde()
	return doSearch(q, query, search, func(i *item) (*SearchResult[T], error) {
		item, err := newItem[T](serde, i)
		if err != nil {
			return nil, err
		}
		return &SearchResult[T]{
			Item:    *item,
			Snippet: i.snippet,
		}, nil
	})
}

func RSearch[T any](q Queryable, query *QueryParams, search *SearchParams) (result []*SearchResult[*Raw[T]], err error) {
	serde := q.getSerde()
	return doSearch(q, query, search, func(i *item) (*SearchResult[*Raw[T]], error) {
		item, err := newRawItem[T](serde, i)
		if err != nil {
			return nil, err
		}
		return &SearchResult[*Raw[T]]{
			Item:    *item,
			Snippet: i.snippet,
		}, nil
	})
}

func doSearch[I any](q Queryable, query *QueryParams, search *SearchParams, buildItem func(*item) (I, error)) (items []I, err error) {
	var _items []*item
	_items, err = q.list(query, search)
	if err != nil {
		return
	}

	items = make([]I, 0, len(_items))
	for _, i := range _items {
		var item I
		item, err = buildItem(i)
		if err != nil {
			return
		}
		items = append(items, item)
	}
	return
}

func newItem[T any](s *serde, i *item) (*Item[T], error) {
	_value, err := s.deserialize(i.value)
	if err != nil {
		return nil, err
	}
	return &Item[T]{
		Path:       i.path,
		DetailPath: i.detailPath,
		Value:      _value.(T),
	}, nil
}

func newRawItem[T any](s *serde, i *item) (*Item[*Raw[T]], error) {
	result := &Item[*Raw[T]]{
		Path:       i.path,
		DetailPath: i.detailPath,
	}
	if len(i.value) > 0 {
		result.Value = &Raw[T]{
			serde: s,
			Bytes: i.value,
		}
	}
	return result, nil
}
