package pathdb

import (
	"fmt"
	"strings"
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
	t, err := d.Begin()
	if err != nil {
		return fmt.Errorf("mutate: begin transaction: %w", err)
	}

	err = fn(t)
	if err == nil {
		err = t.Commit()
		if err != nil {
			return fmt.Errorf("mutate: commit transaction: %w", err)
		}
		return nil
	} else {
		rollbackErr := t.Rollback()
		if rollbackErr != nil {
			return fmt.Errorf("mutate: rollback transaction: %w", rollbackErr)
		}
		return fmt.Errorf("mutate: fn: %w", err)
	}
}

func PutAll[T any](t TX, values map[string]T) error {
	for path, value := range values {
		err := Put(t, path, value, "")
		if err != nil {
			return fmt.Errorf("putall: put: %w", err)
		}
	}
	return nil
}

func Put[T any](t TX, path string, value T, fullText string) error {
	return t.Put(path, value, nil, fullText, true)
}

func PutRaw[T any](t TX, path string, value *Raw[T], fullText string) error {
	return t.Put(path, nil, value.Bytes, fullText, true)
}

func PutIfAbsent[T any](t TX, path string, value T, fullText string) (bool, error) {
	err := t.Put(path, value, nil, fullText, false)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			// this means there was already a value at that path
			return false, nil
		}
		return false, fmt.Errorf("putifabsent: put: %w", err)
	}
	return true, nil
}

func GetOrPut[T any](t TX, path string, value T, fullText string) (T, error) {
	var result T
	b, err := t.Get(path)
	if err != nil {
		return result, fmt.Errorf("getorput: get: %w", err)
	}
	if b != nil {
		var _result interface{}
		_result, err = t.getSerde().deserialize(b)
		if err != nil {
			return result, fmt.Errorf("getorput: deserialize: %w", err)
		}
		result = _result.(T)
		return result, nil
	}
	result = value
	err = Put(t, path, value, fullText)
	if err != nil {
		return result, fmt.Errorf("getorput: put: %w", err)
	}
	return result, nil
}

func Delete(t TX, path string) error {
	return t.Delete(path)
}

func Get[T any](q Queryable, path string) (T, error) {
	var result T
	var _result *Raw[T]
	_result, err := RGet[T](q, path)
	if err != nil {
		return result, fmt.Errorf("get: rget: %w", err)
	}
	if _result != nil {
		result, err = _result.Value()
		if err != nil {
			return result, fmt.Errorf("get: value: %w", err)
		}
	}
	return result, nil
}

func RGet[T any](q Queryable, path string) (*Raw[T], error) {
	var result *Raw[T]
	var b []byte
	b, err := q.Get(path)
	if err != nil {
		return result, fmt.Errorf("rget: get: %w", err)
	}
	if len(b) > 0 {
		result = &Raw[T]{
			serde: q.getSerde(),
			Bytes: b,
		}
	}
	return result, nil
}

func List[T any](q Queryable, query *QueryParams) ([]*Item[T], error) {
	serde := q.getSerde()
	result, err := doSearch(q, query, nil, func(i *item) (*Item[T], error) {
		item, err := newItem[T](serde, i)
		if err != nil {
			return item, fmt.Errorf("list: dosearch: newitem: %w", err)
		}
		return item, nil
	})
	if err != nil {
		return result, fmt.Errorf("list: dosearch: %w", err)
	}
	return result, nil
}

func RList[T any](q Queryable, query *QueryParams) ([]*Item[*Raw[T]], error) {
	serde := q.getSerde()
	result, err := doSearch(q, query, nil, func(i *item) (*Item[*Raw[T]], error) {
		return newRawItem[T](serde, i), nil
	})
	if err != nil {
		return result, fmt.Errorf("list: dosearch: %w", err)
	}
	return result, nil
}

func ListPaths(q Queryable, query *QueryParams) ([]string, error) {
	result, err := doSearch(q, query, nil, func(i *item) (string, error) {
		return i.path, nil
	})
	if err != nil {
		return result, fmt.Errorf("list: dosearch: %w", err)
	}
	return result, nil
}

func Search[T any](q Queryable, query *QueryParams, search *SearchParams) ([]*SearchResult[T], error) {
	serde := q.getSerde()
	result, err := doSearch(q, query, search, func(i *item) (*SearchResult[T], error) {
		item, err := newItem[T](serde, i)
		if err != nil {
			return nil, fmt.Errorf("search: dosearch: newitem: %w", err)
		}
		return &SearchResult[T]{
			Item:    *item,
			Snippet: i.snippet,
		}, nil
	})
	if err != nil {
		return result, fmt.Errorf("list: dosearch: %w", err)
	}
	return result, nil
}

func RSearch[T any](q Queryable, query *QueryParams, search *SearchParams) ([]*SearchResult[*Raw[T]], error) {
	serde := q.getSerde()
	result, err := doSearch(q, query, search, func(i *item) (*SearchResult[*Raw[T]], error) {
		item := newRawItem[T](serde, i)
		return &SearchResult[*Raw[T]]{
			Item:    *item,
			Snippet: i.snippet,
		}, nil
	})
	if err != nil {
		return result, fmt.Errorf("list: dosearch: %w", err)
	}
	return result, nil
}

func doSearch[I any](q Queryable, query *QueryParams, search *SearchParams, buildItem func(*item) (I, error)) ([]I, error) {
	var items []I
	var _items []*item
	_items, err := q.List(query, search)
	if err != nil {
		return items, fmt.Errorf("dosearch: list: %w", err)
	}

	items = make([]I, 0, len(_items))
	for _, i := range _items {
		var item I
		item, err = buildItem(i)
		if err != nil {
			return items, fmt.Errorf("dosearch: builditem: %w", err)
		}
		items = append(items, item)
	}

	return items, nil
}

func newItem[T any](s *serde, i *item) (*Item[T], error) {
	_value, err := s.deserialize(i.value)
	if err != nil {
		return nil, fmt.Errorf("newitem: deserialize: %w", err)
	}
	return &Item[T]{
		Path:       i.path,
		DetailPath: i.detailPath,
		Value:      _value.(T),
	}, nil
}

func newRawItem[T any](s *serde, i *item) *Item[*Raw[T]] {
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
	return result
}
