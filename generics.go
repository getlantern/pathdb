package pathdb

func Get[T any](db DB, key string) (T, error) {
	var result T
	return result, nil
}