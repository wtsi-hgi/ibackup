package db

import "iter"

type scanner interface {
	Scan(dest ...any) error
}

type IterErr[T any] struct {
	Iter  iter.Seq[T]
	Error error
}

func noSeq[T any](_ func(T) bool) {}

func iterRows[T any](d *DBRO, scanner func(scanner) (T, error), query string, args ...any) *IterErr[T] {
	var ie IterErr[T]

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return iterErr[T](err)
	}

	ie.Iter = func(yield func(T) bool) {
		defer rows.Close()

		for rows.Next() {
			v, err := scanner(rows)
			if err != nil {
				ie.Error = err

				return
			}

			if !yield(v) {
				break
			}
		}

		ie.Error = rows.Err()
	}

	return &ie
}

func iterErr[T any](err error) *IterErr[T] {
	return &IterErr[T]{
		Iter:  noSeq[T],
		Error: err,
	}
}

func (i *IterErr[T]) ForEach(fn func(T) error) error {
	if i.Error != nil {
		return i.Error
	}

	for item := range i.Iter {
		if err := fn(item); err != nil {
			return err
		}
	}

	return i.Error
}
