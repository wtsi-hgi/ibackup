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
			set, err := scanner(rows)
			if err != nil {
				ie.Error = err

				return
			}

			if !yield(set) {
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
