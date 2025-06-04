package bolt

import (
	"os"

	"github.com/wtsi-hgi/ibackup/set/db"
	"go.etcd.io/bbolt"
)

type d = bbolt.DB

type DB struct {
	*d
}

func Open(path string, mode os.FileMode, options *bbolt.Options) (db.DB, error) { //nolint:ireturn
	db, err := bbolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}

	return DB{db}, err
}

func (d DB) Update(fn func(db.Tx) error) error {
	return d.d.Update(func(tx *bbolt.Tx) error {
		return fn(Tx{tx})
	})
}

func (d DB) View(fn func(db.Tx) error) error {
	return d.d.View(func(tx *bbolt.Tx) error {
		return fn(Tx{tx})
	})
}

type tx = bbolt.Tx

type Tx struct {
	*tx
}

func (t Tx) Bucket(bucket []byte) db.Bucket { //nolint:ireturn
	b := t.tx.Bucket(bucket)
	if b == nil {
		return nil
	}

	return Bucket{b}
}

func (t Tx) CreateBucketIfNotExists(bucket []byte) (db.Bucket, error) { //nolint:ireturn
	b, err := t.tx.CreateBucketIfNotExists(bucket)
	if err != nil {
		return nil, err
	}

	return Bucket{b}, nil
}

type bucket = bbolt.Bucket

type Bucket struct {
	*bucket
}

func (b Bucket) CreateBucketIfNotExists(bucket []byte) (db.Bucket, error) { //nolint:ireturn
	bb, err := b.bucket.CreateBucketIfNotExists(bucket)
	if err != nil {
		return nil, err
	}

	return Bucket{bb}, nil
}

func (b Bucket) Bucket(bucket []byte) db.Bucket { //nolint:ireturn
	bb := b.bucket.Bucket(bucket)
	if bb == nil {
		return nil
	}

	return Bucket{bb}
}
