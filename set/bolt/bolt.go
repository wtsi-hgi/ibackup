package bolt

import (
	"os"

	"github.com/wtsi-hgi/ibackup/set/db"
	"go.etcd.io/bbolt"
)

type DB struct {
	*bbolt.DB
}

func Open(path string, mode os.FileMode, options *bbolt.Options) (db.DB, error) { //nolint:ireturn
	db, err := bbolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}

	return DB{db}, err
}

func (d DB) Update(fn func(db.Tx) error) error {
	return d.DB.Update(func(tx *bbolt.Tx) error {
		return fn(Tx{tx})
	})
}

func (d DB) View(fn func(db.Tx) error) error {
	return d.DB.View(func(tx *bbolt.Tx) error {
		return fn(Tx{tx})
	})
}

type Tx struct {
	*bbolt.Tx
}

func (t Tx) Bucket(bucket []byte) db.Bucket { //nolint:ireturn
	b := t.Tx.Bucket(bucket)
	if b == nil {
		return nil
	}

	return Bucket{b}
}

func (t Tx) CreateBucketIfNotExists(bucket []byte) (db.Bucket, error) { //nolint:ireturn
	b, err := t.Tx.CreateBucketIfNotExists(bucket)
	if err != nil {
		return nil, err
	}

	return Bucket{b}, nil
}

type Bucket struct {
	*bbolt.Bucket
}

func (b Bucket) CreateBucketIfNotExists(bucket []byte) (db.Bucket, error) { //nolint:ireturn
	bb, err := b.Bucket.CreateBucketIfNotExists(bucket)
	if err != nil {
		return nil, err
	}

	return Bucket{bb}, nil
}
