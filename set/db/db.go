package db

import "io"

type DB interface {
	View(fn func(Tx) error) error
	Update(fn func(Tx) error) error
	io.Closer
}

type Tx interface {
	CreateBucketIfNotExists(bucket []byte) (Bucket, error)
	Bucket(bucket []byte) Bucket
	io.WriterTo
}

type Bucket interface {
	Get(id []byte) []byte
	Put(id, value []byte) error
	Delete(id []byte) error
	ForEach(fn func([]byte, []byte) error) error
	Cursor() Cursor
	CreateBucket(bucket []byte) (Bucket, error)
	CreateBucketIfNotExists(bucket []byte) (Bucket, error)
	DeleteBucket(bucket []byte) error
	Bucket(bucket []byte) Bucket
	NextSequence() (uint64, error)
}

type Cursor interface {
	First() ([]byte, []byte)
	Next() ([]byte, []byte)
	Seek(key []byte) ([]byte, []byte)
}
