package db

type DB interface {
	View(fn func(Tx) error) error
	Update(fn func(Tx) error) error
}

type Tx interface {
	CreateBucketIfNotExists(bucket []byte) (Bucket, error)
	Bucket(bucket []byte) Bucket
}

type Bucket interface {
	Get(id []byte) []byte
	Put(id, value []byte) error
	CreateBucketIfNotExists(bucket []byte) (Bucket, error)
	ForEach(fn func([]byte, []byte) error) error
}
