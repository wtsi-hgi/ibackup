package set

import (
	bolt "go.etcd.io/bbolt"
)

const txLimit = 10_000

type BoltDB struct {
	*bolt.DB
}

func (b *BoltDB) Update(fn func(Tx) error) error {
	tx := &limitedTx{db: b.DB, writable: true}

	if err := fn(tx); err != nil {
		tx.Rollback()

		return err
	}

	return tx.Commit()
}

func (b *BoltDB) View(fn func(Tx) error) error {
	tx := &limitedTx{db: b.DB}

	if err := fn(tx); err != nil {
		tx.Rollback()

		return err
	}

	return tx.Rollback()
}

// Tx represents the required methods in a bolt Transaction.
type Tx interface {
	Bucket([]byte) Bucket
	CreateBucketIfNotExists([]byte) (Bucket, error)
}

type limitedTx struct {
	db       *bolt.DB
	tx       *bolt.Tx
	count    int
	writable bool
}

func (l *limitedTx) init() error {
	if l.tx != nil {
		return nil
	}

	var err error

	l.tx, err = l.db.Begin(l.writable)

	return err
}

func (l *limitedTx) Rollback() error {
	if l.tx == nil {
		return nil
	}

	return l.tx.Rollback()
}

func (l *limitedTx) Commit() error {
	if l.tx == nil {
		return nil
	}

	if err := l.tx.Commit(); err != nil {
		return err
	}

	l.count = 0
	l.tx = nil

	return nil
}

func (l *limitedTx) Bucket(key []byte) Bucket {
	b := l.getBucket(key)
	if b == nil {
		return nil
	}

	return &limitedBucket{p: l, key: key, bucket: b}
}

func (l *limitedTx) CreateBucketIfNotExists(key []byte) (Bucket, error) {
	if err := l.init(); err != nil {
		return nil, err
	}

	b, err := l.tx.CreateBucketIfNotExists(key)
	if err != nil {
		return nil, err
	}

	return &limitedBucket{p: l, key: key, bucket: b}, nil
}

func (l *limitedTx) Tx() *bolt.Tx {
	return l.tx
}

func (l *limitedTx) getBucket(key []byte) *bolt.Bucket {
	if err := l.init(); err != nil {
		return nil
	}

	return l.tx.Bucket(key)
}

func (l *limitedTx) incrPut() error {
	l.count++

	if l.count == txLimit {
		if err := l.Commit(); err != nil {
			return err
		}
	}

	return l.init()
}

type Bucket interface {
	Bucket([]byte) Bucket
	CreateBucket([]byte) (Bucket, error)
	CreateBucketIfNotExists([]byte) (Bucket, error)
	Get([]byte) []byte
	Put([]byte, []byte) error
	Delete([]byte) error
	DeleteBucket([]byte) error
	NextSequence() (uint64, error)
	ForEach(func([]byte, []byte) error) error
	Cursor() Cursor
}

type bucketParent interface {
	init() error
	Tx() *bolt.Tx
	getBucket([]byte) *bolt.Bucket
	incrPut() error
}

type limitedBucket struct {
	p      bucketParent
	key    []byte
	bucket *bolt.Bucket
}

func (l *limitedBucket) init() error {
	if l.bucket.Tx() == l.p.Tx() {
		return nil
	}

	if err := l.p.init(); err != nil {
		return err
	}

	l.bucket = l.p.getBucket(l.key)

	return nil
}

func (l *limitedBucket) Tx() *bolt.Tx {
	return l.p.Tx()
}

func (l *limitedBucket) getBucket(key []byte) *bolt.Bucket {
	if err := l.p.init(); err != nil {
		return nil
	}

	return l.bucket.Bucket(key)
}

func (l *limitedBucket) Bucket(key []byte) Bucket {
	b := l.getBucket(key)
	if b == nil {
		return nil
	}

	return &limitedBucket{p: l, key: key, bucket: b}
}

func (l *limitedBucket) CreateBucket(key []byte) (Bucket, error) {
	if err := l.init(); err != nil {
		return nil, err
	}

	b, err := l.bucket.CreateBucket(key)
	if err != nil {
		return nil, err
	}

	return &limitedBucket{p: l, key: key, bucket: b}, nil
}

func (l *limitedBucket) CreateBucketIfNotExists(key []byte) (Bucket, error) {
	if err := l.init(); err != nil {
		return nil, err
	}

	b, err := l.bucket.CreateBucketIfNotExists(key)
	if err != nil {
		return nil, err
	}

	return &limitedBucket{p: l, key: key, bucket: b}, nil
}

func (l *limitedBucket) Get(key []byte) []byte {
	if err := l.init(); err != nil {
		return nil
	}

	return l.bucket.Get(key)
}

func (l *limitedBucket) incrPut() error {
	if err := l.p.incrPut(); err != nil {
		return err
	}

	return l.init()
}

func (l *limitedBucket) Put(key, value []byte) error {
	if err := l.incrPut(); err != nil {
		return err
	}

	return l.bucket.Put(key, value)
}

func (l *limitedBucket) Delete(key []byte) error {
	if err := l.init(); err != nil {
		return err
	}

	return l.bucket.Delete(key)
}

func (l *limitedBucket) DeleteBucket(key []byte) error {
	if err := l.init(); err != nil {
		return err
	}

	return l.bucket.DeleteBucket(key)
}

func (l *limitedBucket) NextSequence() (uint64, error) {
	if err := l.init(); err != nil {
		return 0, err
	}

	return l.bucket.NextSequence()
}

func (l *limitedBucket) ForEach(fn func([]byte, []byte) error) error {
	if err := l.init(); err != nil {
		return err
	}

	return l.bucket.ForEach(fn)
}

func (l *limitedBucket) cursor() *bolt.Cursor {
	l.init()

	return l.bucket.Cursor()
}

func (l *limitedBucket) Cursor() Cursor {
	c := l.cursor()
	if c == nil {
		return nil
	}

	return &limitedCursor{
		bucket: l,
		cursor: c,
	}
}

type Cursor interface {
	First() ([]byte, []byte)
	Next() ([]byte, []byte)
	Seek([]byte) ([]byte, []byte)
	Delete() error
}

type limitedCursor struct {
	bucket  *limitedBucket
	cursor  *bolt.Cursor
	keyData [bolt.MaxKeySize]byte
	key     []byte
}

func (l *limitedCursor) init() {
	if l.cursor.Bucket().Tx() != l.bucket.Tx() {
		l.cursor = l.bucket.cursor()

		if l.cursor != nil && l.key != nil {
			l.cursor.Seek(l.key)
		}
	}
}

func (l *limitedCursor) First() ([]byte, []byte) {
	l.init()

	k, v := l.cursor.First()
	l.key = append(l.keyData[:], k...)

	return k, v
}

func (l *limitedCursor) Next() ([]byte, []byte) {
	l.init()

	k, v := l.cursor.Next()
	l.key = append(l.keyData[:], k...)

	return k, v
}

func (l *limitedCursor) Seek(key []byte) ([]byte, []byte) {
	l.init()

	k, v := l.cursor.Seek(key)
	l.key = append(l.keyData[:], k...)

	return k, v
}

func (l *limitedCursor) Delete() error {
	l.init()

	l.bucket.incrPut()

	return l.cursor.Delete()
}
