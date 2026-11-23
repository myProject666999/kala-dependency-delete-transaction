package boltdb

import (
	"bytes"
	"encoding/gob"
	"os"
	"strings"
	"time"

	"github.com/ajvb/kala/job"

	log "github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

var (
	jobBucket = []byte("jobs")
)

func GetBoltDB(path string) *BoltJobDB {
	if path != "" && !strings.HasSuffix(path, "/") {
		path += "/"
	}
	path += "jobdb.db"
	var perms os.FileMode = 0o0600
	database, err := bolt.Open(path, perms, &bolt.Options{Timeout: time.Second * 10}) //nolint:gomnd
	if err != nil {
		log.Fatal(err)
	}
	return &BoltJobDB{
		path:   path,
		dbConn: database,
	}
}

type BoltJobDB struct {
	dbConn *bolt.DB
	path   string
}

func (db *BoltJobDB) Close() error {
	return db.dbConn.Close()
}

func (db *BoltJobDB) GetAll() ([]*job.Job, error) {
	allJobs := []*job.Job{}

	err := db.dbConn.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(jobBucket)
		if err != nil {
			return err
		}

		err = bucket.ForEach(func(k, v []byte) error {
			buffer := bytes.NewBuffer(v)
			dec := gob.NewDecoder(buffer)
			j := new(job.Job)
			err = dec.Decode(j)

			if err != nil {
				return err
			}

			err = j.InitDelayDuration(false)

			if err != nil {
				return err
			}

			allJobs = append(allJobs, j)

			return nil
		})

		return err
	})

	return allJobs, err
}

func (db *BoltJobDB) Get(id string) (*job.Job, error) {
	j := new(job.Job)

	err := db.dbConn.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobBucket)

		v := b.Get([]byte(id))
		if v == nil {
			return job.ErrJobNotFound(id)
		}

		buf := bytes.NewBuffer(v)
		err := gob.NewDecoder(buf).Decode(j)

		return err
	})
	if err != nil {
		return nil, err
	}

	j.Id = id
	return j, nil
}

func (db *BoltJobDB) Delete(id string) error {
	err := db.dbConn.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(jobBucket)
		return bucket.Delete([]byte(id))
	})
	return err
}

func (db *BoltJobDB) Save(j *job.Job) error {
	err := db.dbConn.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(jobBucket)
		if err != nil {
			return err
		}

		return saveJob(bucket, j)
	})
	return err
}

// ApplyBatch writes every delete and reference update in one Bolt transaction.
// Bolt commits a transaction entirely or not at all, so a crash leaves the
// reference graph exactly as it was before the batch.
func (db *BoltJobDB) ApplyBatch(batch job.JobBatch) error {
	return db.dbConn.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(jobBucket)
		if err != nil {
			return err
		}

		for _, j := range batch.Saves {
			if err := saveJob(bucket, j); err != nil {
				return err
			}
		}
		for _, upd := range batch.Updates {
			if err := saveReferenceUpdate(bucket, upd); err != nil {
				return err
			}
		}
		for _, id := range batch.Deletes {
			if err := bucket.Delete([]byte(id)); err != nil {
				return err
			}
		}
		return nil
	})
}

func saveJob(bucket *bolt.Bucket, j *job.Job) error {
	buffer := new(bytes.Buffer)
	enc := gob.NewEncoder(buffer)
	if err := enc.Encode(j); err != nil {
		return err
	}
	return bucket.Put([]byte(j.Id), buffer.Bytes())
}

// saveReferenceUpdate rewrites only the reference lists of a stored job,
// leaving the rest of the record as stored. An absent job is skipped.
func saveReferenceUpdate(bucket *bolt.Bucket, upd job.JobReferenceUpdate) error {
	v := bucket.Get([]byte(upd.Id))
	if v == nil {
		return nil
	}
	stored := new(job.Job)
	if err := gob.NewDecoder(bytes.NewBuffer(v)).Decode(stored); err != nil {
		return err
	}
	stored.ParentJobs = upd.ParentJobs
	stored.DependentJobs = upd.DependentJobs
	return saveJob(bucket, stored)
}

var _ job.BatchJobDB = (*BoltJobDB)(nil)
