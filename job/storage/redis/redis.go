package redis

import (
	"github.com/ajvb/kala/job"

	"github.com/garyburd/redigo/redis"
	log "github.com/sirupsen/logrus"
)

var (
	// HashKey is the hash key where jobs are persisted.
	HashKey = "kala:jobs"
)

// DB is concrete implementation of the JobDB interface, that uses Redis for persistence.
type DB struct {
	conn redis.Conn
}

// New instantiates a new DB.
func New(address string, password redis.DialOption, sendPassword bool) *DB {
	var conn redis.Conn
	var err error
	if address == "" {
		address = "127.0.0.1:6379"
	}
	if sendPassword {
		conn, err = redis.Dial("tcp", address, password)
	} else {
		conn, err = redis.Dial("tcp", address)
	}
	if err != nil {
		log.Fatal(err)
	}
	return &DB{
		conn: conn,
	}
}

// GetAll returns all persisted Jobs.
func (d DB) GetAll() ([]*job.Job, error) {
	jobs := []*job.Job{}

	vals, err := d.conn.Do("HVALS", HashKey)
	if err != nil {
		return jobs, err
	}

	for _, val := range vals.([]interface{}) {
		j, err := job.NewFromBytes(val.([]byte))
		if err != nil {
			return nil, err
		}

		err = j.InitDelayDuration(false)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, j)
	}

	return jobs, nil
}

// Get returns a persisted Job.
func (d DB) Get(id string) (*job.Job, error) {
	val, err := d.conn.Do("HGET", HashKey, id)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, job.ErrJobNotFound(id)
	}

	return job.NewFromBytes(val.([]byte))
}

// Delete deletes a persisted Job.
func (d DB) Delete(id string) error {
	_, err := d.conn.Do("HDEL", HashKey, id)
	if err != nil {
		return err
	}

	return nil
}

// Save persists a Job.
func (d DB) Save(j *job.Job) error {
	bytes, err := j.Bytes()
	if err != nil {
		return err
	}

	_, err = d.conn.Do("HSET", HashKey, j.Id, bytes)
	if err != nil {
		return err
	}

	return nil
}

// ApplyBatch runs the whole batch inside one MULTI/EXEC transaction, which
// Redis executes atomically. A crash before EXEC leaves no partial writes.
func (d DB) ApplyBatch(batch job.JobBatch) error {
	updates, err := d.referenceUpdateValues(batch.Updates)
	if err != nil {
		return err
	}

	if err := d.conn.Send("MULTI"); err != nil {
		return err
	}
	for _, j := range batch.Saves {
		bytes, err := j.Bytes()
		if err != nil {
			d.abortTransaction()
			return err
		}
		if err := d.conn.Send("HSET", HashKey, j.Id, bytes); err != nil {
			d.abortTransaction()
			return err
		}
	}
	for _, upd := range updates {
		if err := d.conn.Send("HSET", HashKey, upd.id, upd.bytes); err != nil {
			d.abortTransaction()
			return err
		}
	}
	for _, id := range batch.Deletes {
		if err := d.conn.Send("HDEL", HashKey, id); err != nil {
			d.abortTransaction()
			return err
		}
	}
	_, err = d.conn.Do("EXEC")
	return err
}

type referenceUpdateValue struct {
	id    string
	bytes []byte
}

// referenceUpdateValues loads each stored job named by an update and returns
// the re-encoded record whose reference lists were replaced. Absent jobs are
// skipped. Reads happen before MULTI so the transaction itself is write-only.
func (d DB) referenceUpdateValues(updates []job.JobReferenceUpdate) ([]referenceUpdateValue, error) {
	out := make([]referenceUpdateValue, 0, len(updates))
	for _, upd := range updates {
		stored, err := d.Get(upd.Id)
		if err != nil {
			// A job that is not stored (or was concurrently removed) is skipped,
			// matching the batch contract.
			continue
		}
		stored.ParentJobs = upd.ParentJobs
		stored.DependentJobs = upd.DependentJobs
		b, err := stored.Bytes()
		if err != nil {
			return nil, err
		}
		out = append(out, referenceUpdateValue{id: upd.Id, bytes: b})
	}
	return out, nil
}

// abortTransaction sends DISCARD so a partially queued MULTI does not linger
// on the pooled connection.
func (d DB) abortTransaction() {
	_, _ = d.conn.Do("DISCARD")
}

// Close closes the connection to Redis.
func (d DB) Close() error {
	err := d.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

var _ job.BatchJobDB = (*DB)(nil)
