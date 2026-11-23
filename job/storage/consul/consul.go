package consul

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/ajvb/kala/job"

	"github.com/hashicorp/consul/api"

	log "github.com/sirupsen/logrus"
)

var (
	prefix = "kala/jobs/"
)

func New(address string) *ConsulJobDB {
	config := api.DefaultConfig()
	if address != "" {
		config.Address = address
	}
	client, err := api.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	return &ConsulJobDB{
		conn: client.KV(),
	}
}

type ConsulJobDB struct {
	conn *api.KV
}

func (db *ConsulJobDB) Close() error {
	return nil
}

func (db *ConsulJobDB) GetAll() ([]*job.Job, error) {
	allJobs := []*job.Job{}

	pairs, _, err := db.conn.List(prefix, &api.QueryOptions{RequireConsistent: true})
	if err != nil {
		return allJobs, err
	}
	for _, pair := range pairs {

		buffer := bytes.NewBuffer(pair.Value)
		dec := json.NewDecoder(buffer)
		j := new(job.Job)
		err = dec.Decode(j)
		if err != nil {
			continue
		}
		err = j.InitDelayDuration(false)
		if err != nil {
			continue
		}
		allJobs = append(allJobs, j)
	}

	return allJobs, err
}

func (db *ConsulJobDB) Get(id string) (*job.Job, error) {
	j := new(job.Job)

	pair, _, err := db.conn.Get(prefix+id, &api.QueryOptions{RequireConsistent: true})
	if err != nil {
		return nil, err
	}
	if pair == nil {
		return nil, fmt.Errorf("ID %s not found", id)
	}
	buf := bytes.NewBuffer(pair.Value)
	err = json.NewDecoder(buf).Decode(j)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func (db *ConsulJobDB) Delete(id string) error {
	_, err := db.conn.Delete(prefix+id, &api.WriteOptions{})
	return err
}

func (db *ConsulJobDB) Save(j *job.Job) error {
	buffer := new(bytes.Buffer)
	enc := json.NewEncoder(buffer)
	err := enc.Encode(j)
	if err != nil {
		return err
	}
	pair := &api.KVPair{Key: prefix + j.Id, Value: buffer.Bytes()}
	_, err = db.conn.Put(pair, &api.WriteOptions{})
	return err
}

// ApplyBatch commits the whole batch in one Consul transaction. Consul applies
// a transaction all-or-nothing, so a crash leaves no half-updated reference
// graph.
func (db *ConsulJobDB) ApplyBatch(batch job.JobBatch) error {
	var ops api.KVTxnOps

	for _, j := range batch.Saves {
		value, err := json.Marshal(j)
		if err != nil {
			return err
		}
		ops = append(ops, &api.KVTxnOp{Verb: api.KVSet, Key: prefix + j.Id, Value: value})
	}
	for _, upd := range batch.Updates {
		value, found, err := db.encodeReferenceUpdate(upd)
		if err != nil {
			return err
		}
		if found {
			ops = append(ops, &api.KVTxnOp{Verb: api.KVSet, Key: prefix + upd.Id, Value: value})
		}
	}
	for _, id := range batch.Deletes {
		ops = append(ops, &api.KVTxnOp{Verb: api.KVDelete, Key: prefix + id})
	}
	if len(ops) == 0 {
		return nil
	}

	ok, resp, _, err := db.conn.Txn(ops, nil)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("consul rejected the job batch transaction: %v", resp.Errors)
	}
	return nil
}

// encodeReferenceUpdate returns the stored job re-encoded with only its
// reference lists replaced, and reports false when the job is absent.
func (db *ConsulJobDB) encodeReferenceUpdate(upd job.JobReferenceUpdate) ([]byte, bool, error) {
	pair, _, err := db.conn.Get(prefix+upd.Id, &api.QueryOptions{RequireConsistent: true})
	if err != nil {
		return nil, false, err
	}
	if pair == nil {
		return nil, false, nil
	}
	stored := new(job.Job)
	if err := json.NewDecoder(bytes.NewBuffer(pair.Value)).Decode(stored); err != nil {
		return nil, false, err
	}
	stored.ParentJobs = upd.ParentJobs
	stored.DependentJobs = upd.DependentJobs
	value, err := json.Marshal(stored)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

var _ job.BatchJobDB = (*ConsulJobDB)(nil)
