package redis

import (
	"testing"

	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ajvb/kala/job"
)

// ApplyBatch must queue every delete and reference update and finish with one
// EXEC. A MULTI/EXEC block is executed by Redis as a unit.
func TestApplyBatchUsesTransaction(t *testing.T) {
	c := redigomock.NewConn()
	d := DB{conn: c}

	stored := job.GetMockJob()
	stored.Id = "survivor"
	stored.Name = "keep me"
	storedBytes, err := stored.Bytes()
	require.NoError(t, err)

	// referenceUpdateValues reads the surviving job first.
	c.Command("HGET", HashKey, "survivor").Expect(storedBytes)
	c.Command("MULTI").Expect("OK")
	c.Command("HSET", HashKey, "survivor", redigomock.NewAnyData()).Expect("QUEUED")
	c.Command("HDEL", HashKey, "deleted").Expect("QUEUED")
	c.Command("EXEC").Expect([]interface{}{"OK", int64(1)})

	err = d.ApplyBatch(job.JobBatch{
		Updates: []job.JobReferenceUpdate{{Id: "survivor", ParentJobs: []string{}, DependentJobs: []string{}}},
		Deletes: []string{"deleted"},
	})
	assert.NoError(t, err)
}

// When EXEC reports an error the batch is reported failed so the cache leaves
// itself untouched.
func TestApplyBatchPropagatesExecError(t *testing.T) {
	c := redigomock.NewConn()
	d := DB{conn: c}

	c.Command("MULTI").Expect("OK")
	c.Command("HDEL", HashKey, "deleted").Expect("QUEUED")
	c.Command("EXEC").ExpectError(assert.AnError)

	err := d.ApplyBatch(job.JobBatch{Deletes: []string{"deleted"}})
	assert.Error(t, err)
}
