package boltdb

import (
	"testing"
	"time"

	"github.com/ajvb/kala/job"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ApplyBatch must persist saves, reference updates and deletes together, and
// leave the rest of each updated record untouched.
func TestApplyBatch(t *testing.T) {
	setupTest(t)

	db := GetBoltDB(testDbPath)
	defer db.Close()

	parent := job.GetMockJobWithGenericSchedule(time.Now())
	parent.Id = "parent"
	parent.DependentJobs = []string{"child", "other"}
	require.NoError(t, db.Save(parent))

	other := job.GetMockJobWithGenericSchedule(time.Now())
	other.Id = "other"
	require.NoError(t, db.Save(other))

	newJob := job.GetMockJob()
	newJob.Id = "fresh"

	err := db.ApplyBatch(job.JobBatch{
		Saves: []*job.Job{newJob},
		Updates: []job.JobReferenceUpdate{{
			Id:            "parent",
			ParentJobs:    []string{},
			DependentJobs: []string{"other"},
		}},
		Deletes: []string{"child"},
	})
	require.NoError(t, err)

	gotParent, err := db.Get("parent")
	require.NoError(t, err)
	assert.Equal(t, []string{"other"}, gotParent.DependentJobs, "reference list rewritten")
	assert.Equal(t, parent.Name, gotParent.Name, "other fields preserved")

	_, err = db.Get("child")
	assert.Error(t, err, "deleted id removed")

	_, err = db.Get("other")
	assert.NoError(t, err, "untouched job survives")

	gotNew, err := db.Get("fresh")
	require.NoError(t, err)
	assert.Equal(t, newJob.Name, gotNew.Name)
}

// An update naming a job that is not stored is skipped, matching the batch
// contract, and does not fail the batch.
func TestApplyBatchSkipsAbsentUpdate(t *testing.T) {
	setupTest(t)

	db := GetBoltDB(testDbPath)
	defer db.Close()

	err := db.ApplyBatch(job.JobBatch{
		Updates: []job.JobReferenceUpdate{{Id: "missing", ParentJobs: nil, DependentJobs: nil}},
	})
	assert.NoError(t, err)
}
