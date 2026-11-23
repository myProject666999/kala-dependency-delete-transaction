package mysql

import (
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ajvb/kala/job"
	"github.com/stretchr/testify/assert"
)

// ApplyBatch must write saves, reference updates and deletes inside one
// transaction and commit only after all of them succeed.
func TestApplyBatchCommitsAllWrites(t *testing.T) {
	db, m := NewTestDb()

	j := job.GetMockJob()
	j.Id = "job-a"
	enc, err := json.Marshal(j)
	assert.NoError(t, err)

	m.ExpectBegin()
	m.ExpectExec("replace .*").WithArgs("job-a", string(enc)).WillReturnResult(sqlmock.NewResult(1, 1))
	m.ExpectQuery("select .*").WithArgs("job-p").
		WillReturnRows(sqlmock.NewRows([]string{"job"}).AddRow(string(enc)))
	m.ExpectExec("update .*").WithArgs(sqlmock.AnyArg(), "job-p").WillReturnResult(sqlmock.NewResult(1, 1))
	m.ExpectExec("delete .*").WithArgs("job-d").WillReturnResult(sqlmock.NewResult(1, 1))
	m.ExpectCommit()

	err = db.ApplyBatch(job.JobBatch{
		Saves:   []*job.Job{j},
		Updates: []job.JobReferenceUpdate{{Id: "job-p", ParentJobs: []string{}, DependentJobs: []string{}}},
		Deletes: []string{"job-d"},
	})
	assert.NoError(t, err)
	assert.NoError(t, m.ExpectationsWereMet())
}

// A failure part-way through ApplyBatch must roll the whole transaction back,
// so nothing is partially written.
func TestApplyBatchRollsBackOnError(t *testing.T) {
	db, m := NewTestDb()

	m.ExpectBegin()
	m.ExpectExec("delete .*").WithArgs("job-d").WillReturnError(assert.AnError)
	m.ExpectRollback()

	err := db.ApplyBatch(job.JobBatch{Deletes: []string{"job-d"}})
	assert.Error(t, err)
	assert.NoError(t, m.ExpectationsWereMet())
}
