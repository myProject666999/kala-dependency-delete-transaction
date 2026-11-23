package mongo

import (
	"testing"

	"github.com/ajvb/kala/job"
	"github.com/stretchr/testify/assert"
)

// This driver cannot commit a job delete and its reference repairs atomically,
// so the backend must refuse rather than degrade to a best-effort sequence.
// Deletes against mongo therefore fail with job.ErrAtomicBatchUnsupported.
func TestApplyBatchRefuses(t *testing.T) {
	d := DB{}
	err := d.ApplyBatch(job.JobBatch{Deletes: []string{"x"}})
	assert.ErrorIs(t, err, job.ErrAtomicBatchUnsupported)
}
