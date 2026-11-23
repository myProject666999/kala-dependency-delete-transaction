package job

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
)

// ErrJobNotFound is raised when a Job is unable to be found within a database.
type ErrJobNotFound string

func (id ErrJobNotFound) Error() string {
	return fmt.Sprintf("Job with id of %s not found.", string(id))
}

// ErrAtomicBatchUnsupported is returned when a backend cannot apply a job
// delete together with its dependency repairs as one all-or-nothing unit.
// Such a delete is refused rather than degraded to best-effort: a half-updated
// reference graph is exactly what the synchronous delete exists to prevent.
var ErrAtomicBatchUnsupported = errors.New(
	"job storage backend cannot apply a delete and its dependency repairs atomically",
)

type JobDB interface {
	GetAll() ([]*Job, error)
	Get(id string) (*Job, error)
	Delete(id string) error
	Save(job *Job) error
	Close() error
}

func (j *Job) Delete(cache JobCache) error {
	if err := cache.Delete(j.Id); err != nil {
		log.Errorf("Error occurred while trying to delete job from cache: %s", err)
		return err
	}
	return nil
}

func DeleteAll(cache JobCache) error {
	allJobs := cache.GetAll()
	allJobs.Lock.RLock()
	// make a copy of all jobs to prevent deadlock on delete
	jobsCopy := make([]*Job, 0, len(allJobs.Jobs))
	for _, j := range allJobs.Jobs {
		jobsCopy = append(jobsCopy, j)
	}
	allJobs.Lock.RUnlock()

	for _, j := range jobsCopy {
		if err := j.Delete(cache); err != nil {
			return err
		}
	}
	return nil
}
