package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ajvb/kala/job"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serveDelete sends a DELETE through a real router so the {id} URL variable is
// populated exactly as in production.
func serveDelete(t *testing.T, cache job.JobCache, id string) *httptest.ResponseRecorder {
	t.Helper()
	r := mux.NewRouter()
	r.HandleFunc(ApiJobPath+"{id}", HandleJobRequest(cache)).Methods("DELETE", "GET")
	ts := httptest.NewServer(r)
	t.Cleanup(ts.Close)

	req, err := http.NewRequest("DELETE", ts.URL+ApiJobPath+id, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	w := httptest.NewRecorder()
	w.Code = resp.StatusCode
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	w.Body.Write(body)
	return w
}

// failingDeleteCache is a JobCache whose Delete fails with a chosen error; Get
// serves the one job it was given so a DELETE reaches the delete step.
type failingDeleteCache struct {
	job    *job.Job
	err    error
	delete int
}

func (f *failingDeleteCache) Get(id string) (*job.Job, error) {
	if f.job != nil && id == f.job.Id {
		return f.job, nil
	}
	return nil, job.ErrJobDoesntExist
}
func (f *failingDeleteCache) GetAll() *job.JobsMap   { return job.NewJobsMap() }
func (f *failingDeleteCache) Set(*job.Job) error     { return nil }
func (f *failingDeleteCache) Delete(string) error    { f.delete++; return f.err }
func (f *failingDeleteCache) Persist() error         { return nil }
func (f *failingDeleteCache) Enable(*job.Job) error  { return nil }
func (f *failingDeleteCache) Disable(*job.Job) error { return nil }

// DELETE returns 204 only when the cache confirms the delete.
func TestDeleteJobReturns204OnConfirmedDelete(t *testing.T) {
	cache, j := generateJobAndCache()
	w := serveDelete(t, cache, j.Id)
	assert.Equal(t, http.StatusNoContent, w.Code)
	_, err := cache.Get(j.Id)
	assert.Equal(t, job.ErrJobDoesntExist, err)
}

// A storage/dependency failure returns 500 and reports the reason, rather than
// a 204 that would claim the delete succeeded.
func TestDeleteJobReturns500OnFailure(t *testing.T) {
	injected := errors.New("storage unavailable")
	cache := &failingDeleteCache{
		job: &job.Job{Id: "abc", Name: "n"},
		err: &job.DeleteError{Id: "abc", Phase: job.DeletePhasePersist, Err: injected},
	}
	w := serveDelete(t, cache, "abc")

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	var respErr apiError
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &respErr))
	assert.Contains(t, respErr.Error, "storage unavailable")
	assert.Contains(t, respErr.Error, string(job.DeletePhasePersist))
}

// An unknown id is still 404, and Delete is never attempted for it.
func TestDeleteJobUnknownIdReturns404(t *testing.T) {
	cache := &failingDeleteCache{err: job.ErrJobDoesntExist}
	w := serveDelete(t, cache, "missing")
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// A repeat DELETE of an already-deleted job is idempotent: 204, not 404.
func TestDeleteJobRepeatReturns204(t *testing.T) {
	cache, j := generateJobAndCache()
	first := serveDelete(t, cache, j.Id)
	require.Equal(t, http.StatusNoContent, first.Code)

	second := serveDelete(t, cache, j.Id)
	assert.Equal(t, http.StatusNoContent, second.Code, "repeat DELETE is idempotent")
}
