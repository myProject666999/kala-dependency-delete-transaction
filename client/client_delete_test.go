package client

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ajvb/kala/api"
	"github.com/ajvb/kala/job"
)

// deleteFailingCache makes Delete fail, so the client can be checked against
// the API's 500 path.
type deleteFailingCache struct{ err error }

func (d deleteFailingCache) Get(string) (*job.Job, error) { return &job.Job{Id: "x"}, nil }
func (d deleteFailingCache) GetAll() *job.JobsMap         { return job.NewJobsMap() }
func (d deleteFailingCache) Set(*job.Job) error           { return nil }
func (d deleteFailingCache) Delete(string) error          { return d.err }
func (d deleteFailingCache) Persist() error               { return nil }
func (d deleteFailingCache) Enable(*job.Job) error        { return nil }
func (d deleteFailingCache) Disable(*job.Job) error       { return nil }

// A failed delete returns a *DeleteError carrying the HTTP status and the
// server's explanation, so callers can tell it apart from a network error.
func TestDeleteJobFailureCarriesServerReason(t *testing.T) {
	cache := deleteFailingCache{
		err: &job.DeleteError{Id: "x", Phase: job.DeletePhasePersist, Err: errors.New("backend unavailable")},
	}
	r := mux.NewRouter()
	r.HandleFunc(api.ApiJobPath+"{id}/", api.HandleJobRequest(cache)).Methods("DELETE", "GET")
	ts := httptest.NewServer(r)
	defer ts.Close()

	ok, err := New(ts.URL).DeleteJob("x")
	assert.False(t, ok)
	require.Error(t, err)

	var delErr *DeleteError
	require.True(t, errors.As(err, &delErr))
	assert.Equal(t, http.StatusInternalServerError, delErr.StatusCode)
	assert.Contains(t, delErr.Message, "backend unavailable")
	assert.Contains(t, delErr.Error(), string(job.DeletePhasePersist))
}

// A confirmed delete still returns (true, nil).
func TestDeleteJobSuccessStillNil(t *testing.T) {
	ts := NewTestServer()
	defer ts.Close()
	kc := New(ts.URL)

	id, err := kc.CreateJob(NewJobMap())
	require.NoError(t, err)
	ok, err := kc.DeleteJob(id)
	assert.NoError(t, err)
	assert.True(t, ok)
	cleanUp()
}
