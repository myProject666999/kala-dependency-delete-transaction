package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ajvb/kala/api"
	"github.com/ajvb/kala/job"
)

const (
	methodGet    = "GET"
	methodPost   = "POST"
	methodDelete = "DELETE"
)

var (
	ErrJobNotFound      = errors.New("Job not found")
	ErrJobCreationError = errors.New("Error creating job")

	ErrGenericError = errors.New("An error occurred performing your request")

	jobPath = api.JobPath[:len(api.JobPath)-1]
)

// KalaClient is the base struct for this package.
type KalaClient struct {
	apiEndpoint string
}

// New is used to create a new KalaClient based off of the apiEndpoint
// Example:
// 		c := New("http://127.0.0.1:8000")
func New(apiEndpoint string) *KalaClient {
	apiEndpoint = strings.TrimSuffix(apiEndpoint, "/")
	apiUrlPrefix := api.ApiUrlPrefix[:len(api.ApiUrlPrefix)-1]

	return &KalaClient{
		apiEndpoint: apiEndpoint + apiUrlPrefix,
	}
}

func (kc *KalaClient) encode(value interface{}) (io.Reader, error) {
	if value == nil {
		return nil, nil
	}
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(value); err != nil {
		return nil, err
	}
	return buf, nil
}

func (kc *KalaClient) decode(body io.Reader, target interface{}) error {
	if target == nil {
		return nil
	}
	return json.NewDecoder(body).Decode(target)
}

func (kc *KalaClient) url(parts ...string) string {
	return strings.Join(append([]string{kc.apiEndpoint}, parts...), "/") + "/"
}

func (kc *KalaClient) do(method, url string, expectedStatus int, payload, target interface{}) (
	statusCode int,
	err error,
) {
	body, err := kc.encode(payload)
	if err != nil {
		return
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if status := resp.StatusCode; status != expectedStatus {
		// ErrGenericError stays comparable for existing callers; the server's
		// explanation is carried separately so it cannot leak between requests.
		return status, &unexpectedStatusError{status: status, message: errorMessageFrom(resp.Body)}
	}
	return resp.StatusCode, kc.decode(resp.Body, target)
}

// unexpectedStatusError is returned by do when the server answers with a status
// other than the one expected. It compares equal to ErrGenericError so existing
// callers keep working, and exposes the server's message.
type unexpectedStatusError struct {
	status  int
	message string
}

func (e *unexpectedStatusError) Error() string {
	if e.message != "" {
		return fmt.Sprintf("%s (status %d): %s", ErrGenericError.Error(), e.status, e.message)
	}
	return fmt.Sprintf("%s (status %d)", ErrGenericError.Error(), e.status)
}

// Is lets errors.Is(err, ErrGenericError) keep returning true.
func (e *unexpectedStatusError) Is(target error) bool { return target == ErrGenericError }

// ServerMessage returns the explanation Kala included, if any.
func (e *unexpectedStatusError) ServerMessage() string { return e.message }

// StatusCode returns the HTTP status Kala answered with.
func (e *unexpectedStatusError) StatusCode() int { return e.status }

// errorMessageFrom extracts the "error" field of Kala's JSON error body, if
// present. It never fails; an unreadable body yields "".
func errorMessageFrom(body io.Reader) string {
	var apiErr struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(body).Decode(&apiErr); err != nil {
		return ""
	}
	return apiErr.Error
}

// CreateJob is used for creating a new job within Kala. Note that the
// Name and Command fields are the only ones that are required.
// Example:
// 		c := New("http://127.0.0.1:8000")
// 		body := &job.Job{
//			Schedule: "R2/2015-06-04T19:25:16.828696-07:00/PT10S",
//			Name:	  "test_job",
//			Command:  "bash -c 'date'",
//		}
//		id, err := c.CreateJob(body)
func (kc *KalaClient) CreateJob(body *job.Job) (string, error) {
	id := &api.AddJobResponse{}
	_, err := kc.do(methodPost, kc.url(jobPath), http.StatusCreated, body, id)
	if err != nil {
		if errors.Is(err, ErrGenericError) {
			return "", ErrJobCreationError
		}
		return "", err
	}
	return id.Id, err
}

// GetJob is used to retrieve a Job from Kala by its ID.
// Example:
// 		c := New("http://127.0.0.1:8000")
//		id := "93b65499-b211-49ce-57e0-19e735cc5abd"
//		job, err := c.GetJob(id)
func (kc *KalaClient) GetJob(id string) (*job.Job, error) {
	j := &api.JobResponse{}
	_, err := kc.do(methodGet, kc.url(jobPath, id), http.StatusOK, nil, j)
	if err != nil {
		if errors.Is(err, ErrGenericError) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}
	return j.Job, nil
}

// GetAllJobs returns a map of string (ID's) to job.Job's which contains
// all Jobs currently within Kala.
// Example:
// 		c := New("http://127.0.0.1:8000")
//		jobs, err := c.GetAllJobs()
func (kc *KalaClient) GetAllJobs() (map[string]*job.Job, error) {
	jobs := &api.ListJobsResponse{}
	_, err := kc.do(methodGet, kc.url(jobPath), http.StatusOK, nil, jobs)
	return jobs.Jobs, err
}

// DeleteJob is used to delete a Job from Kala by its ID.
//
// A delete succeeds (true, nil) only when Kala confirmed both the removal and
// its dependency repairs (HTTP 204). If the server refused or could not
// complete the delete (HTTP 500, for example a storage failure or a backend
// that cannot delete atomically), the returned error carries the server's
// reason so the caller can distinguish it from an unreachable server. A repeat
// delete of a job that is already gone also succeeds.
// Example:
// 		c := New("http://127.0.0.1:8000")
//		id := "93b65499-b211-49ce-57e0-19e735cc5abd"
//		ok, err := c.DeleteJob(id)
func (kc *KalaClient) DeleteJob(id string) (bool, error) {
	status, err := kc.do(methodDelete, kc.url(jobPath, id), http.StatusNoContent, nil, nil)
	if err != nil {
		if errors.Is(err, ErrGenericError) {
			var statusErr *unexpectedStatusError
			message := ""
			if errors.As(err, &statusErr) {
				message = statusErr.ServerMessage()
			}
			return false, &DeleteError{StatusCode: status, Message: message}
		}
		return false, err
	}
	return true, nil
}

// DeleteError reports a delete that Kala did not confirm. StatusCode is the
// HTTP status Kala returned, and Message is its explanation when one was sent.
type DeleteError struct {
	StatusCode int
	Message    string
}

func (e *DeleteError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("Delete failed with a status code of %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("Delete failed with a status code of %d", e.StatusCode)
}

// DeleteAllJobs is used to delete all jobs from Kala
// Example:
// 		c := New("http://127.0.0.1:8000")
//		ok, err := c.DeleteAllJobs()
func (kc *KalaClient) DeleteAllJobs() (bool, error) {
	return kc.DeleteJob("all")
}

// GetJobStats is used to retrieve stats about a Job from Kala by its ID.
// Example:
// 		c := New("http://127.0.0.1:8000")
//		id := "93b65499-b211-49ce-57e0-19e735cc5abd"
//		stats, err := c.GetJobStats(id)
func (kc *KalaClient) GetJobStats(id string) ([]*job.JobStat, error) {
	js := &api.ListJobStatsResponse{}
	_, err := kc.do(methodGet, kc.url(jobPath, "stats", id), http.StatusOK, nil, js)
	return js.JobStats, err
}

// StartJob is used to manually start a Job by its ID.
// Example:
// 		c := New("http://127.0.0.1:8000")
//		id := "93b65499-b211-49ce-57e0-19e735cc5abd"
//		ok, err := c.StartJob(id)
func (kc *KalaClient) StartJob(id string) (bool, error) {
	_, err := kc.do(methodPost, kc.url(jobPath, "start", id), http.StatusNoContent, nil, nil)
	if err != nil {
		if errors.Is(err, ErrGenericError) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// GetKalaStats retrieves system-level metrics about Kala
// Example:
// 		c := New("http://127.0.0.1:8000")
//		stats, err := c.GetKalaStats()
func (kc *KalaClient) GetKalaStats() (*job.KalaStats, error) {
	ks := &api.KalaStatsResponse{}
	_, err := kc.do(methodGet, kc.url("stats"), http.StatusOK, nil, ks)
	return ks.Stats, err
}

// DisableJob is used to disable a Job in Kala using its ID.
// Example:
// 		c := New("http://127.0.0.1:8000")
//		id := "93b65499-b211-49ce-57e0-19e735cc5abd"
//		ok, err := c.DisableJob(id)
func (kc *KalaClient) DisableJob(id string) (bool, error) {
	status, err := kc.do(methodPost, kc.url(jobPath, "disable", id), http.StatusNoContent, nil, nil)
	if err != nil {
		if errors.Is(err, ErrGenericError) {
			return false, fmt.Errorf("Disable failed with a status code of %d", status)
		}
		return false, err
	}
	return true, nil
}

// EnableJob is used to enable a disabled Job in Kala using its ID.
// Example:
// 		c := New("http://127.0.0.1:8000")
//		id := "93b65499-b211-49ce-57e0-19e735cc5abd"
//		ok, err := c.EnableJob(id)
func (kc *KalaClient) EnableJob(id string) (bool, error) {
	status, err := kc.do(methodPost, kc.url(jobPath, "enable", id), http.StatusNoContent, nil, nil)
	if err != nil {
		if errors.Is(err, ErrGenericError) {
			return false, fmt.Errorf("Enable failed with a status code of %d", status)
		}
		return false, err
	}
	return true, nil
}
