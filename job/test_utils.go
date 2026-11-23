package job

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ajvb/kala/utils/iso8601"
)

type MockDBGetAll struct {
	MockDB
	response []*Job
}

func (d *MockDBGetAll) GetAll() ([]*Job, error) {
	return d.response, nil
}

// MockDB is an in-memory, always-succeeding JobDB used by the API tests and by
// --no-persist mode. It implements the atomic batch contract.
type MockDB struct {
	lock   sync.Mutex
	stored map[string]*Job
}

func (m *MockDB) GetAll() ([]*Job, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	out := make([]*Job, 0, len(m.stored))
	for _, j := range m.stored {
		out = append(out, j)
	}
	return out, nil
}
func (m *MockDB) Get(id string) (*Job, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	j, ok := m.stored[id]
	if !ok {
		return nil, ErrJobNotFound(id)
	}
	return j, nil
}
func (m *MockDB) Delete(id string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.stored, id)
	return nil
}
func (m *MockDB) Save(job *Job) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.stored == nil {
		m.stored = map[string]*Job{}
	}
	m.stored[job.Id] = job
	return nil
}
func (m *MockDB) Close() error {
	return nil
}

// ApplyBatch applies the whole batch under one lock. Nothing is written if the
// batch cannot be applied (this implementation never fails mid-batch), so the
// all-or-nothing contract holds.
func (m *MockDB) ApplyBatch(batch JobBatch) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.stored == nil {
		m.stored = map[string]*Job{}
	}
	for _, j := range batch.Saves {
		m.stored[j.Id] = j
	}
	for _, id := range batch.Deletes {
		delete(m.stored, id)
	}
	for _, u := range batch.Updates {
		if j, ok := m.stored[u.Id]; ok {
			j.ParentJobs = u.ParentJobs
			j.DependentJobs = u.DependentJobs
		}
	}
	return nil
}

var _ BatchJobDB = (*MockDB)(nil)

func NewMockCache() *LockFreeJobCache {
	return NewLockFreeJobCache(&MockDB{})
}

func GetMockJob() *Job {
	return &Job{
		Name:    "mock_job",
		Command: "bash -c 'date'",
		Owner:   "example@example.com",
		Retries: 2,
	}
}

func GetMockFailingJob() *Job {
	return &Job{
		Name:    "mock_failing_job",
		Command: "asdf",
		Owner:   "example@example.com",
		Retries: 2,
	}
}

func GetMockRemoteJob(props RemoteProperties) *Job {
	return &Job{
		Name:             "mock_remote_job",
		Command:          "",
		JobType:          RemoteJob,
		RemoteProperties: props,
	}
}

func GetMockJobWithSchedule(repeat int, scheduleTime time.Time, delay string) *Job {
	genericMockJob := GetMockJob()

	parsedTime := scheduleTime.Format(time.RFC3339)
	scheduleStr := fmt.Sprintf("R%d/%s/%s", repeat, parsedTime, delay)
	genericMockJob.Schedule = scheduleStr

	return genericMockJob
}

func GetMockRecurringJobWithSchedule(scheduleTime time.Time, delay string) *Job {
	genericMockJob := GetMockJob()

	parsedTime := scheduleTime.Format(time.RFC3339)
	scheduleStr := fmt.Sprintf("R/%s/%s", parsedTime, delay)
	genericMockJob.Schedule = scheduleStr
	genericMockJob.timesToRepeat = -1

	parsedDuration, _ := iso8601.FromString(delay)

	genericMockJob.delayDuration = parsedDuration

	return genericMockJob
}

func GetMockJobStats(oldDate time.Time, count int) []*JobStat {
	stats := make([]*JobStat, 0)
	for i := 1; i <= count; i++ {
		el := &JobStat{
			JobId:             "stats-id-" + fmt.Sprint(i),
			NumberOfRetries:   0,
			ExecutionDuration: 10000,
			Success:           true,
			RanAt:             oldDate,
		}
		stats = append(stats, el)
	}
	return stats
}

func GetMockJobWithGenericSchedule(now time.Time) *Job {
	fiveMinutesFromNow := now.Add(time.Minute * 5)
	return GetMockJobWithSchedule(2, fiveMinutesFromNow, "P1DT10M10S")
}

func parseTime(t *testing.T, value string) time.Time {
	now, err := time.Parse("2006-Jan-02 15:04", value)
	if err != nil {
		t.Fatal(err)
	}
	return now
}

func parseTimeInLocation(t *testing.T, value string, location string) time.Time {
	loc, err := time.LoadLocation(location)
	if err != nil {
		t.Fatal(err)
	}
	now, err := time.ParseInLocation("2006-Jan-02 15:04", value, loc)
	if err != nil {
		t.Fatal(err)
	}
	return now
}

// Used to hand off execution briefly so that jobs can run and so on.
func briefPause() {
	time.Sleep(time.Millisecond * 100)
}

func awaitJobRan(t *testing.T, j *Job, timeout time.Duration) {
	t.Helper()
	select {
	case <-j.ranChan:
	case <-time.After(timeout):
		t.Fatal("Job failed to run")
	}
}

var _ JobDB = (*MemoryDB)(nil)

type MemoryDB struct {
	m    map[string]*Job
	lock sync.RWMutex
}

func NewMemoryDB() *MemoryDB {
	return &MemoryDB{
		m: map[string]*Job{},
	}
}

func (m *MemoryDB) GetAll() (ret []*Job, _ error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, v := range m.m {
		ret = append(ret, v)
	}
	return
}

func (m *MemoryDB) Get(id string) (*Job, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	j, exist := m.m[id]
	if !exist {
		return nil, ErrJobNotFound(id)
	}
	return j, nil
}

func (m *MemoryDB) Delete(id string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if _, exists := m.m[id]; !exists {
		return errors.New("Doesn't exist") // Used for testing
	}
	delete(m.m, id)
	// log.Printf("After delete: %+v", m)
	return nil
}

func (m *MemoryDB) Save(j *Job) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.m[j.Id] = j
	return nil
}

// ApplyBatch applies the whole batch under one lock; the in-memory map cannot
// fail part-way, so the all-or-nothing contract holds.
func (m *MemoryDB) ApplyBatch(batch JobBatch) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, j := range batch.Saves {
		m.m[j.Id] = j
	}
	for _, id := range batch.Deletes {
		delete(m.m, id)
	}
	for _, u := range batch.Updates {
		if j, ok := m.m[u.Id]; ok {
			j.ParentJobs = u.ParentJobs
			j.DependentJobs = u.DependentJobs
		}
	}
	return nil
}

var _ BatchJobDB = (*MemoryDB)(nil)

// FailingBatchDB wraps a JobDB whose batch commits can be made to fail. It is
// used to prove that a delete which cannot be committed leaves the cache
// untouched, and that a repeat delete can succeed afterwards.
type FailingBatchDB struct {
	JobDB
	// ApplyErr, when non-nil, is returned by ApplyBatch and no write happens.
	ApplyErr error
	// Calls counts ApplyBatch invocations.
	Calls int
	// Applied records the batches that were committed.
	Applied []JobBatch
	lock    sync.Mutex
}

func (f *FailingBatchDB) ApplyBatch(batch JobBatch) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.Calls++
	if f.ApplyErr != nil {
		return f.ApplyErr
	}
	batcher, ok := f.JobDB.(BatchJobDB)
	if !ok {
		return ErrAtomicBatchUnsupported
	}
	if err := batcher.ApplyBatch(batch); err != nil {
		return err
	}
	f.Applied = append(f.Applied, batch)
	return nil
}

func (f *FailingBatchDB) setApplyErr(err error) {
	f.lock.Lock()
	f.ApplyErr = err
	f.lock.Unlock()
}

func (f *FailingBatchDB) calls() int {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.Calls
}

var _ BatchJobDB = (*FailingBatchDB)(nil)

func (m *MemoryDB) Close() error {
	return nil
}
