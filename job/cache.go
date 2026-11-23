package job

import (
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cornelk/hashmap"
	log "github.com/sirupsen/logrus"
)

var (
	ErrJobDoesntExist = errors.New("The job you requested does not exist")
)

type JobCache interface {
	Get(id string) (*Job, error)
	GetAll() *JobsMap
	Set(j *Job) error
	Delete(id string) error
	Persist() error
	Enable(j *Job) error
	Disable(j *Job) error
}

type JobsMap struct {
	Jobs map[string]*Job
	Lock sync.RWMutex
}

func NewJobsMap() *JobsMap {
	return &JobsMap{
		Jobs: map[string]*Job{},
		Lock: sync.RWMutex{},
	}
}

type MemoryJobCache struct {
	// Jobs is a map from Job id's to pointers to the jobs.
	// Used as the main "data store" within this cache implementation.
	jobs           *JobsMap
	jobDB          JobDB
	PersistOnWrite bool

	// graph serialises creation with deletion, so a delete plan cannot race a
	// parent reference update from Job.Init.
	graph sync.Mutex
	// deletions holds tombstones and retained plans for deletes that committed
	// to storage but did not finish applying in memory.
	deletions deleteState
	// deleteApplyHook, when set by a test, runs after a delete's backend batch
	// commits and before the in-memory apply, so apply-phase failures can be
	// injected deterministically.
	deleteApplyHook func() error
}

func NewMemoryJobCache(jobDB JobDB) *MemoryJobCache {
	return &MemoryJobCache{
		jobs:  NewJobsMap(),
		jobDB: jobDB,
	}
}

func (c *MemoryJobCache) Start(persistWaitTime time.Duration) {
	if persistWaitTime == 0 {
		c.PersistOnWrite = true
	}

	// Prep cache
	allJobs, err := c.jobDB.GetAll()
	if err != nil {
		log.Fatal(err)
	}
	for _, j := range allJobs {
		if j.ShouldStartWaiting() {
			j.StartWaiting(c, false)
		}
		err = c.Set(j)
		if err != nil {
			log.Errorln(err)
		}
	}

	// Occasionally, save items in cache to db.
	if persistWaitTime > 0 {
		go c.PersistEvery(persistWaitTime)
	}

	// Process-level defer for shutting down the db.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		s := <-ch
		log.Infof("Process got signal: %s", s)
		log.Infof("Shutting down....")

		// Persist all jobs to database
		err = c.Persist()
		if err != nil {
			log.Errorln(err)
		}

		// Close the database
		c.jobDB.Close()

		os.Exit(0)
	}()
}

func (c *MemoryJobCache) Get(id string) (*Job, error) {
	if c.deletions.isDeleted(id) {
		return nil, ErrJobDoesntExist
	}

	c.jobs.Lock.RLock()
	defer c.jobs.Lock.RUnlock()

	j := c.jobs.Jobs[id]
	if j == nil {
		return nil, ErrJobDoesntExist
	}

	return j, nil
}

func (c *MemoryJobCache) GetAll() *JobsMap {
	return c.jobs
}

func (c *MemoryJobCache) Set(j *Job) error {
	if j == nil {
		return nil
	}

	// Serialise with delete so a run finishing around a delete cannot Save the
	// job back between the delete's commit and its tombstone.
	c.graph.Lock()
	defer c.graph.Unlock()

	c.jobs.Lock.Lock()
	defer c.jobs.Lock.Unlock()

	// A deleted job must never be written back: a run that started before the
	// delete finishes after it, and must not resurrect the job in storage.
	if c.deletions.isDeleted(j.Id) {
		return nil
	}

	if c.PersistOnWrite {
		if err := c.jobDB.Save(j); err != nil {
			return err
		}
	}

	c.jobs.Jobs[j.Id] = j
	return nil
}

// registerJobBatch writes a new job together with its parent link in one
// backend batch, refusing when the backend cannot commit it atomically.
func (c *MemoryJobCache) registerJobBatch(j *Job, parents []*Job) error {
	return registerCreatedJobBatch(c.jobDB, c.PersistOnWrite, c.mirrorCreatedJob, j, parents)
}

// mirrorCreatedJob copies an already-persisted job into the cache without
// writing to the backend again.
func (c *MemoryJobCache) mirrorCreatedJob(j *Job) {
	c.jobs.Lock.Lock()
	c.jobs.Jobs[j.Id] = j
	c.jobs.Lock.Unlock()
}

// Delete removes a job and repairs parent/child references as one synchronous
// operation. See delete.go for the phases and their guarantees.
func (c *MemoryJobCache) Delete(id string) error {
	return deleteJob(&c.graph, &c.deletions, c.memoryDeleteBackend(), id)
}

func (c *MemoryJobCache) memoryDeleteBackend() deleteBackend {
	return deleteBackend{
		db: c.jobDB,
		snapshot: func() map[string]*Job {
			return c.snapshotLocked()
		},
		applyUpdates: c.applyReferenceUpdates,
		removeJobs:   c.removeJobs,
		beforeApply:  c.deleteApplyHook,
	}
}

// SetDeleteApplyHook installs a deterministic apply-phase failure for tests.
// It returns a function that removes the hook.
func (c *MemoryJobCache) SetDeleteApplyHook(hook func() error) func() {
	c.deleteApplyHook = hook
	return func() { c.deleteApplyHook = nil }
}

// snapshotLocked copies the id→job map. The graph mutex is held by deleteJob,
// and the map lock is held only long enough to copy.
func (c *MemoryJobCache) snapshotLocked() map[string]*Job {
	c.jobs.Lock.RLock()
	defer c.jobs.Lock.RUnlock()
	out := make(map[string]*Job, len(c.jobs.Jobs))
	for id, j := range c.jobs.Jobs {
		out[id] = j
	}
	return out
}

// applyReferenceUpdates rewrites references on surviving jobs in place, under
// each job's own lock. The map read lock is held across the lookup so a
// concurrent Set cannot mutate the map underneath it.
func (c *MemoryJobCache) applyReferenceUpdates(updates []JobReferenceUpdate) {
	c.jobs.Lock.RLock()
	defer c.jobs.Lock.RUnlock()

	for _, u := range updates {
		j := c.jobs.Jobs[u.Id]
		if j == nil {
			continue
		}
		j.lock.Lock()
		j.ParentJobs = u.ParentJobs
		j.DependentJobs = u.DependentJobs
		j.lock.Unlock()
	}
}

func (c *MemoryJobCache) removeJobs(victims []string) {
	c.jobs.Lock.Lock()
	defer c.jobs.Lock.Unlock()
	for _, id := range victims {
		if j := c.jobs.Jobs[id]; j != nil {
			j.StopTimer()
		}
		delete(c.jobs.Jobs, id)
	}
}

func (c *MemoryJobCache) Enable(j *Job) error {
	return enable(j, c, c.PersistOnWrite)
}

// Disable stops a job from running by stopping its jobTimer. It also sets Job.Disabled to true,
// which is reflected in the UI.
func (c *MemoryJobCache) Disable(j *Job) error {
	return disable(j, c, c.PersistOnWrite)
}

func (c *MemoryJobCache) Persist() error {
	c.jobs.Lock.RLock()
	defer c.jobs.Lock.RUnlock()
	for _, j := range c.jobs.Jobs {
		if c.deletions.isDeleted(j.Id) {
			continue
		}
		err := c.jobDB.Save(j)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *MemoryJobCache) PersistEvery(persistWaitTime time.Duration) {
	wait := time.NewTicker(persistWaitTime).C
	var err error
	for {
		<-wait
		err = c.Persist()
		if err != nil {
			log.Errorf("Error occurred persisting the database. Err: %s", err)
		}
	}
}

type LockFreeJobCache struct {
	jobs            *hashmap.HashMap
	jobDB           JobDB
	retentionPeriod time.Duration
	PersistOnWrite  bool
	Clock

	// graph serialises creation with deletion. The job map itself stays
	// lock-free; this only guards multi-job graph changes.
	graph sync.Mutex
	// deletions holds tombstones and retained plans for deletes that committed
	// to storage but did not finish applying in memory.
	deletions deleteState
	// deleteApplyHook, when set by a test, runs after a delete's backend batch
	// commits and before the in-memory apply, so apply-phase failures can be
	// injected deterministically.
	deleteApplyHook func() error
}

func NewLockFreeJobCache(jobDB JobDB) *LockFreeJobCache {
	return &LockFreeJobCache{
		jobs:            hashmap.New(8), //nolint:gomnd
		jobDB:           jobDB,
		retentionPeriod: -1,
	}
}

func (c *LockFreeJobCache) Start(persistWaitTime time.Duration, jobstatTtl time.Duration) {
	if persistWaitTime == 0 {
		c.PersistOnWrite = true
	}

	// Prep cache
	allJobs, err := c.jobDB.GetAll()
	if err != nil {
		log.Fatal(err)
	}
	for _, j := range allJobs {
		if j.Schedule == "" {
			log.Infof("Job %s:%s skipped.", j.Name, j.Id)
			continue
		}
		if j.ShouldStartWaiting() {
			j.StartWaiting(c, false)
		}
		log.Infof("Job %s:%s added to cache.", j.Name, j.Id)
		err := c.Set(j)
		if err != nil {
			log.Errorln(err)
		}
	}
	// Occasionally, save items in cache to db.
	if persistWaitTime > 0 {
		go c.PersistEvery(persistWaitTime)
	}

	// Run retention every minute to clean up old job stats entries
	if jobstatTtl > 0 {
		c.retentionPeriod = jobstatTtl
		go c.RetainEvery(1 * time.Minute)
	}

	// Process-level defer for shutting down the db.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		s := <-ch
		log.Infof("Process got signal: %s", s)
		log.Infof("Shutting down....")

		// Persist all jobs to database
		log.Errorln(c.Persist())

		// Close the database
		c.jobDB.Close()

		os.Exit(0)
	}()
}

func (c *LockFreeJobCache) Get(id string) (*Job, error) {
	if c.deletions.isDeleted(id) {
		return nil, ErrJobDoesntExist
	}

	val, exists := c.jobs.GetStringKey(id)
	if val == nil || !exists {
		return nil, ErrJobDoesntExist
	}
	j := val.(*Job)
	if j == nil {
		return nil, ErrJobDoesntExist
	}
	return j, nil
}

func (c *LockFreeJobCache) GetAll() *JobsMap {
	jm := NewJobsMap()
	for el := range c.jobs.Iter() {
		id := el.Key.(string)
		if c.deletions.isDeleted(id) {
			continue
		}
		jm.Jobs[id] = el.Value.(*Job)
	}
	return jm
}

func (c *LockFreeJobCache) Set(j *Job) error {
	if j == nil {
		return nil
	}

	// Serialise with delete so a run finishing around a delete cannot Save the
	// job back between the delete's commit and its tombstone. Taking the graph
	// mutex here is cheap (Set is not on the hot run path in a way that
	// contends with deletes) and closes that window.
	c.graph.Lock()
	defer c.graph.Unlock()

	// A deleted job must never be written back: a run that started before the
	// delete finishes after it, and must not resurrect the job in storage.
	if c.deletions.isDeleted(j.Id) {
		return nil
	}

	if c.PersistOnWrite {
		if err := c.jobDB.Save(j); err != nil {
			return err
		}
	}

	c.jobs.Set(j.Id, j)
	return nil
}

// registerJobBatch writes a new job together with its parent link in one
// backend batch, refusing when the backend cannot commit it atomically.
func (c *LockFreeJobCache) registerJobBatch(j *Job, parents []*Job) error {
	return registerCreatedJobBatch(c.jobDB, c.PersistOnWrite, func(j *Job) { c.jobs.Set(j.Id, j) }, j, parents)
}

// registerCreatedJobBatch persists a newly created job and the parent
// reference updates as one backend batch, then mirrors both into the cache.
// Init has already validated the parents and holds the graph mutex.
//
// The parents' DependentJobs is mutated only after the batch commits, so a
// failed batch leaves no parent in memory naming a child that was never
// created. With persistOnWrite false the caller persists later via Persist,
// exactly as Set did before; the in-memory link is still applied afterwards so
// the code has a single mutation point.
func registerCreatedJobBatch(db JobDB, persistOnWrite bool, mirror func(*Job), j *Job, parents []*Job) error {
	type pendingUpdate struct {
		parent  *Job
		updated []string
	}
	pending := make([]pendingUpdate, 0, len(parents))
	updates := make([]JobReferenceUpdate, 0, len(parents))
	for _, parent := range parents {
		parent.lock.RLock()
		current := parent.DependentJobs
		next, changed := appendUniqueID(current, j.Id)
		update := JobReferenceUpdate{
			Id:            parent.Id,
			ParentJobs:    parent.ParentJobs,
			DependentJobs: next,
		}
		parent.lock.RUnlock()
		if changed {
			pending = append(pending, pendingUpdate{parent: parent, updated: next})
			updates = append(updates, update)
		}
	}

	if persistOnWrite {
		batcher, ok := db.(BatchJobDB)
		if !ok {
			return ErrAtomicBatchUnsupported
		}
		if err := batcher.ApplyBatch(JobBatch{Saves: []*Job{j}, Updates: updates}); err != nil {
			return err
		}
	}

	// Committed (or non-transactional): now apply the parent links in memory.
	for _, p := range pending {
		p.parent.lock.Lock()
		p.parent.DependentJobs = p.updated
		p.parent.lock.Unlock()
	}
	mirror(j)
	return nil
}

// Delete removes a job and repairs parent/child references as one synchronous
// operation. See delete.go for the phases and their guarantees.
func (c *LockFreeJobCache) Delete(id string) error {
	return deleteJob(&c.graph, &c.deletions, c.lockFreeDeleteBackend(), id)
}

func (c *LockFreeJobCache) lockFreeDeleteBackend() deleteBackend {
	return deleteBackend{
		db:           c.jobDB,
		snapshot:     c.snapshotLocked,
		applyUpdates: c.applyReferenceUpdates,
		removeJobs:   c.removeJobs,
		beforeApply:  c.deleteApplyHook,
	}
}

// SetDeleteApplyHook installs a deterministic apply-phase failure for tests.
// It returns a function that removes the hook.
func (c *LockFreeJobCache) SetDeleteApplyHook(hook func() error) func() {
	c.deleteApplyHook = hook
	return func() { c.deleteApplyHook = nil }
}

// snapshotLocked copies the id→job map. The graph mutex is held by deleteJob.
func (c *LockFreeJobCache) snapshotLocked() map[string]*Job {
	out := make(map[string]*Job)
	for el := range c.jobs.Iter() {
		out[el.Key.(string)] = el.Value.(*Job)
	}
	return out
}

// applyReferenceUpdates rewrites references on surviving jobs in place, under
// each job's own lock.
func (c *LockFreeJobCache) applyReferenceUpdates(updates []JobReferenceUpdate) {
	for _, u := range updates {
		val, ok := c.jobs.GetStringKey(u.Id)
		if !ok || val == nil {
			continue
		}
		j := val.(*Job)
		j.lock.Lock()
		j.ParentJobs = u.ParentJobs
		j.DependentJobs = u.DependentJobs
		j.lock.Unlock()
	}
}

func (c *LockFreeJobCache) removeJobs(victims []string) {
	for _, id := range victims {
		if val, ok := c.jobs.GetStringKey(id); ok && val != nil {
			val.(*Job).StopTimer()
		}
		c.jobs.Del(id)
	}
}

func (c *LockFreeJobCache) Enable(j *Job) error {
	return enable(j, c, c.PersistOnWrite)
}

// Disable stops a job from running by stopping its jobTimer. It also sets Job.Disabled to true,
// which is reflected in the UI.
func (c *LockFreeJobCache) Disable(j *Job) error {
	return disable(j, c, c.PersistOnWrite)
}

func (c *LockFreeJobCache) Persist() error {
	jm := c.GetAll()
	for _, j := range jm.Jobs {
		j.lock.RLock()
		err := c.jobDB.Save(j)
		if err != nil {
			j.lock.RUnlock()
			return err
		}
		j.lock.RUnlock()
	}
	return nil
}
func (c *LockFreeJobCache) PersistEvery(persistWaitTime time.Duration) {
	wait := time.NewTicker(persistWaitTime).C
	var err error
	for {
		<-wait
		err = c.Persist()
		if err != nil {
			log.Errorf("Error occurred persisting the database. Err: %s", err)
		}
	}
}

func (c *LockFreeJobCache) locateJobStatsIndexForRetention(stats []*JobStat) (marker int) {
	now := time.Now()
	expiresAt := now.Add(-c.retentionPeriod)
	pos := -1
	for i, el := range stats {
		diff := el.RanAt.Sub(expiresAt)
		if diff < 0 {
			pos = i
		}
	}
	return pos
}

func (c *LockFreeJobCache) Retain() error {
	for el := range c.jobs.Iter() {
		job := el.Value.(*Job)
		c.compactJobStats(job)
	}
	return nil
}

func (c *LockFreeJobCache) compactJobStats(job *Job) {
	job.lock.Lock()
	defer job.lock.Unlock()
	pos := c.locateJobStatsIndexForRetention(job.Stats)
	if pos >= 0 {
		log.Infof("JobStats TTL: removing %d items", pos+1)
		tmp := make([]*JobStat, len(job.Stats)-pos-1)
		copy(tmp, job.Stats[pos+1:])
		job.Stats = tmp
	}
}

func (c *LockFreeJobCache) RetainEvery(retentionWaitTime time.Duration) {
	wait := time.NewTicker(retentionWaitTime).C
	var err error
	for {
		<-wait
		err = c.Retain()
		if err != nil {
			log.Errorf("Error occurred during invoking retention. Err: %s", err)
		}
	}
}
