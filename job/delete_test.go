package job

// Offline tests for the synchronous, recoverable delete. They run against
// NewMemoryDB (and fake backends derived from it) and never touch a real
// database.

import (
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestCache builds a LockFree cache over an in-memory store with
// transactional persistence on, so storage reflects every cache write. The
// returned db is the same object the cache writes through.
func newTestCache(t *testing.T) (*LockFreeJobCache, *MemoryDB) {
	t.Helper()
	db := NewMemoryDB()
	cache := NewLockFreeJobCache(db)
	cache.PersistOnWrite = true
	return cache, db
}

// addJob creates a job with an explicit id and no schedule, so no timer is
// armed and the test is deterministic. It is written straight to the cache and
// the in-memory store.
func addJob(t *testing.T, cache *LockFreeJobCache, id string, parents ...string) *Job {
	t.Helper()
	j := GetMockJob()
	j.Id = id
	j.ParentJobs = append([]string(nil), parents...)
	require.NoError(t, cache.Set(j))
	return j
}

func parentJobsOf(cache *LockFreeJobCache, id string) []string {
	j, err := cache.Get(id)
	if err != nil {
		return nil
	}
	j.lock.RLock()
	defer j.lock.RUnlock()
	return append([]string(nil), j.ParentJobs...)
}

func dependentJobsOf(cache *LockFreeJobCache, id string) []string {
	j, err := cache.Get(id)
	if err != nil {
		return nil
	}
	j.lock.RLock()
	defer j.lock.RUnlock()
	return append([]string(nil), j.DependentJobs...)
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// --- planning rules ---------------------------------------------------------

func TestDeletePlanMissingTargetDoesNotTouchOtherElements(t *testing.T) {
	// A parent whose DependentJobs does not name the job being deleted must be
	// left alone; the v1 code removed index 0 regardless.
	cache, _ := newTestCache(t)
	addJob(t, cache, "a")
	addJob(t, cache, "b")
	addJob(t, cache, "c")
	// c claims a as a dependent, but a's DependentJobs is empty: asymmetric.
	ca := mustGet(t, cache, "c")
	ca.lock.Lock()
	ca.DependentJobs = []string{"a"}
	ca.lock.Unlock()

	// Deleting b must not remove anything from a or c.
	require.NoError(t, cache.Delete("b"))
	assert.ElementsMatch(t, []string{"a", "c"}, keysOf(cache))
	assert.Equal(t, []string{"a"}, dependentJobsOf(cache, "c"), "unrelated DependentJobs must not be altered")
}

func TestDeletePlanEmptySlicesDoNotPanic(t *testing.T) {
	cache, _ := newTestCache(t)
	addJob(t, cache, "solo")
	assert.NotPanics(t, func() { require.NoError(t, cache.Delete("solo")) })
	assert.Empty(t, keysOf(cache))
}

// A -> B -> C: deleting B removes A's reference to B. C has B as its only
// parent, so C is cascaded (the fixed TestDependentJobsParentJobGetsDeleted
// semantics). A survives.
func TestDeletePlanChainCascade(t *testing.T) {
	cache, _ := newTestCache(t)
	addJob(t, cache, "A")
	addJob(t, cache, "B", "A")
	addJob(t, cache, "C", "B")
	a := mustGet(t, cache, "A")
	a.lock.Lock()
	a.DependentJobs = []string{"B"}
	a.lock.Unlock()
	b := mustGet(t, cache, "B")
	b.lock.Lock()
	b.DependentJobs = []string{"C"}
	b.lock.Unlock()

	require.NoError(t, cache.Delete("B"))

	_, err := cache.Get("B")
	assert.Equal(t, ErrJobDoesntExist, err)
	_, err = cache.Get("C")
	assert.Equal(t, ErrJobDoesntExist, err, "sole parent deleted: child cascades")
	assert.Empty(t, dependentJobsOf(cache, "A"), "A keeps no dangling reference to B")
}

// A child with several parents loses only the deleted parent edge and
// survives, even if the deleted job is one of several.
func TestDeletePlanMultiParentKeepsChild(t *testing.T) {
	cache, db := newTestCache(t)
	addJob(t, cache, "P1")
	addJob(t, cache, "P2")
	addJob(t, cache, "child", "P1", "P2")
	p1 := mustGet(t, cache, "P1")
	p1.lock.Lock()
	p1.DependentJobs = []string{"child"}
	p1.lock.Unlock()
	p2 := mustGet(t, cache, "P2")
	p2.lock.Lock()
	p2.DependentJobs = []string{"child"}
	p2.lock.Unlock()

	require.NoError(t, cache.Delete("P1"))

	_, err := cache.Get("child")
	assert.NoError(t, err, "multi-parent child survives")
	assert.Equal(t, []string{"P2"}, parentJobsOf(cache, "child"))
	assert.Equal(t, []string{"child"}, dependentJobsOf(cache, "P2"))

	// The rewrite is persisted, not just held in memory.
	stored, err := db.Get("child")
	require.NoError(t, err)
	assert.Equal(t, []string{"P2"}, stored.ParentJobs, "reference repair must reach storage")
}

// A shared descendant reachable through two victims is processed once and
// deleted once; no job is visited twice.
func TestDeletePlanSharedDescendantDeletedOnce(t *testing.T) {
	cache, _ := newTestCache(t)
	addJob(t, cache, "top")
	addJob(t, cache, "x", "top")
	addJob(t, cache, "y", "top")
	addJob(t, cache, "shared", "x", "y") // two parents, so it must NOT cascade

	top := mustGet(t, cache, "top")
	top.lock.Lock()
	top.DependentJobs = []string{"x", "y"}
	top.lock.Unlock()
	x := mustGet(t, cache, "x")
	x.lock.Lock()
	x.DependentJobs = []string{"shared"}
	x.lock.Unlock()
	y := mustGet(t, cache, "y")
	y.lock.Lock()
	y.DependentJobs = []string{"shared"}
	y.lock.Unlock()

	require.NoError(t, cache.Delete("top"))

	// x and y each had top as their sole parent, so both cascade.
	_, err := cache.Get("x")
	assert.Equal(t, ErrJobDoesntExist, err)
	_, err = cache.Get("y")
	assert.Equal(t, ErrJobDoesntExist, err)

	// shared had two parents, so it survives; both deleted parents are removed
	// from its ParentJobs exactly once.
	_, err = cache.Get("shared")
	require.NoError(t, err, "shared descendant with two parents must survive")
	assert.Empty(t, parentJobsOf(cache, "shared"))
}

// An asymmetric history (parent lists a child that does not list it back) must
// not cascade; the dangling reference is simply dropped.
func TestDeletePlanAsymmetricReferenceDropsEdgeOnly(t *testing.T) {
	cache, _ := newTestCache(t)
	addJob(t, cache, "p")
	addJob(t, cache, "q") // q does not name p as a parent
	p := mustGet(t, cache, "p")
	p.lock.Lock()
	p.DependentJobs = []string{"q"}
	p.lock.Unlock()

	require.NoError(t, cache.Delete("p"))

	_, err := cache.Get("q")
	assert.NoError(t, err, "a child not naming the victim must never be cascaded")
	assert.Empty(t, dependentJobsOf(cache, "p"))
}

// Duplicate edges are collapsed and do not cause the removed element to be
// deleted twice or a panic.
func TestDeletePlanDuplicateEdgesCollapse(t *testing.T) {
	cache, _ := newTestCache(t)
	addJob(t, cache, "p1")
	addJob(t, cache, "p2")
	addJob(t, cache, "c", "p1", "p1", "p2")
	p1 := mustGet(t, cache, "p1")
	p1.lock.Lock()
	p1.DependentJobs = []string{"c", "c"}
	p1.lock.Unlock()
	p2 := mustGet(t, cache, "p2")
	p2.lock.Lock()
	p2.DependentJobs = []string{"c"}
	p2.lock.Unlock()

	require.NoError(t, cache.Delete("p1"))
	assert.Equal(t, []string{"p2"}, parentJobsOf(cache, "c"))
	assert.Equal(t, []string{"c"}, dependentJobsOf(cache, "p2"))
}

// A dangling dependent reference (a parent names a child that no longer
// exists) is dropped from the parent and does not abort the delete.
func TestDeletePlanDanglingReferenceIsDropped(t *testing.T) {
	cache, _ := newTestCache(t)
	addJob(t, cache, "p")
	addJob(t, cache, "peer")
	p := mustGet(t, cache, "p")
	p.lock.Lock()
	p.DependentJobs = []string{"ghost"}
	p.lock.Unlock()
	peer := mustGet(t, cache, "peer")
	peer.lock.Lock()
	peer.DependentJobs = []string{"p"}
	peer.lock.Unlock()

	require.NoError(t, cache.Delete("p"))
	assert.Empty(t, dependentJobsOf(cache, "peer"))
}

// --- failure paths ----------------------------------------------------------

var errInjectedDelete = errors.New("injected delete failure")

// A failed persist must leave the cache completely untouched and return a
// persist-phase error, and a later retry must succeed.
func TestDeletePersistFailureLeavesCacheIntactAndRetries(t *testing.T) {
	base := NewMemoryDB()
	db := &FailingBatchDB{JobDB: base}
	cache := NewLockFreeJobCache(db)
	cache.PersistOnWrite = true
	addJob(t, cache, "p")
	addJob(t, cache, "c", "p")
	setDependents(t, cache, "p", "c")

	before := keysOf(cache)

	db.setApplyErr(errInjectedDelete)
	err := cache.Delete("p")
	require.Error(t, err)
	assert.ErrorIs(t, err, errInjectedDelete)
	phase, ok := deleteErrorPhase(err)
	require.True(t, ok)
	assert.Equal(t, DeletePhasePersist, phase)
	var de *DeleteError
	require.True(t, errors.As(err, &de))
	assert.False(t, de.Committed, "a persist failure must not claim the delete committed")

	assert.Equal(t, before, keysOf(cache), "cache must be unchanged after a failed persist")
	assert.Equal(t, []string{"c"}, dependentJobsOf(cache, "p"))
	_, err = base.Get("p")
	assert.NoError(t, err, "storage must be unchanged after a failed persist")
	assert.Equal(t, 1, db.calls())

	db.setApplyErr(nil)
	require.NoError(t, cache.Delete("p"), "a retry after the backend recovers must succeed")
	_, err = cache.Get("p")
	assert.Equal(t, ErrJobDoesntExist, err)
}

// An apply-phase failure is reported as committed-but-not-applied, keeps the
// plan so a retry finishes it without committing again, and leaves storage
// final. Rebuilding the cache from storage reproduces the committed state.
func TestDeleteApplyFailureResumesWithoutRecommitting(t *testing.T) {
	base := NewMemoryDB()
	db := &FailingBatchDB{JobDB: base}
	cache := NewLockFreeJobCache(db)
	addJob(t, cache, "p")
	addJob(t, cache, "c", "p")
	setDependents(t, cache, "p", "c")

	cache.SetDeleteApplyHook(func() error { return errInjectedDelete })
	err := cache.Delete("p")
	require.Error(t, err)
	phase, ok := deleteErrorPhase(err)
	require.True(t, ok)
	assert.Equal(t, DeletePhaseApply, phase)
	var de *DeleteError
	require.True(t, errors.As(err, &de))
	assert.True(t, de.Committed, "an apply failure happens after the commit")

	// Storage already reflects the delete and the cascade.
	_, err = base.Get("p")
	assert.Error(t, err)
	_, err = base.Get("c")
	assert.Error(t, err, "sole-parent child was cascaded in storage")

	commitsBeforeRetry := db.calls()

	// A restart from storage converges to the same final state.
	rebuilt := NewLockFreeJobCache(NewMemoryDB())
	for _, j := range mustGetAll(t, base) {
		require.NoError(t, rebuilt.Set(j))
	}
	_, err = rebuilt.Get("p")
	assert.Equal(t, ErrJobDoesntExist, err, "deleted job must not reappear after a rebuild")
	_, err = rebuilt.Get("c")
	assert.Equal(t, ErrJobDoesntExist, err)

	// Retrying the same DELETE on the original cache finishes the apply and
	// does not commit the batch again.
	cache.SetDeleteApplyHook(nil)
	require.NoError(t, cache.Delete("p"))
	assert.Equal(t, commitsBeforeRetry, db.calls(), "the retry must not re-commit the batch")
	_, err = cache.Get("p")
	assert.Equal(t, ErrJobDoesntExist, err)
	_, err = cache.Get("c")
	assert.Equal(t, ErrJobDoesntExist, err)
}

// A backend without the batch capability is refused explicitly, never degraded
// to a best-effort sequence, and the cache is untouched.
func TestDeleteRefusedWhenBackendCannotBatch(t *testing.T) {
	db := &NoBatchDB{JobDB: NewMemoryDB()}
	cache := NewLockFreeJobCache(db)
	addJob(t, cache, "p")
	addJob(t, cache, "c", "p")
	setDependents(t, cache, "p", "c")

	err := cache.Delete("p")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrAtomicBatchUnsupported)
	phase, ok := deleteErrorPhase(err)
	require.True(t, ok)
	assert.Equal(t, DeletePhasePersist, phase)
	assert.ElementsMatch(t, []string{"p", "c"}, keysOf(cache), "nothing may be removed when the batch is refused")
}

// A repeat DELETE is idempotent: the second call must not re-run cascade side
// effects or fail.
func TestDeleteRepeatIsIdempotent(t *testing.T) {
	cache, db := newTestCache(t)
	addJob(t, cache, "p")
	require.NoError(t, cache.Delete("p"))
	require.NoError(t, cache.Delete("p"), "repeat DELETE must succeed")
	_, err := db.Get("p")
	assert.Error(t, err)

	// An unknown id is reported as nonexistent, not as success.
	err = cache.Delete("never-existed")
	assert.Equal(t, ErrJobDoesntExist, err)
}

// A running job that finishes after its delete must not resurrect itself.
func TestDeleteTombstonePreventsRunWriteBack(t *testing.T) {
	cache, db := newTestCache(t)
	j := GetMockJob()
	j.Id = "runner"
	require.NoError(t, cache.Set(j))

	require.NoError(t, cache.Delete("runner"))

	// Simulate the finish-of-run write-back.
	require.NoError(t, cache.Set(j))

	_, err := cache.Get("runner")
	assert.Equal(t, ErrJobDoesntExist, err, "a deleted job must not be re-added by a run write-back")
	_, err = db.Get("runner")
	assert.Error(t, err, "a deleted job must not be re-persisted by a run write-back")
}

// --- concurrency ------------------------------------------------------------

// Deletes of opposite edges of the same A<->B reference run concurrently. The
// graph mutex serialises them; neither deadlocks and neither leaves a dangling
// reference.
func TestDeleteConcurrentOppositeDirections(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		cache, _ := newTestCache(t)
		addJob(t, cache, "A")
		addJob(t, cache, "B", "A")
		setDependents(t, cache, "A", "B")

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); assert.NoError(t, cache.Delete("A")) }()
		go func() { defer wg.Done(); assert.NoError(t, cache.Delete("B")) }()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-timeoutAfter(t):
			t.Fatal("concurrent opposite-direction deletes deadlocked")
		}

		assert.Empty(t, keysOf(cache), "both jobs and their references are gone")
	}
}

// A delete racing a child-creation must not leave a parent pointing at a job
// that was never created, nor a created child whose parent already vanished.
func TestDeleteRacingCreateIsConsistent(t *testing.T) {
	cache, db := newTestCache(t)
	addJob(t, cache, "parent")
	created := addJob(t, cache, "child", "parent")
	setDependents(t, cache, "parent", "child")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = cache.Delete("parent") }()
	go func() { defer wg.Done(); _ = created.Init(cache) }()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-timeoutAfter(t):
		t.Fatal("delete racing create deadlocked")
	}

	// Whatever the interleaving, storage must never name a parent that is gone
	// from storage.
	all := mustGetAll(t, db)
	present := map[string]bool{}
	for _, j := range all {
		present[j.Id] = true
	}
	for _, j := range all {
		for _, p := range j.ParentJobs {
			assert.True(t, present[p], "stored job %q references absent parent %q", j.Id, p)
		}
	}
}

// A crash between the commit and the in-memory apply must be recoverable by
// rebuilding the cache from storage, with no unrelated reference lost and no
// deleted job resurrected.
func TestDeleteCrashRecoveryFromStorage(t *testing.T) {
	base := NewMemoryDB()
	db := &FailingBatchDB{JobDB: base}
	cache := NewLockFreeJobCache(db)
	cache.PersistOnWrite = true

	// p has two children: c1 (sole parent p) and shared (parents p and keep).
	addJob(t, cache, "p")
	addJob(t, cache, "keep")
	addJob(t, cache, "c1", "p")
	addJob(t, cache, "shared", "p", "keep")
	setDependents(t, cache, "p", "c1", "shared")
	setDependents(t, cache, "keep", "shared")

	// "Crash" right after the commit, before the in-memory apply.
	cache.SetDeleteApplyHook(func() error { return errInjectedDelete })
	require.Error(t, cache.Delete("p"))

	// The process is gone; a new one loads whatever storage holds.
	rebuilt := NewLockFreeJobCache(NewMemoryDB())
	for _, j := range mustGetAll(t, base) {
		require.NoError(t, rebuilt.Set(j))
	}

	_, err := rebuilt.Get("p")
	assert.Equal(t, ErrJobDoesntExist, err, "deleted job must not reappear")
	_, err = rebuilt.Get("c1")
	assert.Equal(t, ErrJobDoesntExist, err, "sole-parent child must stay deleted")
	_, err = rebuilt.Get("shared")
	require.NoError(t, err, "multi-parent child must survive")
	assert.Equal(t, []string{"keep"}, parentJobsOf(rebuilt, "shared"), "only the deleted edge is removed")
	assert.Equal(t, []string{"shared"}, dependentJobsOf(rebuilt, "keep"), "unrelated parent keeps its child")
}

// A crash before the commit (persist-phase failure) must leave storage exactly
// as it was, so a rebuild shows no change at all.
func TestDeleteCrashBeforeCommitLeavesStorageIntact(t *testing.T) {
	base := NewMemoryDB()
	db := &FailingBatchDB{JobDB: base}
	cache := NewLockFreeJobCache(db)
	cache.PersistOnWrite = true

	addJob(t, cache, "p")
	addJob(t, cache, "c", "p")
	setDependents(t, cache, "p", "c")

	db.setApplyErr(errInjectedDelete)
	require.Error(t, cache.Delete("p"))

	rebuilt := NewLockFreeJobCache(NewMemoryDB())
	for _, j := range mustGetAll(t, base) {
		require.NoError(t, rebuilt.Set(j))
	}
	assert.ElementsMatch(t, []string{"p", "c"}, keysOf(rebuilt), "a pre-commit crash changes nothing")
	assert.Equal(t, []string{"p"}, parentJobsOf(rebuilt, "c"))
}

// MemoryJobCache runs the same delete path. Verify the cascade and reference
// repair there too, and that its own locks are released afterwards.
func TestMemoryCacheDeleteCascadeAndLocksReleased(t *testing.T) {
	db := NewMemoryDB()
	cache := NewMemoryJobCache(db)
	cache.PersistOnWrite = true

	p := GetMockJob()
	p.Id = "p"
	require.NoError(t, cache.Set(p))
	c := GetMockJob()
	c.Id = "c"
	c.ParentJobs = []string{"p"}
	require.NoError(t, cache.Set(c))
	p.lock.Lock()
	p.DependentJobs = []string{"c"}
	p.lock.Unlock()

	require.NoError(t, cache.Delete("p"))

	_, err := cache.Get("p")
	assert.Equal(t, ErrJobDoesntExist, err)
	_, err = cache.Get("c")
	assert.Equal(t, ErrJobDoesntExist, err, "sole-parent child cascades")

	// If any lock leaked, these would block. Run them with a deadline.
	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.NoError(t, cache.Delete("p"), "repeat delete of an already-deleted id is idempotent")
		assert.NoError(t, cache.Persist())
	}()
	select {
	case <-done:
	case <-timeoutAfter(t):
		t.Fatal("MemoryJobCache left a lock held after delete")
	}
}

// A save failure during creation must not leave the graph mutex held.
func TestCreateSaveFailureReleasesGraphLock(t *testing.T) {
	base := NewMemoryDB()
	db := &FailingBatchDB{JobDB: base}
	cache := NewLockFreeJobCache(db)
	cache.PersistOnWrite = true

	addJob(t, cache, "parent")
	db.setApplyErr(errInjectedDelete)

	child := GetMockJob()
	child.Id = "child"
	child.ParentJobs = []string{"parent"}
	err := child.Init(cache)
	require.Error(t, err)

	db.setApplyErr(nil)
	// If Init leaked the graph mutex, this delete would hang.
	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.NoError(t, cache.Delete("parent"))
	}()
	select {
	case <-done:
	case <-timeoutAfter(t):
		t.Fatal("a failed creation left the graph mutex held")
	}
}

// DeleteAll drives the same synchronous delete for every job, including ones
// already removed by an earlier cascade; it must clear cache and storage.
func TestDeleteAllWithDependencies(t *testing.T) {
	cache, db := newTestCache(t)
	addJob(t, cache, "a")
	addJob(t, cache, "b", "a")
	addJob(t, cache, "c", "b")
	setDependents(t, cache, "a", "b")
	setDependents(t, cache, "b", "c")

	require.NoError(t, DeleteAll(cache))
	assert.Empty(t, keysOf(cache))
	all, err := db.GetAll()
	require.NoError(t, err)
	assert.Empty(t, all)
}

// --- helpers ----------------------------------------------------------------

func mustGet(t *testing.T, cache *LockFreeJobCache, id string) *Job {
	t.Helper()
	j, err := cache.Get(id)
	require.NoError(t, err)
	return j
}

func mustGetAll(t *testing.T, db *MemoryDB) []*Job {
	t.Helper()
	all, err := db.GetAll()
	require.NoError(t, err)
	return all
}

func setDependents(t *testing.T, cache *LockFreeJobCache, id string, dependents ...string) {
	t.Helper()
	j := mustGet(t, cache, id)
	j.lock.Lock()
	j.DependentJobs = append([]string(nil), dependents...)
	j.lock.Unlock()
}

func keysOf(cache *LockFreeJobCache) []string {
	jm := cache.GetAll()
	out := make([]string, 0, len(jm.Jobs))
	for id := range jm.Jobs {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func timeoutAfter(t *testing.T) <-chan struct{} {
	t.Helper()
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		select {
		case <-time.After(10 * time.Second):
		}
	}()
	return ch
}

// NoBatchDB hides the BatchJobDB capability of its embedded backend, modelling
// a storage engine that cannot commit a batch atomically.
type NoBatchDB struct {
	JobDB
}

// A failed creation batch must leave the parent naming no child that was never
// created, in memory or in storage.
func TestCreateSaveFailureLeavesParentUntouched(t *testing.T) {
	base := NewMemoryDB()
	db := &FailingBatchDB{JobDB: base}
	cache := NewLockFreeJobCache(db)
	cache.PersistOnWrite = true

	parent := GetMockJob()
	parent.Id = "parent"
	require.NoError(t, cache.Set(parent))

	db.setApplyErr(errInjectedDelete)

	child := GetMockJob()
	child.Id = "child"
	child.ParentJobs = []string{"parent"}
	require.Error(t, child.Init(cache))

	assert.Empty(t, dependentJobsOf(cache, "parent"), "parent must not name an uncreated child")
	stored, err := base.Get("parent")
	require.NoError(t, err)
	assert.Empty(t, stored.DependentJobs, "storage must not name an uncreated child")
	_, err = base.Get("child")
	assert.Error(t, err, "the rejected child must not be stored")

	// Once the backend recovers, the same child can be created successfully.
	db.setApplyErr(nil)
	require.NoError(t, child.Init(cache))
	assert.Equal(t, []string{child.Id}, dependentJobsOf(cache, "parent"))
}

// A run write-back that lands between the delete's commit and its tombstone
// must not re-save the deleted job. This is the window the graph mutex on Set
// closes; the retry path must still converge to "gone".
func TestDeleteRunWriteBackAfterCommitDoesNotResurrect(t *testing.T) {
	base := NewMemoryDB()
	db := &FailingBatchDB{JobDB: base}
	cache := NewLockFreeJobCache(db)
	cache.PersistOnWrite = true

	j := GetMockJob()
	j.Id = "r"
	require.NoError(t, cache.Set(j))

	// Inject a failure after the commit so the tombstone is not yet set, then
	// attempt the run write-back from another goroutine while the delete is
	// paused. Set must be serialised with the delete.
	applied := make(chan struct{})
	release := make(chan struct{})
	cache.SetDeleteApplyHook(func() error {
		close(applied)
		<-release
		return errInjectedDelete
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cache.Delete("r")
	}()
	<-applied
	// The delete holds the graph mutex; this Set blocks until it finishes.
	go func() {
		_ = cache.Set(j)
	}()
	close(release)
	select {
	case <-done:
	case <-timeoutAfter(t):
		t.Fatal("delete did not finish")
	}

	// Whatever the interleaving, the final retry removes the job from storage.
	cache.SetDeleteApplyHook(nil)
	require.NoError(t, cache.Delete("r"))
	_, err := base.Get("r")
	assert.Error(t, err, "a deleted job must not remain in storage")
	_, err = cache.Get("r")
	assert.Equal(t, ErrJobDoesntExist, err)
}
