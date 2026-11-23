package job

// Deleting a job and repairing parent/child references is one synchronous,
// recoverable operation:
//
//  1. The cache's graph mutex is taken. Job.Init (creation plus the parent
//     reference update) and this delete both take it, so a delete plan can
//     never be built against references a concurrent create already changed.
//  2. A plan is built from a snapshot: the jobs to delete, the surviving jobs
//     whose ParentJobs / DependentJobs must be rewritten, and any anomalies in
//     the stored graph. Building changes nothing.
//  3. The whole plan is handed to the backend as one JobBatch. Backends that
//     can commit it atomically implement BatchJobDB. Any other backend makes
//     the delete fail with ErrAtomicBatchUnsupported rather than degrade to a
//     best-effort sequence.
//  4. Only after the backend confirmed the batch is the cache changed: deleted
//     jobs are tombstoned, surviving references are rewritten, timers of
//     deleted jobs are stopped and the jobs are removed from the cache.
//
// The backend batch is the commit point. If step 3 fails the cache is
// untouched and a *DeleteError with Phase=DeletePhasePersist is returned. If
// the in-memory apply in step 4 panics, storage is already final: the plan is
// retained, the call returns a *DeleteError with Phase=DeletePhaseApply and
// Committed=true, and a repeat DELETE of the same id completes the retained
// apply without committing anything again. Rebuilding the cache from storage
// is also safe because storage already holds the final state.

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	log "github.com/sirupsen/logrus"
)

// DeletePhase names the stage at which a delete stopped.
type DeletePhase string

const (
	// DeletePhasePersist means the backend batch was not committed; both the
	// cache and storage are unchanged.
	DeletePhasePersist DeletePhase = "persist"
	// DeletePhaseApply means the backend batch was committed but the in-memory
	// apply did not finish. Storage is final.
	DeletePhaseApply DeletePhase = "apply"
)

// DeleteError reports a delete that did not complete, and what it left behind.
type DeleteError struct {
	// Id is the job whose deletion was requested.
	Id string
	// Phase is the stage that failed.
	Phase DeletePhase
	// Committed reports whether the backend batch reached storage.
	Committed bool
	// Err is the underlying cause.
	Err error
}

func (e *DeleteError) Error() string {
	state := "not committed; cache and storage unchanged"
	if e.Committed {
		state = "committed to storage; in-memory apply incomplete"
	}
	return fmt.Sprintf("delete of job %s failed at phase %s (%s): %v", e.Id, e.Phase, state, e.Err)
}

func (e *DeleteError) Unwrap() error { return e.Err }

// JobReferenceUpdate rewrites the two reference lists of one surviving job.
type JobReferenceUpdate struct {
	Id            string
	ParentJobs    []string
	DependentJobs []string
}

// JobBatch is the complete set of writes one graph change needs.
//
// ApplyBatch must apply the batch entirely or not at all. On error the caller
// assumes neither the deletes, saves nor updates took effect. A backend that
// cannot promise that must not implement BatchJobDB.
type JobBatch struct {
	// Deletes are job ids to remove. Removing an absent id succeeds, so a
	// retried batch is idempotent.
	Deletes []string
	// Saves insert or replace whole jobs (used when creating a job).
	Saves []*Job
	// Updates rewrite reference lists of surviving jobs. An update for an
	// absent job is skipped, and a delete always wins over an update for the
	// same id.
	Updates []JobReferenceUpdate
}

// BatchJobDB is implemented by backends able to commit a JobBatch atomically.
type BatchJobDB interface {
	ApplyBatch(batch JobBatch) error
}

// deletePlan is the immutable result of planning one delete. Planning reads
// the jobs but never mutates them.
type deletePlan struct {
	target    string
	victims   []string
	updates   []JobReferenceUpdate
	anomalies []string
}

func (p *deletePlan) batch() JobBatch {
	return JobBatch{Deletes: append([]string(nil), p.victims...), Updates: p.updates}
}

func (p *deletePlan) contains(id string) bool {
	for _, v := range p.victims {
		if v == id {
			return true
		}
	}
	return false
}

// planDelete computes which jobs a delete removes and which references it
// repairs.
//
// Cascade rule: a job is deleted only when a victim's DependentJobs names it as
// a child AND that child's ParentJobs names exactly that victim as its sole
// parent. Everything else is a reference repair, never a wider cascade:
//
//   - a multi-parent child loses the victim from ParentJobs and survives, even
//     when every parent is in the same batch;
//   - a job named as a dependent but absent from the cache is reported as an
//     anomaly and is not cascaded;
//   - a job whose ParentJobs does not name the victim (asymmetric history) is
//     not cascaded, whatever else it references;
//   - duplicate ids are collapsed, and dangling references are left alone.
//
// Each id is visited once, so a shared descendant is neither processed twice
// nor deleted through two paths.
func planDelete(snapshot map[string]*Job, target string) *deletePlan {
	p := &deletePlan{target: target}

	victimSet := map[string]struct{}{target: {}}
	queue := []string{target}

	for len(queue) > 0 {
		v := queue[0]
		queue = queue[1:]

		vj := snapshot[v]
		if vj == nil {
			continue
		}
		for _, childID := range uniqueStrings(vj.DependentJobs) {
			if childID == "" {
				p.anomaly("job %q lists an empty dependent id", v)
				continue
			}
			if _, already := victimSet[childID]; already {
				continue
			}
			child := snapshot[childID]
			if child == nil {
				p.anomaly("dependent %q of deleted job %q does not exist; not cascading", childID, v)
				continue
			}
			parents := uniqueStrings(child.ParentJobs)
			if len(parents) == 1 && parents[0] == v {
				victimSet[childID] = struct{}{}
				queue = append(queue, childID)
				continue
			}
			if !containsString(parents, v) {
				p.anomaly("job %q lists dependent %q, but that job does not name %q as a parent; not cascading",
					v, childID, v)
			}
		}
	}

	p.victims = sortedKeys(victimSet)

	for id, j := range snapshot {
		if _, isVictim := victimSet[id]; isVictim {
			continue
		}
		parents, parentsChanged := dropIDs(j.ParentJobs, victimSet)
		dependents, dependentsChanged := dropIDs(j.DependentJobs, victimSet)
		if !parentsChanged && !dependentsChanged {
			continue
		}
		p.updates = append(p.updates, JobReferenceUpdate{
			Id:            id,
			ParentJobs:    parents,
			DependentJobs: dependents,
		})
	}
	sort.Slice(p.updates, func(i, k int) bool { return p.updates[i].Id < p.updates[k].Id })

	return p
}

func (p *deletePlan) anomaly(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	p.anomalies = append(p.anomalies, msg)
	log.Warnf("job delete plan for %s: %s", p.target, msg)
}

// deleteState holds the per-cache bookkeeping a delete needs. Tombstones and
// pending plans are read by Get/Set/Persist without the graph mutex, hence the
// dedicated lock.
type deleteState struct {
	mu         sync.RWMutex
	tombstones map[string]struct{}
	pending    map[string]*deletePlan
}

func (s *deleteState) init() {
	if s.tombstones == nil {
		s.tombstones = map[string]struct{}{}
	}
	if s.pending == nil {
		s.pending = map[string]*deletePlan{}
	}
}

func (s *deleteState) isDeleted(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.tombstones[id]
	return ok
}

func (s *deleteState) markDeleted(ids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	for _, id := range ids {
		s.tombstones[id] = struct{}{}
	}
}

func (s *deleteState) setPending(plan *deletePlan) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	s.pending[plan.target] = plan
}

func (s *deleteState) pendingPlan(id string) *deletePlan {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pending[id]
}

func (s *deleteState) clearPending(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, id)
}

// deleteBackend adapts one cache implementation to the shared delete logic.
type deleteBackend struct {
	db           JobDB
	snapshot     func() map[string]*Job
	applyUpdates func(updates []JobReferenceUpdate)
	removeJobs   func(victims []string)
	// beforeApply, when set, runs after the batch committed and before the
	// in-memory apply. Tests use it to inject an apply-phase failure.
	beforeApply func() error
}

// deleteJob runs the delete steps described at the top of this file. graph must
// be the cache's graph mutex; the caller must not hold it.
func deleteJob(graph *sync.Mutex, state *deleteState, backend deleteBackend, id string) error {
	graph.Lock()
	defer graph.Unlock()

	// A previous DELETE that committed but did not finish applying is resumed
	// here instead of committing again.
	if plan := state.pendingPlan(id); plan != nil {
		return finishCommittedDelete(state, backend, plan)
	}
	// A completed delete is idempotent: repeat it without cascade side effects.
	if state.isDeleted(id) {
		return nil
	}

	snapshot := backend.snapshot()
	if _, exists := snapshot[id]; !exists {
		return ErrJobDoesntExist
	}

	plan := planDelete(snapshot, id)

	batcher, ok := backend.db.(BatchJobDB)
	if !ok {
		return &DeleteError{Id: id, Phase: DeletePhasePersist, Err: ErrAtomicBatchUnsupported}
	}
	if err := batcher.ApplyBatch(plan.batch()); err != nil {
		return &DeleteError{Id: id, Phase: DeletePhasePersist, Err: err}
	}

	// From here storage is final. Remember the plan so a failed apply can be
	// completed by retrying the same DELETE.
	state.setPending(plan)
	return finishCommittedDelete(state, backend, plan)
}

// finishCommittedDelete performs the in-memory half of a delete whose backend
// batch already committed. Failures leave the plan pending so a retry resumes
// it; they never cause storage to be written again.
func finishCommittedDelete(state *deleteState, backend deleteBackend, plan *deletePlan) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("job delete for %s committed to storage, but applying it in memory panicked: %v; "+
				"retry the DELETE or rebuild the cache from storage", plan.target, r)
			err = &DeleteError{Id: plan.target, Phase: DeletePhaseApply, Committed: true,
				Err: fmt.Errorf("panic while applying delete: %v", r)}
		}
	}()

	if backend.beforeApply != nil {
		if failErr := backend.beforeApply(); failErr != nil {
			return &DeleteError{Id: plan.target, Phase: DeletePhaseApply, Committed: true, Err: failErr}
		}
	}

	// Tombstone first so a finishing run's write-back cannot resurrect the job
	// and a concurrent Get reports it as gone.
	state.markDeleted(plan.victims)
	backend.applyUpdates(plan.updates)
	backend.removeJobs(plan.victims)
	state.clearPending(plan.target)
	return nil
}

// deleteErrorPhase reports the delete phase a cache.Delete error came from, if
// any. Used by tests and callers that need to distinguish "nothing happened"
// from "committed but not applied".
func deleteErrorPhase(err error) (DeletePhase, bool) {
	var de *DeleteError
	if errors.As(err, &de) {
		return de.Phase, true
	}
	return "", false
}

// dropIDs returns ids with every entry in drop removed and duplicate entries
// collapsed, preserving first-seen order. It reports whether anything changed.
// An empty input is returned untouched; empty and nil never panic.
func dropIDs(ids []string, drop map[string]struct{}) ([]string, bool) {
	if len(ids) == 0 {
		return ids, false
	}
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	changed := false
	for _, id := range ids {
		if _, dropped := drop[id]; dropped {
			changed = true
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			changed = true
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if !changed {
		return ids, false
	}
	return out, true
}

// uniqueStrings returns ids without duplicates, preserving first-seen order.
func uniqueStrings(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// appendUniqueID appends id only if it is not already present. It reports
// whether the slice changed.
func appendUniqueID(ids []string, id string) ([]string, bool) {
	if containsString(ids, id) {
		return ids, false
	}
	return append(ids, id), true
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
