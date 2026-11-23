package job

import (
	"sync"
	"time"

	// This library abstracts the time functionality of the OS so that it can be controlled during unit tests.
	// It was selected over thejerf/abtime because abtime is geared towards precision timing rather than scheduling.
	// Other libraries were reviewed and rejected due to outstanding bugs.
	"github.com/mixer/clock"
)

// realClock is the production clock. It mirrors clock.DefaultClock but keeps a
// *time.Timer / *time.Ticker behind a pointer instead of embedding a time.Timer
// by value the way clock.DefaultClock does.
//
// Copying a time.Timer is not safe on Go 1.23+: the runtime locates a timer's
// state through the address of the Timer value, so a copied Timer makes Stop
// read unrelated memory. Go 1.27 additionally aborts the process with
// "stop of synctest timer from outside bubble" when such a copy is stopped from
// a different goroutine, which is exactly what cache.Delete does. Talking to
// the original *time.Timer avoids both problems.
type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (realClock) Sleep(d time.Duration)                  { time.Sleep(d) }
func (realClock) Tick(d time.Duration) <-chan time.Time  { return time.Tick(d) }
func (realClock) Since(t time.Time) time.Duration        { return time.Since(t) }

func (realClock) AfterFunc(d time.Duration, f func()) clock.Timer {
	return &realTimer{t: time.AfterFunc(d, f)}
}

func (realClock) NewTimer(d time.Duration) clock.Timer {
	return &realTimer{t: time.NewTimer(d)}
}

func (realClock) NewTicker(d time.Duration) clock.Ticker {
	return &realTicker{t: time.NewTicker(d)}
}

type realTimer struct {
	t *time.Timer
}

func (r *realTimer) Chan() <-chan time.Time     { return r.t.C }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }

type realTicker struct {
	t *time.Ticker
}

func (r *realTicker) Chan() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()                  { r.t.Stop() }

type Clock struct {
	clock.Clock
	lock sync.RWMutex
}

func (clk *Clock) SetClock(in clock.Clock) {
	clk.lock.Lock()
	defer clk.lock.Unlock()
	clk.Clock = in
}

func (clk *Clock) Time() clock.Clock {
	clk.lock.RLock()
	defer clk.lock.RUnlock()

	if clk.Clock == nil {
		clk.lock.RUnlock()
		clk.lock.Lock()
		clk.Clock = realClock{}
		clk.lock.Unlock()
		clk.lock.RLock()
	}

	return clk.Clock
}

func (clk *Clock) TimeSet() (result bool) {
	clk.lock.RLock()
	result = clk.Clock != nil
	clk.lock.RUnlock()
	return
}

type Clocker interface {
	Time() clock.Clock
	TimeSet() bool
}
