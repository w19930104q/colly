// Copyright 2018 Adam Tauber
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colly

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// clock abstracts the time operations used by the per-hop rate limiter so
// that the scheduling logic can be driven by a deterministic, controlled
// clock in tests.
type clock interface {
	Now() time.Time
	// NewTimer creates a timer that fires once the clock reaches
	// now+d. A non-positive d fires immediately. The returned timer
	// must be Stop()ed if the caller abandons the wait before it fires.
	NewTimer(d time.Duration) clockTimer
}

// clockTimer is the timer abstraction the scheduler waits on.
type clockTimer interface {
	// C returns the channel that receives (or is closed) once the
	// deadline is reached.
	C() <-chan time.Time
	// Stop prevents the timer from firing. It returns true if the call
	// stopped the timer before it fired. Stopping an already-fired
	// timer is a harmless no-op returning false.
	Stop() bool
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) NewTimer(d time.Duration) clockTimer {
	return &realTimer{t: time.NewTimer(d)}
}

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time { return r.t.C }
func (r *realTimer) Stop() bool          { return r.t.Stop() }

// controlledClock is a manually advanced clock for deterministic tests.
// Timers register a deadline plus a wake channel; Advance closes the
// channels of every timer whose deadline has been reached. The design is
// event-driven and spawns no goroutines, so it cannot leak.
type controlledClock struct {
	lock    sync.Mutex
	now     time.Time
	pending []*controlledTimer
}

func newControlledClock(start time.Time) *controlledClock {
	return &controlledClock{now: start}
}

func (c *controlledClock) Now() time.Time {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.now
}

func (c *controlledClock) NewTimer(d time.Duration) clockTimer {
	tm := &controlledTimer{clock: c, deadline: c.Now().Add(d), ch: make(chan time.Time, 1)}
	c.lock.Lock()
	defer c.lock.Unlock()
	if d <= 0 || !c.now.Before(tm.deadline) {
		tm.ch <- tm.deadline
		return tm
	}
	c.pending = append(c.pending, tm)
	return tm
}

// Advance moves the clock forward and fires every timer whose deadline
// has been reached.
func (c *controlledClock) Advance(d time.Duration) {
	c.lock.Lock()
	c.now = c.now.Add(d)
	remaining := c.pending[:0]
	for _, tm := range c.pending {
		if !c.now.Before(tm.deadline) {
			tm.ch <- tm.deadline
		} else {
			remaining = append(remaining, tm)
		}
	}
	c.pending = remaining
	c.lock.Unlock()
}

// controlledTimer is removed from its clock's pending list by Stop.
type controlledTimer struct {
	clock    *controlledClock
	deadline time.Time
	ch       chan time.Time
	stopOnce sync.Once
}

func (t *controlledTimer) C() <-chan time.Time { return t.ch }

func (t *controlledTimer) Stop() bool {
	stopped := false
	t.stopOnce.Do(func() {
		t.clock.lock.Lock()
		for i, tm := range t.clock.pending {
			if tm == t {
				t.clock.pending = append(t.clock.pending[:i], t.clock.pending[i+1:]...)
				stopped = true
				break
			}
		}
		t.clock.lock.Unlock()
	})
	return stopped
}

// hopWaiter is a single queued request waiting for a slot and/or the
// per-rule cooldown to elapse. The ordered slice grants tickets FIFO,
// which makes dispatch order deterministic.
type hopWaiter struct {
	id uint64
	ch chan struct{}
}

// hopGate is the per-LimitRule scheduler for hop-by-hop rate limiting.
//
// A hop may be sent only when both conditions hold:
//   - fewer than Parallelism hops of this rule are currently in flight;
//   - the rule-wide cooldown (Delay + a random fraction of RandomDelay,
//     started when the previous hop closed its response body) has elapsed.
//
// Every completed hop holds its slot until its response body is closed,
// then arms a cooldown. Failed and aborted hops arm the same cooldown.
// Cancelling a waiting hop only removes that waiter: it never releases a
// slot early nor shortens the cooldown, so other waiters cannot be let
// through ahead of schedule.
//
// A single monitor goroutine owns every grant decision. It starts with
// the first waiter and exits once no waiter remains, so no scheduler
// goroutine outlives the requests that needed it.
type hopGate struct {
	clock       clock
	parallelism int
	delay       time.Duration
	randomDelay time.Duration

	// wake is closed on every state change (new waiter, cancellation,
	// released slot, armed cooldown) and immediately replaced with a
	// fresh channel. The monitor always captures the current channel
	// while holding lock, so no broadcast can be missed.
	lock         sync.Mutex
	wake         chan struct{}
	inFlight     int
	nextReady    time.Time
	waiters      []hopWaiter
	nextID       uint64
	running      bool
	acquisitions int
}

// hopGateStats is a point-in-time snapshot of a hopGate, used by tests to
// assert concurrency caps, cooldown readiness and the absence of leaks.
type hopGateStats struct {
	InFlight     int
	Waiters      int
	NextReady    time.Time
	Acquisitions int
}

func (g *hopGate) initGate() {
	g.wake = make(chan struct{})
}

// broadcast signals the monitor. Caller must hold g.lock.
func (g *hopGate) broadcast() {
	close(g.wake)
	g.wake = make(chan struct{})
}

func (g *hopGate) stats() hopGateStats {
	g.lock.Lock()
	defer g.lock.Unlock()
	return hopGateStats{
		InFlight:     g.inFlight,
		Waiters:      len(g.waiters),
		NextReady:    g.nextReady,
		Acquisitions: g.acquisitions,
	}
}

// gateStats returns the gate's snapshot once it exists, or a zero value
// before the first matching hop has lazily created it. Tests use this to
// avoid racing lazy initialization.
func (r *LimitRule) gateStats() (hopGateStats, bool) {
	r.lock.Lock()
	g := r.gate
	r.lock.Unlock()
	if g == nil {
		return hopGateStats{}, false
	}
	return g.stats(), true
}

// canGrant reports whether the head waiter may be sent now. Caller holds
// g.lock.
func (g *hopGate) canGrant(now time.Time) bool {
	return g.inFlight < g.parallelism && !now.Before(g.nextReady)
}

// acquire blocks until a slot is granted to the caller or ctx is done.
// On success the caller owns exactly one slot, which must be returned via
// release. Context cancellation removes only this waiter: in-flight hops
// and the cooldown schedule are left untouched.
func (g *hopGate) acquire(ctx context.Context) error {
	g.lock.Lock()
	if g.canGrant(g.clock.Now()) {
		g.inFlight++
		g.acquisitions++
		g.lock.Unlock()
		return nil
	}
	id := g.nextID
	g.nextID++
	ch := make(chan struct{})
	g.waiters = append(g.waiters, hopWaiter{id: id, ch: ch})
	if !g.running {
		// Start under g.lock so the previous monitor cannot observe an
		// empty waiter list and exit before this waiter is published.
		g.running = true
		go g.monitor()
	}
	g.broadcast()
	g.lock.Unlock()

	context.AfterFunc(ctx, func() {
		g.lock.Lock()
		g.removeWaiter(id)
		g.broadcast()
		g.lock.Unlock()
	})

	select {
	case <-ch:
		// Slot granted; the monitor already accounted inFlight and the
		// acquisition, so nothing more to do.
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// monitor is the sole grantor of slots to queued waiters. It waits on
// either a clock timer (cooldown elapsing) or the wake channel (a slot
// release, a cancellation or a new waiter), then re-evaluates. It exits
// once no waiter remains, leaving no goroutine behind.
func (g *hopGate) monitor() {
	g.lock.Lock()
	for {
		now := g.clock.Now()
		for len(g.waiters) > 0 && g.canGrant(now) {
			head := g.waiters[0]
			g.waiters = g.waiters[1:]
			g.inFlight++
			g.acquisitions++
			close(head.ch)
		}
		if len(g.waiters) == 0 {
			g.running = false
			g.lock.Unlock()
			return
		}

		// Capture the current wake channel under the lock: any broadcast
		// after this point closes exactly this channel, so a wake-up can
		// never be lost while the lock is released.
		wake := g.wake
		now = g.clock.Now()
		var timerC <-chan time.Time
		var tm clockTimer
		if now.Before(g.nextReady) {
			tm = g.clock.NewTimer(g.nextReady.Sub(now))
			timerC = tm.C()
		}
		g.lock.Unlock()

		select {
		case <-timerC:
			// Cooldown elapsed.
		case <-wake:
			// State changed; re-evaluate (slot freed, nextReady moved,
			// waiter cancelled).
			if tm != nil {
				tm.Stop()
			}
		}
		g.lock.Lock()
	}
}

// release returns one slot and arms the cooldown. It is called exactly
// once per acquired slot, when the response body is closed — including
// bodies closed after a failed or aborted request.
func (g *hopGate) release() {
	g.lock.Lock()
	g.inFlight--
	if g.delay > 0 || g.randomDelay > 0 {
		random := time.Duration(0)
		if g.randomDelay > 0 {
			random = time.Duration(rand.Int63n(int64(g.randomDelay)))
		}
		if readyAt := g.clock.Now().Add(g.delay + random); readyAt.After(g.nextReady) {
			g.nextReady = readyAt
		}
	}
	g.broadcast()
	g.lock.Unlock()
}

// Caller must hold g.lock.
func (g *hopGate) removeWaiter(id uint64) {
	for i, w := range g.waiters {
		if w.id == id {
			g.waiters = append(g.waiters[:i], g.waiters[i+1:]...)
			return
		}
	}
}

// hopGateBody wraps a response body so that the hop's slot is returned
// when the body is closed. Close is idempotent: a double close releases
// the slot exactly once.
type hopGateBody struct {
	io.ReadCloser
	gate *hopGate
	once sync.Once
}

func (b *hopGateBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.gate.release)
	return err
}

// hopLimitKeyType is an unexported context key type to avoid collisions.
type hopLimitKeyType struct{}

// hopChainKeyType marks a redirect chain that already owns a slot. The
// http.Client follows redirects by issuing the next hop from inside the
// same RoundTrip call: a redirect hop therefore reuses the entry hop's
// slot (held until the final response body closes) instead of blocking on
// acquire, which would deadlock a rule whose Parallelism is smaller than
// the chain length. Every hop is still counted as an acquisition and is
// matched against its own target host's rule when the chain crosses
// domains; an unmatched redirect hop simply rides the entry's slot.
type hopChainKeyType struct{}

// hopLimitKey marks requests that originate from a Collector and must
// therefore pass through per-hop rate limiting. Requests whose context
// carries no marker are external, direct uses of the Client/Transport and
// are forwarded untouched.
var (
	hopLimitKey hopLimitKeyType
	hopChainKey hopChainKeyType
)

// hopLimitTransport wraps the Collector's http.RoundTripper. It is a
// transparent delegate: only requests carrying the Collector marker in
// their context are rate limited, so injecting a custom Client or
// Transport keeps working and calling it directly is never throttled.
type hopLimitTransport struct {
	base    http.RoundTripper
	backend *httpBackend
}

func (t *hopLimitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Redirect hop: the chain already holds a slot. Count the hop against
	// the rule matching its own target host, but never block — the slot
	// is released once when the final body closes.
	if entryGate, ok := req.Context().Value(hopChainKey).(*hopGate); ok {
		if g := t.gateFor(req); g != nil {
			g.countRedirectHop()
		}
		resp, err := t.base.RoundTrip(req)
		if err != nil {
			// A failed redirect ends the whole chain; the entry slot is
			// released here exactly once.
			entryGate.release()
			return nil, err
		}
		// Intermediate redirect response: close its body promptly instead
		// of wrapping it (the slot is released only by the final body
		// close). The standard client already drains redirect bodies, and
		// Close is idempotent.
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			return resp, nil
		}
		// Final response of the chain: own the entry slot until closed.
		resp.Body = &hopGateBody{ReadCloser: resp.Body, gate: entryGate}
		return resp, nil
	}

	g := t.gateFor(req)
	if g == nil {
		return t.base.RoundTrip(req)
	}
	// The request context is the Collector's Context, so waiting for a
	// slot responds to cancellation: no new HTTP request is sent once
	// the Collector is cancelled.
	if err := g.acquire(req.Context()); err != nil {
		return nil, err
	}
	// Tag the request so every redirect issued within this RoundTrip
	// re-enters RoundTrip without acquiring another slot.
	req = req.WithContext(context.WithValue(req.Context(), hopChainKey, g))
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		// Failed attempts occupy a slot and cool down just like
		// successful ones.
		g.release()
		return nil, err
	}
	resp.Body = &hopGateBody{ReadCloser: resp.Body, gate: g}
	return resp, nil
}

// countRedirectHop records a redirect hop as an acquisition without
// touching inFlight: the entry hop owns the slot for the whole chain.
func (g *hopGate) countRedirectHop() {
	g.lock.Lock()
	g.acquisitions++
	g.lock.Unlock()
}

// gateFor resolves the per-hop gate for a marked request's target host,
// or returns nil for unmarked / unmatched requests.
func (t *hopLimitTransport) gateFor(req *http.Request) *hopGate {
	marked, _ := req.Context().Value(hopLimitKey).(*httpBackend)
	if marked == nil || marked != t.backend {
		return nil
	}
	rule := t.backend.GetMatchingRule(req.URL.Host)
	if rule == nil || !rule.Hop {
		return nil
	}
	return rule.hopGateLocked()
}

// hopGateLocked lazily builds the rule's gate. Delay, RandomDelay and
// Parallelism are snapshotted at first use; the injectable clock allows
// tests to drive the gate with a controlled clock.
func (r *LimitRule) hopGateLocked() *hopGate {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.gate == nil {
		clk := r.clock
		if clk == nil {
			clk = realClock{}
		}
		r.gate = &hopGate{
			clock:       clk,
			parallelism: max(r.Parallelism, 1),
			delay:       r.Delay,
			randomDelay: r.RandomDelay,
		}
		r.gate.initGate()
	}
	return r.gate
}

// enableHopLimiting wraps the backend's Transport in a hopLimitTransport.
// It is idempotent and preserves any previously injected Transport (or
// the http.DefaultTransport when none is set) as the delegated base.
func (h *httpBackend) enableHopLimiting() {
	if h.hopEnabled.Load() {
		return
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.hopEnabled.Load() {
		return
	}
	h.ensureHopTransportLocked()
	h.hopEnabled.Store(true)
}

// ensureHopTransport (re)installs the limiting wrapper after the whole
// http.Client has been replaced (e.g. SetClient or Appengine). It is a
// no-op until at least one Hop rule has been registered.
func (h *httpBackend) ensureHopTransport() {
	if !h.hopEnabled.Load() {
		return
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	h.ensureHopTransportLocked()
}

// Caller must hold h.lock.
func (h *httpBackend) ensureHopTransportLocked() {
	if _, ok := h.Client.Transport.(*hopLimitTransport); ok {
		return
	}
	base := h.Client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	h.Client.Transport = &hopLimitTransport{base: base, backend: h}
}

// markHopContext attaches the per-hop limiting marker to req when at
// least one rule enables hop mode. Unmarked requests bypass the limiting
// Transport logic entirely.
func (h *httpBackend) markHopContext(ctx context.Context) context.Context {
	if !h.hopEnabled.Load() {
		return ctx
	}
	return context.WithValue(ctx, hopLimitKey, h)
}
