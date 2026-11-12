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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubRoundTrip is a scripted, in-process RoundTripper for hop limiting
// tests. It records every RoundTrip entry (the point where a hop occupies
// a slot), supports scripted redirects, and can hold a response body open
// until release is called so concurrency caps are asserted deterministically.
type stubRoundTrip struct {
	lock      sync.Mutex
	entries   []stubEntry
	blocking  map[string]chan struct{} // URL path -> release channel
	bodyHeld  map[string]chan struct{} // URL path -> closed when body closes
	failHosts map[string]bool
}

type stubEntry struct {
	host string
	path string
	fail bool
}

func newStubRoundTrip() *stubRoundTrip {
	return &stubRoundTrip{
		blocking:  map[string]chan struct{}{},
		bodyHeld:  map[string]chan struct{}{},
		failHosts: map[string]bool{},
	}
}

// hold keeps responses for the given path open until release(path).
func (s *stubRoundTrip) hold(path string) <-chan struct{} {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.blocking[path] = make(chan struct{})
	held := make(chan struct{})
	s.bodyHeld[path] = held
	return held
}

func (s *stubRoundTrip) release(path string) {
	s.lock.Lock()
	ch := s.blocking[path]
	s.lock.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (s *stubRoundTrip) fail(host string) {
	s.lock.Lock()
	s.failHosts[host] = true
	s.lock.Unlock()
}

func (s *stubRoundTrip) unFail(host string) {
	s.lock.Lock()
	delete(s.failHosts, host)
	s.lock.Unlock()
}

func (s *stubRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) {
	s.lock.Lock()
	failed := s.failHosts[req.URL.Host]
	// Record every dispatch, including failed ones, so hop counting can
	// be asserted on both success and failure paths.
	s.entries = append(s.entries, stubEntry{host: req.URL.Host, path: req.URL.Path, fail: failed})
	if failed {
		s.lock.Unlock()
		return nil, errors.New("simulated transport failure")
	}
	release := s.blocking[req.URL.Path]
	held := s.bodyHeld[req.URL.Path]
	s.lock.Unlock()

	if release != nil {
		select {
		case <-release:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}

	resp := &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"text/html"}},
		Request:    req,
	}
	rc := io.NopCloser(bytes.NewReader([]byte("ok")))
	if held != nil {
		rc = &closeSignaler{ReadCloser: rc, done: held}
	}
	resp.Body = rc
	return resp, nil
}

type closeSignaler struct {
	io.ReadCloser
	once sync.Once
	done chan struct{}
}

func (c *closeSignaler) Close() error {
	err := c.ReadCloser.Close()
	c.once.Do(func() { close(c.done) })
	return err
}

func (s *stubRoundTrip) count(host string) int {
	s.lock.Lock()
	defer s.lock.Unlock()
	n := 0
	for _, e := range s.entries {
		if host == "" || e.host == host {
			n++
		}
	}
	return n
}

func (s *stubRoundTrip) sawPath(path string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, e := range s.entries {
		if e.path == path {
			return true
		}
	}
	return false
}

// installHopRule builds an async Collector whose Transport is the stub and
// registers a Hop rule driven by the given clock. The clock is attached
// before Limit() so the lazily-created gate picks it up.
func installHopRule(t *testing.T, rt *stubRoundTrip, rule *LimitRule, clk clock) *Collector {
	t.Helper()
	rule.clock = clk
	c := NewCollector(Async(true), AllowURLRevisit())
	c.SetClient(&http.Client{Transport: rt})
	if err := c.Limit(rule); err != nil {
		t.Fatalf("Limit: %v", err)
	}
	return c
}

func waitDone(t *testing.T, c *Collector) {
	t.Helper()
	done := make(chan struct{})
	go func() { c.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait() did not finish")
	}
}

// gateStats safely reads a rule's gate, which is lazily created on the
// first matching hop.
func gateStats(rule *LimitRule) hopGateStats {
	if st, ok := rule.gateStats(); ok {
		return st
	}
	return hopGateStats{}
}

// TestHopLimitCrossOriginMultiHop runs a request that redirects across
// hosts against real httptest servers: every hop (initial, redirect,
// final) is counted against the matching rule and Parallelism=1 holds.
func TestHopLimitCrossOriginMultiHop(t *testing.T) {
	clk := newControlledClock(time.Unix(2000, 0))
	rule := &LimitRule{DomainGlob: "*", Parallelism: 1, Delay: time.Second, Hop: true}

	targetReady := make(chan struct{})
	targetRelease := make(chan struct{})
	var targetInFlight int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&targetInFlight, 1)
		close(targetReady)
		<-targetRelease
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/target", http.StatusSeeOther)
	}))
	defer origin.Close()

	c := NewCollector(Async(true), AllowURLRevisit())
	rule.clock = clk
	if err := c.Limit(rule); err != nil {
		t.Fatalf("Limit: %v", err)
	}
	var responses int64
	c.OnResponse(func(r *Response) { atomic.AddInt64(&responses, 1) })

	if err := c.Visit(origin.URL + "/start"); err != nil {
		t.Fatalf("Visit: %v", err)
	}
	// Wait until the entry hop completed and its redirect hop is queued;
	// only then advance the controlled clock, otherwise the broadcast is
	// missed by the not-yet-queued waiter.
	waitFor(t, func() bool { return gateStats(rule).Waiters == 1 }, "redirect hop to queue")
	clk.Advance(time.Second + time.Millisecond)
	select {
	case <-targetReady:
	case <-time.After(5 * time.Second):
		t.Fatal("final cross-origin hop never reached the target server")
	}

	st := gateStats(rule)
	if st.InFlight != 1 {
		t.Fatalf("inFlight=%d, want 1 while final body is open", st.InFlight)
	}
	if atomic.LoadInt64(&targetInFlight) != 1 {
		t.Fatalf("target hops=%d, want 1", targetInFlight)
	}
	// Two distinct hosts were visited via the redirect chain.
	originHost := mustURLHost(t, origin.URL)
	targetHost := mustURLHost(t, target.URL)
	if originHost == targetHost {
		t.Fatalf("test setup: redirect must cross hosts, got %q", originHost)
	}

	close(targetRelease)
	waitDone(t, c)
	if atomic.LoadInt64(&responses) != 1 {
		t.Fatalf("OnResponse count=%d, want 1 (callback semantics preserved)", responses)
	}
	if st := gateStats(rule); st.InFlight != 0 || st.Waiters != 0 {
		t.Fatalf("gate leaked after completion: %+v", st)
	}
}

func mustURLHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}

// TestHopLimitSharedRuleClones verifies that a Collector and its clone
// share the rule's Parallelism budget and count together.
func TestHopLimitSharedRuleClones(t *testing.T) {
	rt := newStubRoundTrip()
	clk := newControlledClock(time.Unix(3000, 0))
	rule := &LimitRule{DomainGlob: "*", Parallelism: 1, Delay: time.Second, Hop: true}
	c := installHopRule(t, rt, rule, clk)
	clone := c.Clone()

	heldA := rt.hold("/a")
	rt.hold("/b")

	if err := c.Visit("http://shared.example/a"); err != nil {
		t.Fatalf("visit a: %v", err)
	}
	waitFor(t, func() bool { return rt.count("shared.example") == 1 }, "first hop")

	if err := clone.Visit("http://shared.example/b"); err != nil {
		t.Fatalf("visit b: %v", err)
	}
	// The clone's hop must not start while the parent holds the only slot.
	time.Sleep(30 * time.Millisecond)
	if got := rt.count("shared.example"); got != 1 {
		t.Fatalf("shared rule admitted %d hops with Parallelism=1, want 1", got)
	}
	if st := gateStats(rule); st.Waiters != 1 || st.InFlight != 1 {
		t.Fatalf("shared gate state=%+v, want inFlight=1 waiters=1", st)
	}

	rt.release("/a")
	select {
	case <-heldA:
	case <-time.After(3 * time.Second):
		t.Fatal("body a never closed")
	}
	// The body's Close signals heldA before gate.release() runs, so wait
	// until the slot is actually returned and the cooldown armed before
	// advancing the controlled clock: otherwise the Advance would precede
	// the timer creation and the frozen clock would never fire it.
	waitFor(t, func() bool {
		st := gateStats(rule)
		return st.InFlight == 0 && !st.NextReady.IsZero()
	}, "slot released and cooldown armed")
	// Cooldown gates the next hop; advance the controlled clock.
	clk.Advance(time.Second + time.Millisecond)
	waitFor(t, func() bool { return rt.count("shared.example") == 2 }, "clone hop after cooldown")
	rt.release("/b")

	done := make(chan struct{})
	go func() { c.Wait(); clone.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait on parent and clone did not finish")
	}
	st := gateStats(rule)
	if st.InFlight != 0 || st.Waiters != 0 {
		t.Fatalf("shared gate leaked: %+v", st)
	}
	if st.Acquisitions != 2 {
		t.Fatalf("acquisitions=%d, want 2 across collector and clone", st.Acquisitions)
	}
}

// TestHopLimitIndependentRules proves rules matching different hosts keep
// independent budgets: a blocked hop on one host does not stall another.
func TestHopLimitIndependentRules(t *testing.T) {
	rt := newStubRoundTrip()
	clk := newControlledClock(time.Unix(4000, 0))
	c := NewCollector(Async(true), AllowURLRevisit())
	c.SetClient(&http.Client{Transport: rt})
	r1 := &LimitRule{DomainRegexp: `one\.example`, Parallelism: 1, Delay: time.Hour, Hop: true, clock: clk}
	r2 := &LimitRule{DomainRegexp: `two\.example`, Parallelism: 1, Delay: time.Hour, Hop: true, clock: clk}
	if err := c.Limits([]*LimitRule{r1, r2}); err != nil {
		t.Fatalf("Limits: %v", err)
	}
	rt.hold("/one")
	rt.hold("/two")

	if err := c.Visit("http://one.example/one"); err != nil {
		t.Fatalf("visit one: %v", err)
	}
	waitFor(t, func() bool { return rt.count("one.example") == 1 }, "host one hop")
	if err := c.Visit("http://two.example/two"); err != nil {
		t.Fatalf("visit two: %v", err)
	}
	waitFor(t, func() bool { return rt.count("two.example") == 1 }, "independent host two hop")

	if st := gateStats(r1); st.InFlight != 1 {
		t.Fatalf("rule1 inFlight=%d, want 1", st.InFlight)
	}
	if st := gateStats(r2); st.InFlight != 1 {
		t.Fatalf("rule2 inFlight=%d, want 1 (independent budget)", st.InFlight)
	}
	rt.release("/one")
	rt.release("/two")
	waitDone(t, c)
}

// TestHopLimitBodyNotClosedHoldsSlot ensures the slot stays occupied from
// dispatch until the response body is closed.
func TestHopLimitBodyNotClosedHoldsSlot(t *testing.T) {
	rt := newStubRoundTrip()
	clk := newControlledClock(time.Unix(5000, 0))
	rule := &LimitRule{DomainGlob: "*", Parallelism: 1, Delay: time.Second, Hop: true}
	c := installHopRule(t, rt, rule, clk)
	rt.hold("/hold")

	if err := c.Visit("http://hold.example/hold"); err != nil {
		t.Fatalf("visit: %v", err)
	}
	waitFor(t, func() bool { return rt.count("hold.example") == 1 }, "first hop")

	go func() { _ = c.Visit("http://hold.example/hold2") }()
	waitFor(t, func() bool { return gateStats(rule).Waiters == 1 }, "second waiter")
	// Advancing the clock cannot admit the second hop while the body is open.
	clk.Advance(time.Minute)
	time.Sleep(30 * time.Millisecond)
	if got := rt.count("hold.example"); got != 1 {
		t.Fatalf("slot released before body close: hops=%d", got)
	}
	rt.release("/hold")
	// Body close arms the cooldown; wait for the cooldown boundary to be
	// visible before advancing so the controlled-clock broadcast is not
	// missed.
	waitFor(t, func() bool { return !gateStats(rule).NextReady.IsZero() }, "cooldown armed")
	clk.Advance(time.Second + time.Millisecond)
	waitFor(t, func() bool { return rt.count("hold.example") == 2 }, "second hop after body close")
	waitDone(t, c)
}

// TestHopLimitFailureCoolsDownAndCounts covers a transport-level failure:
// the failed attempt occupies and releases a slot, arms a cooldown, and a
// Retry attempt is a separately counted hop admitted only afterwards.
func TestHopLimitFailureCoolsDownAndCounts(t *testing.T) {
	rt := newStubRoundTrip()
	clk := newControlledClock(time.Unix(6000, 0))
	rule := &LimitRule{DomainGlob: "*", Parallelism: 1, Delay: 5 * time.Second, Hop: true}
	c := installHopRule(t, rt, rule, clk)
	rt.fail("fail.example")

	var errorsSeen int64
	c.OnError(func(r *Response, err error) {
		atomic.AddInt64(&errorsSeen, 1)
		if r.Ctx.Get("retry") == "" {
			r.Ctx.Put("retry", "1")
			rt.unFail("fail.example")
			_ = r.Request.Retry()
		}
	})
	if err := c.Visit("http://fail.example/x"); err != nil {
		t.Fatalf("visit: %v", err)
	}
	waitFor(t, func() bool { return atomic.LoadInt64(&errorsSeen) >= 1 }, "first failure")
	// The failed attempt armed a cooldown; the retry waits behind it.
	waitFor(t, func() bool {
		st := gateStats(rule)
		return st.Waiters == 1 || rt.count("fail.example") == 2
	}, "retry queued or served")
	clk.Advance(5*time.Second + time.Millisecond)
	waitFor(t, func() bool { return rt.count("fail.example") == 2 }, "retry hop after cooldown")
	waitDone(t, c)
	st := gateStats(rule)
	if st.Acquisitions != 2 {
		t.Fatalf("acquisitions=%d, want 2 (failed attempt + retry)", st.Acquisitions)
	}
	if st.InFlight != 0 {
		t.Fatalf("failed attempt leaked slot: inFlight=%d", st.InFlight)
	}
}

// TestHopLimitCancellationStopsNewRequests verifies Collector.Context
// cancellation: queued hops return, no further HTTP request is sent, and
// Wait() still drains cleanly.
func TestHopLimitCancellationStopsNewRequests(t *testing.T) {
	rt := newStubRoundTrip()
	clk := newControlledClock(time.Unix(7000, 0))
	rule := &LimitRule{DomainGlob: "*", Parallelism: 1, Delay: time.Hour, Hop: true}
	ctx, cancel := context.WithCancel(context.Background())
	c := installHopRule(t, rt, rule, clk)
	c.Context = ctx
	rt.hold("/busy")

	if err := c.Visit("http://cancel.example/busy"); err != nil {
		t.Fatalf("visit busy: %v", err)
	}
	waitFor(t, func() bool { return rt.count("cancel.example") == 1 }, "first hop in flight")

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = c.Visit(fmt.Sprintf("http://cancel.example/queued-%d", i))
		}(i)
	}
	waitFor(t, func() bool { return gateStats(rule).Waiters == 3 }, "all queued")
	before := rt.count("cancel.example")

	cancel()
	waitFor(t, func() bool { return gateStats(rule).Waiters == 0 }, "queued hops cancelled")
	time.Sleep(30 * time.Millisecond)
	if got := rt.count("cancel.example"); got != before {
		t.Fatalf("cancelled collector sent new requests: before=%d after=%d", before, got)
	}

	rt.release("/busy")
	waitDone(t, c)
	if st := gateStats(rule); st.InFlight != 0 {
		t.Fatalf("in-flight slot leaked after cancel: %d", st.InFlight)
	}
}

// TestHopLimitCacheHitOccupiesNoSlot verifies that a cache hit never
// reaches the limiting Transport and is not counted.
func TestHopLimitCacheHitOccupiesNoSlot(t *testing.T) {
	rt := newStubRoundTrip()
	// Real clock: this test asserts cache bypass, not scheduling timing,
	// so the clock must advance on its own to avoid cooldown stalls.
	rule := &LimitRule{DomainGlob: "*", Parallelism: 1, Delay: time.Millisecond, Hop: true}
	c := installHopRule(t, rt, rule, realClock{})

	cacheDir := filepath.Join(t.TempDir(), "cache")
	c.CacheDir = cacheDir
	c.AllowURLRevisit = true

	if err := c.Visit("http://cached.example/page"); err != nil {
		t.Fatalf("first visit: %v", err)
	}
	waitFor(t, func() bool { return rt.count("cached.example") == 1 }, "first (uncached) hop")
	firstAcq := gateStats(rule).Acquisitions

	if err := c.Visit("http://cached.example/page"); err != nil {
		t.Fatalf("second visit: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if got := rt.count("cached.example"); got != 1 {
		t.Fatalf("cache hit performed a network hop: count=%d", got)
	}
	if st := gateStats(rule); st.Acquisitions != firstAcq {
		t.Fatalf("cache hit was counted: acquisitions %d -> %d", firstAcq, st.Acquisitions)
	}
	if _, err := os.Stat(cacheDir); err != nil {
		t.Fatalf("cache dir missing: %v", err)
	}
	waitDone(t, c)
}

// TestHopLimitInjectedClientDirectCallUnthrottled ensures external,
// direct use of an injected Client is not rate limited: an unmarked
// request bypasses the gate even while a Hop rule is active.
func TestHopLimitInjectedClientDirectCallUnthrottled(t *testing.T) {
	rt := newStubRoundTrip()
	clk := newControlledClock(time.Unix(9000, 0))
	rule := &LimitRule{DomainGlob: "*", Parallelism: 1, Delay: time.Hour, Hop: true}
	c := installHopRule(t, rt, rule, clk)

	req, err := http.NewRequest("GET", "http://direct.example/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		resp, err := c.backend.Client.Do(req)
		if err != nil {
			t.Fatalf("direct Do %d: %v", i, err)
		}
		resp.Body.Close()
	}
	// No Collector-marked request ran, so the gate was never created and
	// no acquisition could be recorded.
	if rule.gate != nil && gateStats(rule).Acquisitions != 0 {
		t.Fatalf("external calls were throttled: acquisitions=%d", gateStats(rule).Acquisitions)
	}
	if got := rt.count("direct.example"); got != 5 {
		t.Fatalf("direct calls=%d, want 5", got)
	}

	// A marked Collector request still works and gets its own gate.
	if err := c.Visit("http://marked.example/y"); err != nil {
		t.Fatalf("marked visit: %v", err)
	}
	waitDone(t, c)
	if rule.gate == nil || gateStats(rule).Acquisitions != 1 {
		t.Fatalf("marked request not counted: gate=%+v", rule.gate)
	}
}

// TestHopLimitRulePriorityPreserved verifies that when several rules
// match, the first registered rule wins (classic GetMatchingRule
// semantics), now including Hop rules.
func TestHopLimitRulePriorityPreserved(t *testing.T) {
	rt := newStubRoundTrip()
	clk := newControlledClock(time.Unix(10000, 0))
	c := NewCollector(Async(true), AllowURLRevisit())
	c.SetClient(&http.Client{Transport: rt})
	first := &LimitRule{DomainGlob: "*", Parallelism: 1, Delay: time.Second, Hop: true, clock: clk}
	second := &LimitRule{DomainRegexp: `.*`, Parallelism: 1, Delay: time.Hour, Hop: true, clock: clk}
	if err := c.Limits([]*LimitRule{first, second}); err != nil {
		t.Fatalf("Limits: %v", err)
	}
	rt.hold("/p")
	if err := c.Visit("http://prio.example/p"); err != nil {
		t.Fatalf("visit: %v", err)
	}
	waitFor(t, func() bool { return rt.count("prio.example") == 1 }, "first hop")
	if got := c.backend.GetMatchingRule("prio.example"); got != first {
		t.Fatal("matching rule priority not preserved: first rule should win")
	}
	rt.release("/p")
	waitDone(t, c)
}

// TestClassicLimitModeUnchanged is an end-to-end regression guard: with
// Hop left at its zero value (false), the classic waitChan-based limiter
// remains in use and the hop machinery is never installed.
func TestClassicLimitModeUnchanged(t *testing.T) {
	rt := newStubRoundTrip()
	c := NewCollector(Async(true), AllowURLRevisit())
	c.SetClient(&http.Client{Transport: rt})
	rule := &LimitRule{DomainGlob: "*", Parallelism: 2}
	if err := c.Limit(rule); err != nil {
		t.Fatalf("Limit: %v", err)
	}
	if c.backend.hopEnabled.Load() {
		t.Fatal("hop machinery must stay disabled for classic rules")
	}
	if _, ok := c.backend.Client.Transport.(*hopLimitTransport); ok {
		t.Fatal("hopLimitTransport must not wrap classic collectors")
	}
	if rule.waitChan == nil {
		t.Fatal("classic waitChan not initialized")
	}
	for i := 0; i < 3; i++ {
		if err := c.Visit(fmt.Sprintf("http://classic.example/%d", i)); err != nil {
			t.Fatalf("visit %d: %v", i, err)
		}
	}
	waitDone(t, c)
	if got := rt.count("classic.example"); got != 3 {
		t.Fatalf("classic requests=%d, want 3", got)
	}
}

// TestHopLimitRobotsTxtCounted verifies that the robots.txt fetch is a
// counted hop subject to the same rule, and the page hop waits for the
// robots cooldown. The stub's robots response returns immediately (the
// Collector fetches robots.txt synchronously inside Visit), and the order
// of hops is asserted with the controlled clock.
func TestHopLimitRobotsTxtCounted(t *testing.T) {
	rt := newStubRoundTrip()
	clk := newControlledClock(time.Unix(11000, 0))
	rule := &LimitRule{DomainGlob: "*", Parallelism: 1, Delay: time.Second, Hop: true}
	c := installHopRule(t, rt, rule, clk)
	c.IgnoreRobotsTxt = false

	// Hold only the page body so the final slot stays occupied and the
	// robots hop completes synchronously.
	heldPage := rt.hold("/allowed")

	if err := c.Visit("http://rob.example/allowed"); err != nil {
		t.Fatalf("visit: %v", err)
	}
	// The robots.txt hop must have happened first, counted as an
	// acquisition, and armed a cooldown before the page hop.
	waitFor(t, func() bool { return rt.sawPath("/robots.txt") }, "robots.txt hop")
	if !rt.sawPath("/robots.txt") {
		t.Fatal("robots.txt hop missing")
	}
	// The robots.txt hop arms a cooldown once its body closes. Wait for
	// that cooldown to be visible before advancing the clock, so the
	// controlled Advance cannot race gate.release().
	waitFor(t, func() bool { return !gateStats(rule).NextReady.IsZero() }, "robots cooldown armed")
	// The page hop cannot be dispatched until the cooldown elapses.
	time.Sleep(30 * time.Millisecond)
	if rt.sawPath("/allowed") {
		t.Fatal("page hop dispatched before the robots cooldown elapsed")
	}
	clk.Advance(time.Second + time.Millisecond)
	select {
	case <-heldPage:
		t.Fatal("page body should remain open, occupying the slot")
	default:
	}
	waitFor(t, func() bool { return rt.sawPath("/allowed") }, "page hop after robots cooldown")
	if st := gateStats(rule); st.InFlight != 1 {
		t.Fatalf("page hop must hold a slot: %+v", st)
	}
	rt.release("/allowed")
	waitDone(t, c)
	if st := gateStats(rule); st.InFlight != 0 || st.Waiters != 0 {
		t.Fatalf("gate leaked after robots flow: %+v", st)
	}
	// robots.txt + page = two counted hops.
	if got := rt.count("rob.example"); got != 2 {
		t.Fatalf("hops=%d, want 2 (robots.txt + page)", got)
	}
}

// TestHopLimitNoGoroutineLeak checks that the scheduler goroutine exits
// once the waiter queue drains (no goroutine outlives the traffic), across
// repeated bursts. A real clock with zero Delay keeps the flow unblocked.
func TestHopLimitNoGoroutineLeak(t *testing.T) {
	rt := newStubRoundTrip()
	rule := &LimitRule{DomainGlob: "*", Parallelism: 2, Hop: true}
	c := installHopRule(t, rt, rule, realClock{})

	baseline := runtime.NumGoroutine()
	const bursts, perBurst = 3, 10
	for b := 0; b < bursts; b++ {
		for i := 0; i < perBurst; i++ {
			if err := c.Visit(fmt.Sprintf("http://leak-%d.example/%d", b, i)); err != nil {
				t.Fatalf("visit: %v", err)
			}
		}
		waitDone(t, c)
		waitFor(t, func() bool {
			st := gateStats(rule)
			return st.InFlight == 0 && st.Waiters == 0
		}, "gate idle after burst")
	}
	if got := rt.count(""); got != bursts*perBurst {
		t.Fatalf("dispatched hops=%d, want %d", got, bursts*perBurst)
	}
	// Give the monitor a chance to observe the empty queue and exit.
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > baseline+1 && time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseline+1 {
		t.Fatalf("goroutine leak: baseline=%d now=%d", baseline, got)
	}
	if st := gateStats(rule); st.InFlight != 0 || st.Waiters != 0 {
		t.Fatalf("gate leaked state: %+v", st)
	}
}
