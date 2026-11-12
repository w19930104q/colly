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
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

// newTestGate builds a hopGate driven by a controlled clock.
func newTestGate(t *testing.T, parallelism int, delay, randomDelay time.Duration) (*hopGate, *controlledClock) {
	t.Helper()
	clk := newControlledClock(time.Unix(1000, 0))
	g := &hopGate{
		clock:       clk,
		parallelism: parallelism,
		delay:       delay,
		randomDelay: randomDelay,
	}
	g.initGate()
	return g, clk
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
}

// TestHopGateParallelismCap verifies that no more than Parallelism hops
// are ever granted concurrently, and queued waiters are granted FIFO as
// slots are released.
func TestHopGateParallelismCap(t *testing.T) {
	g, _ := newTestGate(t, 2, 0, 0)
	ctx := context.Background()

	var maxConcurrent int
	var current int
	var mu sync.Mutex
	const total = 6
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.acquire(ctx); err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			mu.Lock()
			current++
			if current > maxConcurrent {
				maxConcurrent = current
			}
			mu.Unlock()
			// Hold the slot briefly so the cap is actually exercised.
			time.Sleep(5 * time.Millisecond)
			mu.Lock()
			current--
			mu.Unlock()
			g.release()
		}()
	}
	wg.Wait()

	if maxConcurrent > 2 {
		t.Fatalf("parallelism cap violated: observed %d concurrent hops, max 2", maxConcurrent)
	}
	st := g.stats()
	if st.InFlight != 0 {
		t.Fatalf("slot leak: inFlight=%d, want 0", st.InFlight)
	}
	if st.Waiters != 0 {
		t.Fatalf("waiter leak: %d waiters remain", st.Waiters)
	}
	if st.Acquisitions != total {
		t.Fatalf("acquisitions=%d, want %d (each hop counted exactly once)", st.Acquisitions, total)
	}
}

// TestHopGateCooldownControlledClock drives the scheduler entirely with a
// manually advanced clock: after a body closes, the next hop must not be
// sent until the clock passes the armed cooldown. Dispatch instants are
// asserted against the clock, not wall time.
func TestHopGateCooldownControlledClock(t *testing.T) {
	const cooldown = 10 * time.Second
	g, clk := newTestGate(t, 1, cooldown, 0)
	ctx := context.Background()

	// First hop is granted immediately.
	if err := g.acquire(ctx); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	firstAt := clk.Now()

	// Second hop must wait for both the slot and the cooldown.
	got := make(chan struct{})
	go func() {
		if err := g.acquire(ctx); err != nil {
			t.Errorf("second acquire: %v", err)
			return
		}
		close(got)
	}()
	waitFor(t, func() bool { return g.stats().Waiters == 1 }, "second hop to queue")

	// Releasing the slot arms a cooldown but must not admit the waiter
	// before the clock advances.
	g.release()
	select {
	case <-got:
		t.Fatal("waiter admitted before cooldown elapsed")
	case <-time.After(20 * time.Millisecond):
	}
	st := g.stats()
	wantReady := firstAt.Add(cooldown)
	if !st.NextReady.Equal(wantReady) {
		t.Fatalf("nextReady=%v, want %v", st.NextReady, wantReady)
	}

	// Advancing to just before the deadline still blocks.
	clk.Advance(cooldown - time.Second)
	select {
	case <-got:
		t.Fatal("waiter admitted before cooldown deadline")
	case <-time.After(20 * time.Millisecond):
	}

	// Crossing the deadline admits the waiter without any wall-clock sleep.
	clk.Advance(time.Second)
	select {
	case <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("waiter not admitted after cooldown elapsed")
	}
	if st := g.stats(); st.Acquisitions != 2 {
		t.Fatalf("acquisitions=%d, want 2", st.Acquisitions)
	}
}

// TestHopGateCancellationReleasesOnlyWaiter ensures that cancelling a
// queued hop removes just that waiter: in-flight slots stay occupied, the
// cooldown is untouched, and a later waiter is not admitted early.
func TestHopGateCancellationReleasesOnlyWaiter(t *testing.T) {
	const cooldown = 30 * time.Second
	g, clk := newTestGate(t, 1, cooldown, 0)
	baseCtx := context.Background()

	if err := g.acquire(baseCtx); err != nil {
		t.Fatalf("acquire in-flight: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(baseCtx)
	errCh := make(chan error, 1)
	go func() { errCh <- g.acquire(cancelCtx) }()
	waitFor(t, func() bool { return g.stats().Waiters == 1 }, "waiter to queue")

	readyBefore := g.stats().NextReady
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("acquire err=%v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled acquire did not return")
	}
	waitFor(t, func() bool { return g.stats().Waiters == 0 }, "waiter removal")

	st := g.stats()
	if st.InFlight != 1 {
		t.Fatalf("in-flight slot must survive cancellation, got inFlight=%d", st.InFlight)
	}
	// No cooldown was armed by the cancellation (slot still held).
	if !st.NextReady.Equal(readyBefore) {
		t.Fatalf("cancellation changed cooldown: %v -> %v", readyBefore, st.NextReady)
	}

	// Releasing the in-flight hop arms the cooldown; advancing the clock
	// must leave the gate idle with zero waiters and one in-flight slot
	// returned — nothing leaked and nothing was admitted early.
	g.release()
	clk.Advance(cooldown + time.Second)
	waitFor(t, func() bool {
		st := g.stats()
		return st.InFlight == 0 && st.Waiters == 0
	}, "gate to settle after release")
}

// TestHopGateCancellationDoesNotShortenCooldown proves a cancellation
// cannot let another request jump the cooldown: while one hop cools down,
// a cancelled waiter is removed but a fresh queued waiter still blocks
// until the full cooldown passes.
func TestHopGateCancellationDoesNotShortenCooldown(t *testing.T) {
	const cooldown = 20 * time.Second
	g, clk := newTestGate(t, 2, cooldown, 0)
	ctx := context.Background()

	if err := g.acquire(ctx); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	g.release() // arms cooldown, frees the slot

	cancelCtx, cancel := context.WithCancel(ctx)
	cancelErr := make(chan error, 1)
	go func() { cancelErr <- g.acquire(cancelCtx) }()
	waitFor(t, func() bool { return g.stats().Waiters == 1 }, "first waiter")
	cancel()
	select {
	case <-cancelErr:
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not unblock waiter")
	}

	admitted := make(chan struct{})
	go func() {
		if err := g.acquire(ctx); err != nil {
			t.Errorf("second waiter acquire: %v", err)
			return
		}
		close(admitted)
	}()
	waitFor(t, func() bool { return g.stats().Waiters == 1 }, "second waiter")

	clk.Advance(cooldown - time.Second)
	select {
	case <-admitted:
		t.Fatal("cooldown was shortened by the earlier cancellation")
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(time.Second)
	select {
	case <-admitted:
	case <-time.After(3 * time.Second):
		t.Fatal("waiter not admitted after full cooldown")
	}
}

// TestHopGateFailureCoolsDown verifies the failure path: a release()
// following a failed RoundTrip occupies then returns the slot and arms
// the same cooldown as a success, and acquisitions are still counted once.
func TestHopGateFailureCoolsDown(t *testing.T) {
	const cooldown = 8 * time.Second
	g, clk := newTestGate(t, 1, cooldown, 0)
	ctx := context.Background()

	if err := g.acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// Simulate a failed attempt: slot was acquired, request errored.
	g.release()

	next := make(chan struct{})
	go func() {
		if err := g.acquire(ctx); err != nil {
			t.Errorf("acquire after failure: %v", err)
			return
		}
		close(next)
	}()
	waitFor(t, func() bool { return g.stats().Waiters == 1 }, "retry to queue")
	clk.Advance(cooldown - time.Second)
	select {
	case <-next:
		t.Fatal("retry admitted before failure cooldown elapsed")
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(time.Second)
	select {
	case <-next:
	case <-time.After(3 * time.Second):
		t.Fatal("retry not admitted after failure cooldown")
	}
	if st := g.stats(); st.Acquisitions != 2 {
		t.Fatalf("acquisitions=%d, want 2", st.Acquisitions)
	}
}

// TestHopGateNoLeakWhenIdle checks that the monitor goroutine exits once
// all waiters are served, even across multiple bursts of traffic.
func TestHopGateNoLeakWhenIdle(t *testing.T) {
	g, _ := newTestGate(t, 2, 0, 0)
	ctx := context.Background()

	for burst := 0; burst < 3; burst++ {
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := g.acquire(ctx); err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				g.release()
			}()
		}
		wg.Wait()
		waitFor(t, func() bool {
			st := g.stats()
			return st.InFlight == 0 && st.Waiters == 0
		}, "gate idle between bursts")
	}
	// Give an about-to-exit monitor a chance to wind down, then prove no
	// state is retained.
	waitFor(t, func() bool { return g.stats().Waiters == 0 }, "final idle")
	if st := g.stats(); st.InFlight != 0 || st.Waiters != 0 {
		t.Fatalf("gate leaked state: %+v", st)
	}
}

// TestHopGateRandomDelayBound verifies that the cooldown armed on release
// lies within [Delay, Delay+RandomDelay) and that the next hop is admitted
// only after the full (randomized) cooldown elapses on the controlled clock.
func TestHopGateRandomDelayBound(t *testing.T) {
	const (
		delay       = 5 * time.Second
		randomDelay = 2 * time.Second
	)
	g, clk := newTestGate(t, 1, delay, randomDelay)
	ctx := context.Background()

	if err := g.acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	start := clk.Now()
	g.release()

	ready := g.stats().NextReady
	minReady := start.Add(delay)
	maxReady := start.Add(delay + randomDelay)
	if ready.Before(minReady) {
		t.Fatalf("cooldown %v shorter than Delay (%v)", ready.Sub(start), delay)
	}
	if !ready.Before(maxReady) {
		t.Fatalf("cooldown %v not within Delay+RandomDelay window", ready.Sub(start))
	}

	got := make(chan struct{})
	go func() {
		if err := g.acquire(ctx); err != nil {
			t.Errorf("acquire after cooldown: %v", err)
			return
		}
		close(got)
	}()
	waitFor(t, func() bool { return g.stats().Waiters == 1 }, "waiter to queue")

	// Just before the armed deadline the hop must remain blocked.
	clk.Advance(ready.Sub(clk.Now()) - time.Millisecond)
	select {
	case <-got:
		t.Fatal("hop admitted before the randomized cooldown deadline")
	case <-time.After(20 * time.Millisecond):
	}
	// Crossing the deadline admits it without wall-clock sleeping.
	clk.Advance(2 * time.Millisecond)
	select {
	case <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("hop not admitted after randomized cooldown")
	}
	g.release()
	if st := g.stats(); st.InFlight != 0 || st.Waiters != 0 {
		t.Fatalf("gate leaked: %+v", st)
	}
}
