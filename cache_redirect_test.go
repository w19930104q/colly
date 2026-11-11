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
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// redirectTestServer serves a multi-hop redirect chain
//
//	/a/start -> 302 /b/mid -> 302 /c/final (text/html)
//	/x/start -> 302 /x/final (text/xml)
//
// and counts every incoming HTTP request so tests can assert how often
// the origin was contacted.
type redirectTestServer struct {
	*httptest.Server
	hits *int64
}

func newRedirectTestServer() *redirectTestServer {
	var hits int64
	mux := http.NewServeMux()
	count := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&hits, 1)
			h(w, r)
		}
	}
	mux.HandleFunc("/a/start", count(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/b/mid", http.StatusFound)
	}))
	mux.HandleFunc("/b/mid", count(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/c/final", http.StatusFound)
	}))
	mux.HandleFunc("/c/final", count(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><body><a href="rel/page">link</a><p>final</p></body></html>`)
	}))
	mux.HandleFunc("/x/start", count(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/x/final", http.StatusFound)
	}))
	mux.HandleFunc("/x/final", count(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		fmt.Fprint(w, `<?xml version="1.0"?><root><a href="rel/x">link</a></root>`)
	}))
	mux.HandleFunc("/cookie/start", count(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/cookie/final", http.StatusFound)
	}))
	mux.HandleFunc("/cookie/final", count(func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "abc"})
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><body><p>cookie</p></body></html>`)
	}))
	s := &redirectTestServer{hits: &hits}
	s.Server = httptest.NewServer(mux)
	return s
}

func (s *redirectTestServer) requests() int64 {
	return atomic.LoadInt64(s.hits)
}

// cacheRunResult captures the observable behavior of one Visit.
type cacheRunResult struct {
	events       []string // callback order
	finalURL     string   // Request.URL seen by OnResponse
	absLink      string   // absolute URL of the relative link seen by OnHTML/OnXML
	body         string
	headersCalls int
}

func collectCacheRun(c *Collector) *cacheRunResult {
	res := &cacheRunResult{}
	c.OnRequest(func(r *Request) {
		res.events = append(res.events, "request")
	})
	c.OnResponseHeaders(func(r *Response) {
		res.headersCalls++
		res.events = append(res.events, "responseHeaders")
	})
	c.OnResponse(func(r *Response) {
		res.events = append(res.events, "response")
		res.finalURL = r.Request.URL.String()
		res.body = string(r.Body)
	})
	c.OnHTML("a", func(e *HTMLElement) {
		res.events = append(res.events, "html")
		res.absLink = e.Request.AbsoluteURL(e.Attr("href"))
	})
	c.OnScraped(func(r *Response) {
		res.events = append(res.events, "scraped")
	})
	return res
}

// TestCacheRedirectColdVsHot compares a cold-cache (live) run with a
// hot-cache run of a fresh collector (simulating a process restart: only
// the cache directory is shared). URLs, relative link resolution,
// callback order, dedup state and network usage must match.
func TestCacheRedirectColdVsHot(t *testing.T) {
	s := newRedirectTestServer()
	defer s.Close()
	cacheDir := t.TempDir()
	startURL := s.URL + "/a/start"

	// Cold run: live fetch through both redirect hops.
	c1 := NewCollector(CacheDir(cacheDir))
	run1 := collectCacheRun(c1)
	if err := c1.Visit(startURL); err != nil {
		t.Fatalf("cold visit failed: %s", err)
	}
	if got := s.requests(); got != 3 {
		t.Fatalf("cold run made %d origin requests, want 3", got)
	}

	// Hot run: fresh collector and fresh storage, same cache dir.
	c2 := NewCollector(CacheDir(cacheDir))
	run2 := collectCacheRun(c2)
	if err := c2.Visit(startURL); err != nil {
		t.Fatalf("hot visit failed: %s", err)
	}
	if got := s.requests(); got != 3 {
		t.Fatalf("hot run made %d origin requests, want 0 (cache hit)", got-3)
	}

	// The page origin must not change on a cache hit.
	if want := s.URL + "/c/final"; run2.finalURL != want {
		t.Errorf("hot run final URL = %q, want %q", run2.finalURL, want)
	}
	if run1.finalURL != run2.finalURL {
		t.Errorf("final URL mismatch: cold %q, hot %q", run1.finalURL, run2.finalURL)
	}
	// Relative links must resolve against the final (post-redirect) URL.
	if want := s.URL + "/c/rel/page"; run2.absLink != want {
		t.Errorf("hot run absolute link = %q, want %q", run2.absLink, want)
	}
	if run1.absLink != run2.absLink {
		t.Errorf("absolute link mismatch: cold %q, hot %q", run1.absLink, run2.absLink)
	}
	if run2.body != run1.body {
		t.Errorf("body mismatch: cold %q, hot %q", run1.body, run2.body)
	}
	// Callback order must be identical, with OnResponseHeaders exactly once.
	wantEvents := []string{"request", "responseHeaders", "response", "html", "scraped"}
	if fmt.Sprint(run1.events) != fmt.Sprint(wantEvents) {
		t.Errorf("cold run events = %v, want %v", run1.events, wantEvents)
	}
	if fmt.Sprint(run2.events) != fmt.Sprint(run1.events) {
		t.Errorf("event order mismatch: cold %v, hot %v", run1.events, run2.events)
	}
	if run2.headersCalls != 1 {
		t.Errorf("hot run OnResponseHeaders called %d times, want 1", run2.headersCalls)
	}
	// Dedup state must match: every redirect hop is marked visited.
	for _, u := range []string{"/a/start", "/b/mid", "/c/final"} {
		for i, c := range []*Collector{c1, c2} {
			visited, err := c.HasVisited(s.URL + u)
			if err != nil || !visited {
				t.Errorf("run %d: HasVisited(%q) = %v, %v; want true, nil", i+1, u, visited, err)
			}
		}
	}
}

// TestCacheRedirectXMLCallbackURL checks that XML callbacks observe the
// final post-redirect URL on a cache hit as well.
func TestCacheRedirectXMLCallbackURL(t *testing.T) {
	s := newRedirectTestServer()
	defer s.Close()
	cacheDir := t.TempDir()

	var coldURL, coldLink, hotURL, hotLink string
	c1 := NewCollector(CacheDir(cacheDir))
	c1.OnXML("//a", func(e *XMLElement) {
		coldURL = e.Request.URL.String()
		coldLink = e.Request.AbsoluteURL(e.Attr("href"))
	})
	if err := c1.Visit(s.URL + "/x/start"); err != nil {
		t.Fatalf("cold visit failed: %s", err)
	}

	c2 := NewCollector(CacheDir(cacheDir))
	c2.OnXML("//a", func(e *XMLElement) {
		hotURL = e.Request.URL.String()
		hotLink = e.Request.AbsoluteURL(e.Attr("href"))
	})
	if err := c2.Visit(s.URL + "/x/start"); err != nil {
		t.Fatalf("hot visit failed: %s", err)
	}
	if got := s.requests(); got != 2 {
		t.Fatalf("origin requests = %d, want 2 (cold fetches both hops, hot run must hit cache)", got)
	}
	if want := s.URL + "/x/final"; hotURL != want {
		t.Errorf("hot XML request URL = %q, want %q", hotURL, want)
	}
	if want := s.URL + "/x/rel/x"; hotLink != want {
		t.Errorf("hot XML absolute link = %q, want %q", hotLink, want)
	}
	if coldURL != hotURL || coldLink != hotLink {
		t.Errorf("cold/hot mismatch: (%q, %q) vs (%q, %q)", coldURL, coldLink, hotURL, hotLink)
	}
}

// TestCacheRedirectPolicyChange verifies that a cache entry whose
// redirect chain violates the collector's *current* filters is rejected
// without delivering the body and without contacting the origin, with
// the same error category a live redirect would produce.
func TestCacheRedirectPolicyChange(t *testing.T) {
	s := newRedirectTestServer()
	defer s.Close()
	cacheDir := t.TempDir()
	startURL := s.URL + "/a/start"

	c1 := NewCollector(CacheDir(cacheDir))
	if err := c1.Visit(startURL); err != nil {
		t.Fatalf("cold visit failed: %s", err)
	}
	if got := s.requests(); got != 3 {
		t.Fatalf("cold run made %d origin requests, want 3", got)
	}

	// URL filter policy change: /b/mid is now disallowed.
	c2 := NewCollector(
		CacheDir(cacheDir),
		DisallowedURLFilters(regexp.MustCompile(`/b/mid`)),
	)
	var responseCalls, scrapedCalls int
	c2.OnResponse(func(r *Response) { responseCalls++ })
	c2.OnScraped(func(r *Response) { scrapedCalls++ })
	err := c2.Visit(startURL)
	if !errors.Is(err, ErrForbiddenURL) {
		t.Errorf("URL filter rejection = %v, want ErrForbiddenURL", err)
	}
	if responseCalls != 0 || scrapedCalls != 0 {
		t.Errorf("rejected cache hit delivered body: OnResponse=%d OnScraped=%d", responseCalls, scrapedCalls)
	}
	if got := s.requests(); got != 3 {
		t.Errorf("rejected cache hit contacted origin: %d requests, want 3 total", got)
	}

	// Domain policy change: the redirect target's domain is now forbidden.
	c3 := NewCollector(
		CacheDir(cacheDir),
		AllowedDomains("example.com"),
	)
	err = c3.Visit(startURL)
	if !errors.Is(err, ErrForbiddenDomain) {
		t.Errorf("domain filter rejection = %v, want ErrForbiddenDomain", err)
	}
	if got := s.requests(); got != 3 {
		t.Errorf("rejected cache hit contacted origin: %d requests, want 3 total", got)
	}
}

// TestCacheRedirectAbort verifies that aborting from OnResponseHeaders on
// a cache hit takes the original error path: ErrAbortedAfterHeaders,
// exactly one OnResponseHeaders call, and no body callbacks or OnScraped.
func TestCacheRedirectAbort(t *testing.T) {
	s := newRedirectTestServer()
	defer s.Close()
	cacheDir := t.TempDir()
	startURL := s.URL + "/a/start"

	c1 := NewCollector(CacheDir(cacheDir))
	if err := c1.Visit(startURL); err != nil {
		t.Fatalf("cold visit failed: %s", err)
	}

	c2 := NewCollector(CacheDir(cacheDir))
	var headersCalls, responseCalls, htmlCalls, scrapedCalls int
	c2.OnResponseHeaders(func(r *Response) {
		headersCalls++
		r.Request.Abort()
	})
	c2.OnResponse(func(r *Response) { responseCalls++ })
	c2.OnHTML("a", func(e *HTMLElement) { htmlCalls++ })
	c2.OnScraped(func(r *Response) { scrapedCalls++ })
	err := c2.Visit(startURL)
	if !errors.Is(err, ErrAbortedAfterHeaders) {
		t.Errorf("abort error = %v, want ErrAbortedAfterHeaders", err)
	}
	if headersCalls != 1 {
		t.Errorf("OnResponseHeaders called %d times, want 1", headersCalls)
	}
	if responseCalls != 0 || htmlCalls != 0 || scrapedCalls != 0 {
		t.Errorf("callbacks after abort: OnResponse=%d OnHTML=%d OnScraped=%d, want all 0",
			responseCalls, htmlCalls, scrapedCalls)
	}
	if got := s.requests(); got != 3 {
		t.Errorf("aborted cache hit contacted origin: %d requests, want 3 total", got)
	}
}

// TestCacheRedirectDedup verifies that the revisit rules are re-checked
// against the redirect chain on a cache hit: visiting the redirect target
// directly first makes a later cached visit fail with AlreadyVisitedError,
// exactly as a live redirect would.
func TestCacheRedirectDedup(t *testing.T) {
	s := newRedirectTestServer()
	defer s.Close()
	cacheDir := t.TempDir()

	c1 := NewCollector(CacheDir(cacheDir))
	if err := c1.Visit(s.URL + "/a/start"); err != nil {
		t.Fatalf("cold visit failed: %s", err)
	}
	if got := s.requests(); got != 3 {
		t.Fatalf("cold run made %d origin requests, want 3", got)
	}

	// Hot run: visit the final URL directly, then the cached start URL.
	c2 := NewCollector(CacheDir(cacheDir))
	if err := c2.Visit(s.URL + "/c/final"); err != nil {
		t.Fatalf("direct visit failed: %s", err)
	}
	err := c2.Visit(s.URL + "/a/start")
	var ave *AlreadyVisitedError
	if !errors.As(err, &ave) {
		t.Fatalf("hot dedup rejection = %v, want AlreadyVisitedError", err)
	}
	if got := s.requests(); got != 4 {
		t.Errorf("hot dedup rejection contacted origin: %d requests, want 4 total", got)
	}

	// Live comparison: same scenario without a cache must produce the
	// same error category and the same dedup state.
	c3 := NewCollector()
	if err := c3.Visit(s.URL + "/c/final"); err != nil {
		t.Fatalf("live direct visit failed: %s", err)
	}
	err = c3.Visit(s.URL + "/a/start")
	if !errors.As(err, &ave) {
		t.Fatalf("live dedup rejection = %v, want AlreadyVisitedError", err)
	}
	for _, u := range []string{"/b/mid", "/c/final"} {
		hot, _ := c2.HasVisited(s.URL + u)
		live, _ := c3.HasVisited(s.URL + u)
		if hot != live {
			t.Errorf("dedup state mismatch for %q: hot=%v live=%v", u, hot, live)
		}
	}
}

// TestCacheRedirectCustomHandler verifies that a custom redirect handler
// is never replayed from cache: the request falls back to the origin so
// the handler observes the live redirect chain.
func TestCacheRedirectCustomHandler(t *testing.T) {
	s := newRedirectTestServer()
	defer s.Close()
	cacheDir := t.TempDir()
	startURL := s.URL + "/a/start"

	c1 := NewCollector(CacheDir(cacheDir))
	if err := c1.Visit(startURL); err != nil {
		t.Fatalf("cold visit failed: %s", err)
	}
	if got := s.requests(); got != 3 {
		t.Fatalf("cold run made %d origin requests, want 3", got)
	}

	c2 := NewCollector(CacheDir(cacheDir))
	var handlerCalls int
	var finalURL string
	c2.SetRedirectHandler(func(req *http.Request, via []*http.Request) error {
		handlerCalls++
		return nil
	})
	c2.OnResponse(func(r *Response) { finalURL = r.Request.URL.String() })
	if err := c2.Visit(startURL); err != nil {
		t.Fatalf("hot visit with custom handler failed: %s", err)
	}
	if handlerCalls != 2 {
		t.Errorf("custom redirect handler called %d times, want 2 (one per hop)", handlerCalls)
	}
	if got := s.requests(); got != 6 {
		t.Errorf("custom handler run made %d origin requests, want 6 total (refetch)", got)
	}
	if want := s.URL + "/c/final"; finalURL != want {
		t.Errorf("final URL = %q, want %q", finalURL, want)
	}
}

// TestCacheOldFormatFallback verifies that cache entries written in the
// old format (bare gob-encoded Response) and corrupted entries safely
// fall back to the origin server, and are rewritten in the new format.
func TestCacheOldFormatFallback(t *testing.T) {
	s := newRedirectTestServer()
	defer s.Close()
	cacheDir := t.TempDir()
	startURL := s.URL + "/a/start"

	// Write an old-format entry: a bare gob-encoded Response.
	sum := sha1.Sum([]byte(startURL))
	hash := hex.EncodeToString(sum[:])
	dir := path.Join(cacheDir, hash[:2])
	if err := os.MkdirAll(dir, 0750); err != nil {
		t.Fatal(err)
	}
	cacheFile := path.Join(dir, hash)
	f, err := os.Create(cacheFile)
	if err != nil {
		t.Fatal(err)
	}
	oldEntry := &Response{
		StatusCode: 200,
		Body:       []byte("stale cached body"),
		Headers:    &http.Header{"Content-Type": []string{"text/html"}},
	}
	if err := gob.NewEncoder(f).Encode(oldEntry); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// The old-format entry must not be served; the origin is contacted
	// and the entry is rewritten in the new format.
	c1 := NewCollector(CacheDir(cacheDir))
	var body, finalURL string
	c1.OnResponse(func(r *Response) {
		body = string(r.Body)
		finalURL = r.Request.URL.String()
	})
	if err := c1.Visit(startURL); err != nil {
		t.Fatalf("visit with old-format entry failed: %s", err)
	}
	if got := s.requests(); got != 3 {
		t.Errorf("old-format entry made %d origin requests, want 3 (fallback)", got)
	}
	if body == "stale cached body" {
		t.Error("old-format entry was served instead of falling back to origin")
	}
	if want := s.URL + "/c/final"; finalURL != want {
		t.Errorf("final URL = %q, want %q", finalURL, want)
	}

	// The rewritten entry is now served from cache.
	c2 := NewCollector(CacheDir(cacheDir))
	c2.OnResponse(func(r *Response) {
		body = string(r.Body)
		finalURL = r.Request.URL.String()
	})
	if err := c2.Visit(startURL); err != nil {
		t.Fatalf("visit with rewritten entry failed: %s", err)
	}
	if got := s.requests(); got != 3 {
		t.Errorf("rewritten entry made %d origin requests, want 3 total (cache hit)", got)
	}
	if want := s.URL + "/c/final"; finalURL != want {
		t.Errorf("final URL from rewritten entry = %q, want %q", finalURL, want)
	}

	// A corrupted entry falls back to the origin as well.
	if err := os.WriteFile(cacheFile, []byte("not a gob stream"), 0644); err != nil {
		t.Fatal(err)
	}
	c3 := NewCollector(CacheDir(cacheDir))
	var body3 string
	c3.OnResponse(func(r *Response) { body3 = string(r.Body) })
	if err := c3.Visit(startURL); err != nil {
		t.Fatalf("visit with corrupted entry failed: %s", err)
	}
	if got := s.requests(); got != 6 {
		t.Errorf("corrupted entry made %d origin requests, want 6 total (fallback)", got)
	}
	if body3 != body {
		t.Errorf("corrupted fallback body = %q, want %q", body3, body)
	}
}

// TestCacheNoCacheHeaderPreserved verifies that a Cache-Control: no-cache
// request header still bypasses the cache entirely.
func TestCacheNoCacheHeaderPreserved(t *testing.T) {
	s := newRedirectTestServer()
	defer s.Close()
	cacheDir := t.TempDir()
	startURL := s.URL + "/a/start"

	c1 := NewCollector(CacheDir(cacheDir))
	if err := c1.Visit(startURL); err != nil {
		t.Fatalf("cold visit failed: %s", err)
	}
	if got := s.requests(); got != 3 {
		t.Fatalf("cold run made %d origin requests, want 3", got)
	}

	hdr := http.Header{"Cache-Control": []string{"no-cache"}}
	c2 := NewCollector(CacheDir(cacheDir), AllowURLRevisit())
	for i := 0; i < 2; i++ {
		if err := c2.Request("GET", startURL, nil, nil, hdr); err != nil {
			t.Fatalf("no-cache request %d failed: %s", i, err)
		}
	}
	if got := s.requests(); got != 9 {
		t.Errorf("no-cache requests made %d origin requests, want 9 total (3+3+3)", got)
	}
}

// TestCacheExpirationPreserved verifies that expired entries are still
// refreshed from the origin.
func TestCacheExpirationPreserved(t *testing.T) {
	s := newRedirectTestServer()
	defer s.Close()
	cacheDir := t.TempDir()
	startURL := s.URL + "/a/start"

	c1 := NewCollector(CacheDir(cacheDir), CacheExpiration(time.Hour))
	if err := c1.Visit(startURL); err != nil {
		t.Fatalf("cold visit failed: %s", err)
	}
	if got := s.requests(); got != 3 {
		t.Fatalf("cold run made %d origin requests, want 3", got)
	}

	// Age the cache file beyond the expiration.
	sum := sha1.Sum([]byte(startURL))
	hash := hex.EncodeToString(sum[:])
	cacheFile := path.Join(cacheDir, hash[:2], hash)
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(cacheFile, old, old); err != nil {
		t.Fatal(err)
	}

	c2 := NewCollector(CacheDir(cacheDir), CacheExpiration(time.Hour))
	if err := c2.Visit(startURL); err != nil {
		t.Fatalf("visit with expired entry failed: %s", err)
	}
	if got := s.requests(); got != 6 {
		t.Errorf("expired entry made %d origin requests, want 6 total (refresh)", got)
	}
}

// TestCacheRedirectNoCookieReplay verifies that a cache hit does not
// replay cookie side effects: Set-Cookie headers of the cached redirect
// chain are delivered in Response.Headers (as in a live fetch) but are
// not applied to the cookie jar again.
func TestCacheRedirectNoCookieReplay(t *testing.T) {
	s := newRedirectTestServer()
	defer s.Close()
	cacheDir := t.TempDir()
	startURL := s.URL + "/cookie/start"

	// Cold run: the live redirect sets the cookie in the jar.
	c1 := NewCollector(CacheDir(cacheDir))
	if err := c1.Visit(startURL); err != nil {
		t.Fatalf("cold visit failed: %s", err)
	}
	if cookies := c1.Cookies(s.URL + "/cookie/final"); len(cookies) != 1 {
		t.Fatalf("cold run jar has %d cookies, want 1", len(cookies))
	}
	if got := s.requests(); got != 2 {
		t.Fatalf("cold run made %d origin requests, want 2", got)
	}

	// Hot run with a fresh jar: the cache hit must not replay the
	// Set-Cookie side effect into the jar.
	c2 := NewCollector(CacheDir(cacheDir))
	var setCookie string
	c2.OnResponse(func(r *Response) {
		setCookie = r.Headers.Get("Set-Cookie")
	})
	if err := c2.Visit(startURL); err != nil {
		t.Fatalf("hot visit failed: %s", err)
	}
	if got := s.requests(); got != 2 {
		t.Fatalf("hot run made %d origin requests, want 2 total (cache hit)", got)
	}
	if cookies := c2.Cookies(s.URL + "/cookie/final"); len(cookies) != 0 {
		t.Errorf("hot run jar has %d cookies, want 0 (no cookie replay)", len(cookies))
	}
	if setCookie == "" {
		t.Error("hot run response lost the cached Set-Cookie header")
	}
}

// TestCacheRedirectCrossProcess verifies that a cache entry written by
// one process is served correctly by another process: the final URL and
// relative link resolution match, and no origin requests are made.
//
// The test re-executes itself in helper mode (CACHETEST_HELPER=1) so
// the two runs share nothing but the cache directory.
func TestCacheRedirectCrossProcess(t *testing.T) {
	if os.Getenv("CACHETEST_HELPER") == "1" {
		c := NewCollector(CacheDir(os.Getenv("CACHETEST_DIR")))
		c.OnResponse(func(r *Response) {
			fmt.Println("URL:", r.Request.URL.String())
		})
		c.OnHTML("a", func(e *HTMLElement) {
			fmt.Println("LINK:", e.Request.AbsoluteURL(e.Attr("href")))
		})
		if err := c.Visit(os.Getenv("CACHETEST_URL")); err != nil {
			fmt.Println("ERR:", err)
			os.Exit(1)
		}
		return
	}

	s := newRedirectTestServer()
	defer s.Close()
	cacheDir := t.TempDir()

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	run := func() string {
		cmd := exec.Command(exe, "-test.run", "^TestCacheRedirectCrossProcess$")
		cmd.Env = append(os.Environ(),
			"CACHETEST_HELPER=1",
			"CACHETEST_DIR="+cacheDir,
			"CACHETEST_URL="+s.URL+"/a/start",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("helper process failed: %s\n%s", err, out)
		}
		return string(out)
	}

	out1 := run()
	if got := s.requests(); got != 3 {
		t.Fatalf("cold process made %d origin requests, want 3", got)
	}
	out2 := run()
	if got := s.requests(); got != 3 {
		t.Fatalf("hot process made %d origin requests, want 3 total (cache hit)", got)
	}
	if out1 != out2 {
		t.Errorf("process outputs differ:\ncold:\n%s\nhot:\n%s", out1, out2)
	}
	for _, want := range []string{
		"URL: " + s.URL + "/c/final\n",
		"LINK: " + s.URL + "/c/rel/page\n",
	} {
		if !strings.Contains(out2, want) {
			t.Errorf("hot process output %q missing %q", out2, want)
		}
	}
}
