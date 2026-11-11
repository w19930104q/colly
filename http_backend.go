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
	"bufio"
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"compress/gzip"

	"github.com/gobwas/glob"
)

type httpBackend struct {
	LimitRules []*LimitRule
	Client     *http.Client
	lock       *sync.RWMutex
}

type checkResponseHeadersFunc func(req *http.Request, statusCode int, header http.Header) bool
type checkRequestHeadersFunc func(req *http.Request) bool

// checkCachedRedirectsFunc re-validates the redirect chain recorded in a
// cache entry against the collector's current configuration. It returns
// refetch=true when the chain cannot be replayed from cache (e.g. a
// custom redirect handler is set) and the request must be sent to the
// origin server instead. A non-nil error rejects the cached response:
// neither the cached body is delivered nor the origin server contacted.
type checkCachedRedirectsFunc func(hops []string) (refetch bool, err error)

// cachedResponse is the on-disk cache entry format for GET responses.
// It extends Response with the final (post-redirect) request URL and the
// redirect chain so that a cache hit can reproduce the same Request.URL
// (and thus the same relative-link resolution) as a live fetch, and can
// re-validate every redirect hop against the collector's current
// domain/URL filters and revisit rules.
//
// Cache files written by older versions contain a bare gob-encoded
// Response. Those decode into cachedResponse with an empty FinalURL and
// are treated as misses: the request falls back to the origin server and
// the entry is rewritten in the new format.
type cachedResponse struct {
	Response
	// FinalURL is the request URL after all redirects have been followed.
	FinalURL string
	// Hops contains the redirect target URLs in the order they were
	// followed. It is empty when no redirect occurred.
	Hops []string
}

// LimitRule provides connection restrictions for domains.
// Both DomainRegexp and DomainGlob can be used to specify
// the included domains patterns, but at least one is required.
// There can be two kind of limitations:
//   - Parallelism: Set limit for the number of concurrent requests to matching domains
//   - Delay: Wait specified amount of time between requests (parallelism is 1 in this case)
type LimitRule struct {
	// DomainRegexp is a regular expression to match against domains
	DomainRegexp string
	// DomainGlob is a glob pattern to match against domains
	DomainGlob string
	// Delay is the duration to wait before creating a new request to the matching domains
	Delay time.Duration
	// RandomDelay is the extra randomized duration to wait added to Delay before creating a new request
	RandomDelay time.Duration
	// Parallelism is the number of the maximum allowed concurrent requests of the matching domains
	Parallelism    int
	waitChan       chan bool
	compiledRegexp *regexp.Regexp
	compiledGlob   *glob.Pattern
	lock           sync.Mutex
}

// Init initializes the private members of LimitRule.
//
// Init is idempotent: once a LimitRule has been successfully initialized,
// subsequent calls are no-ops. This makes it safe to share a single
// *LimitRule across multiple Collectors via Collector.Limit — without this,
// the second Collector's Limit() call would (a) replace waitChan and
// orphan the first Collector's already-acquired slots, deadlocking its
// in-flight requests' defer <-r.waitChan, and (b) race with concurrent
// Match() reads of compiledRegexp / compiledGlob.
func (r *LimitRule) Init() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.waitChan != nil {
		return nil
	}
	hasPattern := false
	if r.DomainRegexp != "" {
		c, err := regexp.Compile(r.DomainRegexp)
		if err != nil {
			return err
		}
		r.compiledRegexp = c
		hasPattern = true
	}
	if r.DomainGlob != "" {
		c, err := glob.Compile(r.DomainGlob)
		if err != nil {
			return err
		}
		r.compiledGlob = c
		hasPattern = true
	}
	if !hasPattern {
		return ErrNoPattern
	}
	r.waitChan = make(chan bool, max(r.Parallelism, 1))
	return nil
}

// Clone returns a shallow copy of the LimitRule containing only its
// exported fields. Unexported state (waitChan, compiled patterns, lock)
// is left zero so the returned rule is independent of the original and
// must be re-initialized via Init before use.
func (r *LimitRule) Clone() *LimitRule {
	return &LimitRule{
		DomainRegexp: r.DomainRegexp,
		DomainGlob:   r.DomainGlob,
		Delay:        r.Delay,
		RandomDelay:  r.RandomDelay,
		Parallelism:  r.Parallelism,
	}
}

func (h *httpBackend) Init(jar http.CookieJar) {
	h.Client = &http.Client{
		Jar:     jar,
		Timeout: 10 * time.Second,
	}
	h.lock = &sync.RWMutex{}
}

// Match checks that the domain parameter triggers the rule
func (r *LimitRule) Match(domain string) bool {
	match := false
	if r.compiledRegexp != nil && r.compiledRegexp.MatchString(domain) {
		match = true
	}
	if r.compiledGlob != nil && r.compiledGlob.Match(domain) {
		match = true
	}
	return match
}

func (h *httpBackend) GetMatchingRule(domain string) *LimitRule {
	if h.LimitRules == nil {
		return nil
	}
	h.lock.RLock()
	defer h.lock.RUnlock()
	for _, r := range h.LimitRules {
		if r.Match(domain) {
			return r
		}
	}
	return nil
}

func (h *httpBackend) Cache(request *http.Request, bodySize int, checkRequestHeadersFunc checkRequestHeadersFunc, checkResponseHeadersFunc checkResponseHeadersFunc, checkCachedRedirectsFunc checkCachedRedirectsFunc, cacheDir string, cacheExpiration time.Duration) (*Response, error) {
	if cacheDir == "" || request.Method != "GET" || request.Header.Get("Cache-Control") == "no-cache" {
		resp, _, err := h.Do(request, bodySize, checkRequestHeadersFunc, checkResponseHeadersFunc)
		return resp, err
	}
	sum := sha1.Sum([]byte(request.URL.String()))
	hash := hex.EncodeToString(sum[:])
	dir := path.Join(cacheDir, hash[:2])
	filename := path.Join(dir, hash)

	if fileInfo, err := os.Stat(filename); err == nil && cacheExpiration > 0 {
		if time.Since(fileInfo.ModTime()) > cacheExpiration {
			_ = os.Remove(filename)
		}
	}

	if file, err := os.Open(filename); err == nil {
		entry := new(cachedResponse)
		decodeErr := gob.NewDecoder(file).Decode(entry)
		file.Close()
		// Old-format entries (bare Response, no FinalURL), corrupted
		// entries and 5xx responses are treated as misses and refreshed
		// from the origin server below.
		if decodeErr == nil && entry.FinalURL != "" && entry.Headers != nil && entry.StatusCode < 500 {
			refetch, err := checkCachedRedirectsFunc(entry.Hops)
			if err != nil {
				// A redirect hop recorded in the entry is rejected by
				// the collector's current configuration: deliver
				// neither the cached body nor a fresh request.
				return nil, err
			}
			if !refetch {
				// Replay the final request URL so that Request.URL,
				// and with it relative link resolution in the HTML/XML
				// callbacks, matches a live fetch.
				finalRequest := request
				if entry.FinalURL != request.URL.String() {
					if u, err := url.Parse(entry.FinalURL); err == nil {
						r2 := new(http.Request)
						*r2 = *request
						r2.URL = u
						finalRequest = r2
					}
				}
				if !checkResponseHeadersFunc(finalRequest, entry.StatusCode, *entry.Headers) {
					return nil, ErrAbortedAfterHeaders
				}
				return &Response{
					StatusCode: entry.StatusCode,
					Body:       entry.Body,
					Headers:    entry.Headers,
				}, nil
			}
		}
	}
	resp, finalURL, err := h.Do(request, bodySize, checkRequestHeadersFunc, checkResponseHeadersFunc)
	if err != nil || resp.StatusCode >= 500 {
		return resp, err
	}
	if _, err := os.Stat(dir); err != nil {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return resp, err
		}
	}
	file, err := os.Create(filename + "~")
	if err != nil {
		return resp, err
	}
	entry := &cachedResponse{Response: *resp}
	if finalURL != nil {
		entry.FinalURL = finalURL.String()
	} else {
		entry.FinalURL = request.URL.String()
	}
	if chain, ok := request.Context().Value(redirectHopsKey).(*redirectChain); ok {
		entry.Hops = chain.hops
	}
	if err := gob.NewEncoder(file).Encode(entry); err != nil {
		file.Close()
		return resp, err
	}
	file.Close()
	return resp, os.Rename(filename+"~", filename)
}

func (h *httpBackend) Do(request *http.Request, bodySize int, checkRequestHeadersFunc checkRequestHeadersFunc, checkResponseHeadersFunc checkResponseHeadersFunc) (*Response, *url.URL, error) {
	r := h.GetMatchingRule(request.URL.Host)
	if r != nil {
		r.waitChan <- true
		defer func(r *LimitRule) {
			randomDelay := time.Duration(0)
			if r.RandomDelay != 0 {
				randomDelay = time.Duration(rand.Int63n(int64(r.RandomDelay)))
			}
			time.Sleep(r.Delay + randomDelay)
			<-r.waitChan
		}(r)
	}
	if !checkRequestHeadersFunc(request) {
		return nil, nil, ErrAbortedBeforeRequest
	}
	res, err := h.Client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()

	finalRequest := request
	if res.Request != nil {
		finalRequest = res.Request
	}
	if !checkResponseHeadersFunc(finalRequest, res.StatusCode, res.Header) {
		// closing res.Body (see defer above) without reading it aborts
		// the download
		return nil, nil, ErrAbortedAfterHeaders
	}

	var bodyReader io.Reader = res.Body
	if bodySize > 0 {
		bodyReader = io.LimitReader(bodyReader, int64(bodySize))
	}
	contentEncoding := strings.ToLower(res.Header.Get("Content-Encoding"))
	if !res.Uncompressed && (strings.Contains(contentEncoding, "gzip") || (contentEncoding == "" && strings.Contains(strings.ToLower(res.Header.Get("Content-Type")), "gzip")) || (strings.HasSuffix(strings.ToLower(finalRequest.URL.Path), ".xml.gz") && res.StatusCode >= 200 && res.StatusCode < 300)) {
		// Even if URL contains .xml.gz, it doesn't mean that we get gzip
		// compressed data back. We might get 404 error page instead,
		// for example. So check gzip magic bytes.
		bufReader := bufio.NewReader(bodyReader)
		bodyReader = bufReader
		magic, err := bufReader.Peek(2)
		switch err {
		case io.EOF:
			// less than 2 bytes, do nothing
		case nil:
			// gzip magic, as specified in RFC 1952
			if magic[0] == 0x1f && magic[1] == 0x8b {
				bodyReader, err = gzip.NewReader(bufReader)
				if err != nil {
					return nil, nil, err
				}
				defer bodyReader.(*gzip.Reader).Close()
			}
		default:
			return nil, nil, err
		}
	}
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return nil, nil, err
	}
	return &Response{
		StatusCode: res.StatusCode,
		Body:       body,
		Headers:    &res.Header,
	}, finalRequest.URL, nil
}

func (h *httpBackend) Limit(rule *LimitRule) error {
	h.lock.Lock()
	if h.LimitRules == nil {
		h.LimitRules = make([]*LimitRule, 0, 8)
	}
	h.LimitRules = append(h.LimitRules, rule)
	h.lock.Unlock()
	return rule.Init()
}

func (h *httpBackend) Limits(rules []*LimitRule) error {
	for _, r := range rules {
		if err := h.Limit(r); err != nil {
			return err
		}
	}
	return nil
}
