package main

import (
	"fmt"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/debug"
)

func main() {
	url := "https://httpbin.org/delay/1"

	// Instantiate default collector
	c := colly.NewCollector(
		colly.Debugger(&debug.LogDebugger{}),
		colly.Async(),
	)

	// Hop-by-hop rate limiting: every HTTP hop (robots.txt, redirects
	// and retries included) counts against Parallelism and is matched
	// by its target host. A slot is held until the response body closes,
	// then the rule cools down for Delay plus a random fraction of
	// RandomDelay before the next hop may be sent.
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*httpbin.*",
		Parallelism: 2,
		Delay:       100 * time.Millisecond,
		RandomDelay: 50 * time.Millisecond,
		Hop:         true,
	})

	c.OnResponse(func(r *colly.Response) {
		fmt.Println("visited", r.Request.URL)
	})

	for i := 0; i < 5; i++ {
		c.Visit(fmt.Sprintf("%s?n=%d", url, i))
	}
	c.Wait()
}
