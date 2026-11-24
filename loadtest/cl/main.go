package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "", "")
	c := flag.Int("c", 50, "")
	d := flag.Duration("d", 10*time.Second, "")
	gz := flag.Bool("gzip", false, "")
	flag.Parse()
	tr := &http.Transport{MaxIdleConnsPerHost: *c, DisableCompression: true}
	cl := &http.Client{Transport: tr}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var lat []time.Duration
	var bytes, errs, non200 int64
	end := time.Now().Add(*d)
	for i := 0; i < *c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(end) {
				req, _ := http.NewRequest("GET", *url, nil)
				if *gz {
					req.Header.Set("Accept-Encoding", "gzip")
				}
				t := time.Now()
				resp, err := cl.Do(req)
				if err != nil {
					atomic.AddInt64(&errs, 1)
					continue
				}
				n, err := io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if err != nil {
					atomic.AddInt64(&errs, 1)
					continue
				}
				if resp.StatusCode != 200 {
					atomic.AddInt64(&non200, 1)
				}
				atomic.AddInt64(&bytes, n)
				mu.Lock()
				lat = append(lat, time.Since(t))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p := func(q float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(float64(len(lat)-1)*q)]
	}
	fmt.Printf("reqs=%d rps=%.0f MB/s=%.0f errs=%d non200=%d p50=%v p95=%v p99=%v max=%v\n", len(lat), float64(len(lat))/d.Seconds(), float64(bytes)/1e6/d.Seconds(), errs, non200, p(.5), p(.95), p(.99), p(1))
}
