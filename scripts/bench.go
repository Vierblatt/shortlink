package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type shortenResp struct {
	Code     string `json:"code"`
	ShortURL string `json:"short_url"`
}

func main() {
	baseURL := "http://localhost:8888"

	// 1. Create a short link with custom code for benchmarking
	body := `{"url":"https://example.com","custom_code":"bench"}`
	resp, err := http.Post(
		baseURL+"/api/shorten",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		fmt.Printf("ERROR: server not reachable — %v\n", err)
		return
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	var sr shortenResp
	json.Unmarshal(data, &sr)
	code := sr.Code
	if code == "" {
		code = "bench"
	}
	fmt.Printf("bench code: %s\n", code)

	// 2. Warm up
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for i := 0; i < 200; i++ {
		client.Get(baseURL + "/" + code)
	}

	// 3. Bench: concurrent redirect requests
	concurrency := 100
	totalReqs := 10000
	var wg sync.WaitGroup
	var mu sync.Mutex
	var latencies []time.Duration
	var errors int

	start := time.Now()
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli := &http.Client{
				CheckRedirect: func(req *http.Request, via []*http.Request) error {
					return http.ErrUseLastResponse
				},
			}
			for i := 0; i < totalReqs/concurrency; i++ {
				t0 := time.Now()
				resp, err := cli.Get(baseURL + "/" + code)
				lat := time.Since(t0)
				if err != nil || resp.StatusCode != 302 {
					mu.Lock()
					errors++
					mu.Unlock()
				}
				if resp != nil {
					resp.Body.Close()
				}
				mu.Lock()
				latencies = append(latencies, lat)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	qps := float64(totalReqs) / elapsed.Seconds()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := len(latencies)
	p50 := latencies[n*50/100]
	p95 := latencies[n*95/100]
	p99 := latencies[n*99/100]

	fmt.Printf("=== benchmark results ===\n")
	fmt.Printf("total_requests: %d\n", totalReqs)
	fmt.Printf("concurrency:    %d\n", concurrency)
	fmt.Printf("duration:       %.2fs\n", elapsed.Seconds())
	fmt.Printf("qps:            %.0f\n", qps)
	fmt.Printf("errors:         %d\n", errors)
	fmt.Printf("p50:            %.2fms\n", float64(p50.Microseconds())/1000)
	fmt.Printf("p95:            %.2fms\n", float64(p95.Microseconds())/1000)
	fmt.Printf("p99:            %.2fms\n", float64(p99.Microseconds())/1000)
}
