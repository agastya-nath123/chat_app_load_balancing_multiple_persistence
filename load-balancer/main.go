package main

import (
	"math"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)
const consecutiveFailureThreshold = 3

type Backend struct {
	URL       *url.URL
	APIURL    *url.URL
	Alive     atomic.Bool
	InFlight  atomic.Int64
	CPUPercent    atomic.Uint64
	MemoryPercent atomic.Uint64
	ConsecFailures atomic.Int32
}

type LoadBalancer struct {
	backends []*Backend
	metrics  Metrics
	CPUThreshold float64
}

type Metrics struct {
	Total         atomic.Uint64
	Success       atomic.Uint64
	Failed        atomic.Uint64
	BackendErrors atomic.Uint64

	LatencyMu sync.Mutex
	Latencies []time.Duration
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if r.statusCode == 0 {
		r.statusCode = http.StatusOK
	}

	return r.ResponseWriter.Write(body)
}

func (m *Metrics) recordLatency(d time.Duration) {
	m.LatencyMu.Lock()
	m.Latencies = append(m.Latencies, d)
	m.LatencyMu.Unlock()
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}

	index := int(float64(len(values)-1) * p)

	return values[index]
}

func (m *Metrics) percentiles() (
	time.Duration,
	time.Duration,
	time.Duration,
) {
	m.LatencyMu.Lock()
	defer m.LatencyMu.Unlock()

	if len(m.Latencies) == 0 {
		return 0, 0, 0
	}

	values := append([]time.Duration(nil), m.Latencies...)

	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})

	p50 := percentile(values, 0.50)
	p95 := percentile(values, 0.95)
	p99 := percentile(values, 0.99)

	return p50, p95, p99
}

const maxExpectedInFlight = 50.0

func backendScore(backend *Backend) float64 {
    cpu := float64(backend.CPUPercent.Load()) / 100.0
    memory := float64(backend.MemoryPercent.Load()) / 100.0
	inFlight := math.Min(float64(backend.InFlight.Load())/maxExpectedInFlight, 1.0)

    return cpu*0.7 + memory*0.2 + inFlight*0.1
}

func (lb *LoadBalancer) nextBackend() *Backend {
    var best *Backend
    bestScore := math.Inf(1)

    for _, backend := range lb.backends {
        if !backend.Alive.Load() {
            continue
        }

        cpu := float64(backend.CPUPercent.Load()) / 100.0

        // Prefer backends below threshold.
        if cpu >= lb.CPUThreshold {
            continue
        }

        score := backendScore(backend)

        if score < bestScore {
            best = backend
            bestScore = score
        }
    }

    // All healthy backends exceeded threshold.
    // Use the least-loaded healthy backend anyway.
    if best == nil {
        for _, backend := range lb.backends {
            if !backend.Alive.Load() {
                continue
            }

            score := backendScore(backend)

            if best == nil || score < bestScore {
                best = backend
                bestScore = score
            }
        }
    }

    return best
}

func (lb *LoadBalancer) statusHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	type BackendStatus struct {
		URL       string `json:"url"`
		APIURL string `json:"api_url"`
		Alive     bool   `json:"alive"`
		InFlight  int64  `json:"in_flight"`
		CPUPercent    float64 `json:"cpu_percent"`
		MemoryPercent float64 `json:"memory_percent"`
	}

	statuses := make([]BackendStatus, 0, len(lb.backends))

	for _, backend := range lb.backends {
		statuses = append(statuses, BackendStatus{
			URL:       backend.URL.String(),
			APIURL: backend.APIURL.String(),
			Alive:     backend.Alive.Load(),
			InFlight:  backend.InFlight.Load(),
			CPUPercent:    float64(backend.CPUPercent.Load()) / 100,
			MemoryPercent: float64(backend.MemoryPercent.Load()) / 100,
		})
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(statuses)
}

func (lb *LoadBalancer) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.URL.Path == "/lb/health" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
		return
	}
	if r.URL.Path == "/lb/status" {
		lb.statusHandler(w, r)
		return
	}
	if r.URL.Path == "/lb/metrics" {
		lb.metricsHandler(w, r)
		return
	}

	if r.URL.Path != "/message" &&
       r.URL.Path != "/feed" {
        http.NotFound(w, r)
        return
    }

	lb.metrics.Total.Add(1)

	backend := lb.nextBackend()

	if backend == nil {
		lb.metrics.Failed.Add(1)
		http.Error(
			w,
			"no healthy backends",
			http.StatusServiceUnavailable,
		)
		return
	}

	backend.InFlight.Add(1)
	defer backend.InFlight.Add(-1)

	start := time.Now()

	var targetURL *url.URL

	if r.URL.Path == "/message" || r.URL.Path == "/feed" {
		targetURL = backend.APIURL
	} else {
		targetURL = backend.URL
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	proxy.Transport = transport

	proxy.ModifyResponse = func(resp *http.Response) error {
		if resp.StatusCode == http.StatusSwitchingProtocols || (resp.StatusCode >= 200 && resp.StatusCode < 400) {
			lb.metrics.Success.Add(1)
			backend.ConsecFailures.Store(0)
		} else {
			lb.metrics.Failed.Add(1)
		}

		elapsed := time.Since(start)
		lb.metrics.recordLatency(elapsed)

		return nil
	}

	proxy.ErrorHandler = func(
		rw http.ResponseWriter,
		req *http.Request,
		err error,
	) {
		//backend.Alive.Store(false)
		failures := backend.ConsecFailures.Add(1)
		if failures >= consecutiveFailureThreshold {
			if backend.Alive.Swap(false) {
				log.Printf(
					"Backend marked UNHEALTHY after %d consecutive failures: %s",
					failures,
					backend.URL,
				)
			}
		}

		lb.metrics.BackendErrors.Add(1)
		lb.metrics.Failed.Add(1)

		http.Error(
			rw,
			"backend unavailable",
			http.StatusBadGateway,
		)
	}

	log.Printf(
		"%s %s -> %s",
		r.Method,
		r.URL.Path,
		targetURL,
	)

	proxy.ServeHTTP(w, r)

}

//var transport = &http.Transport{
//	TLSClientConfig: &tls.Config{
//		InsecureSkipVerify: true,
//	},
//	MaxIdleConns:        2000,
//	MaxIdleConnsPerHost: 1000,
//	IdleConnTimeout:     90 * time.Second,
//}

var transport = &http.Transport{
	MaxIdleConns:          2000,
	MaxIdleConnsPerHost:   1000,
	IdleConnTimeout:       90 * time.Second,
	ResponseHeaderTimeout: 5 * time.Second,
	DialContext: (&net.Dialer{
		Timeout: 2 * time.Second,
	}).DialContext,
}

type HealthResponse struct {
	Status        string  `json:"status"`
	Backend       string  `json:"backend"`
	CPUPercent    float64 `json:"cpu_percent"`
	MemoryPercent float64 `json:"memory_percent"`
}

func (lb *LoadBalancer) checkBackend(backend *Backend) {
	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	resp, err := client.Get(
		backend.APIURL.String() + "/health",
	)

	if err != nil {
		if backend.Alive.Swap(false) {
			log.Printf(
				"Backend became UNHEALTHY: %s",
				backend.URL,
			)
		}

		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if backend.Alive.Swap(false) {
			log.Printf(
				"Backend became UNHEALTHY: %s (status %d)",
				backend.URL,
				resp.StatusCode,
			)
		}

		return
	}

	var health HealthResponse

	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		if backend.Alive.Swap(false) {
			log.Printf(
				"Backend returned invalid health response: %s",
				backend.URL,
			)
		}

		return
	}

	// Store CPU and memory as hundredths.
	backend.CPUPercent.Store(
		uint64(health.CPUPercent * 100),
	)

	backend.MemoryPercent.Store(
		uint64(health.MemoryPercent * 100),
	)

	backend.ConsecFailures.Store(0)

	if !backend.Alive.Swap(true) {
		log.Printf(
			"Backend became HEALTHY: %s (CPU %.2f%%)",
			backend.URL,
			health.CPUPercent,
		)
	}

	log.Printf(
		"Health: %s | CPU %.2f%% | Memory %.2f%%",
		backend.URL,
		health.CPUPercent,
		health.MemoryPercent,
	)
}

func (lb *LoadBalancer) healthWorker(backend *Backend) {
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()

    for {
        lb.checkBackend(backend)
        <-ticker.C
    }
}

func (lb *LoadBalancer) healthLoop() {
    for _, backend := range lb.backends {
        go lb.healthWorker(backend)
    }

    select {}
}

func (lb *LoadBalancer) metricsHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	p50, p95, p99 := lb.metrics.percentiles()

	response := map[string]interface{}{
		"total":          lb.metrics.Total.Load(),
		"success":        lb.metrics.Success.Load(),
		"failed":         lb.metrics.Failed.Load(),
		"backend_errors": lb.metrics.BackendErrors.Load(),
		"p50_ms":         float64(p50) / float64(time.Millisecond),
		"p95_ms":         float64(p95) / float64(time.Millisecond),
		"p99_ms":         float64(p99) / float64(time.Millisecond),
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(response)
}

func main() {
	rawBackends := flag.String(
		"backends",
		"",
		"Comma-separated backend URLs",
	)

	threshold := flag.Float64(
		"threshold",
		0.70,
		"CPU threshold (0.0-1.0) for switching backends",
	)

	flag.Parse()

	if *rawBackends == "" {
		log.Fatal("no backends specified")
	}

	parts := strings.Split(*rawBackends, ";")

	var backends []*Backend

	for _, part := range parts {
		urls := strings.Split(part, ",")

		if len(urls) != 2 {
			log.Fatalf(
				"invalid backend %q: expected CHAT_URL,HEALTH_URL",
				part,
			)
		}

		chatURL, err := url.Parse(strings.TrimSpace(urls[0]))
		if err != nil {
			log.Fatalf(
				"invalid chat URL %q: %v",
				urls[0],
				err,
			)
		}

		apiURL, err := url.Parse(strings.TrimSpace(urls[1]))
		if err != nil {
			log.Fatalf(
				"invalid API URL %q: %v",
				urls[1],
				err,
			)
		}

		backend := &Backend{
			URL:       chatURL,
			APIURL: apiURL,
		}

		backend.Alive.Store(false)

		backends = append(backends, backend)

		log.Printf(
			"Backend: %s | Health: %s",
			chatURL,
			apiURL,
		)
	}

	lb := &LoadBalancer{
		backends:      backends,
		CPUThreshold: *threshold,
	}
	log.Printf("CPU threshold: %.2f", *threshold)

	_ = lb
	go lb.healthLoop()

	server := &http.Server{
		Addr:    ":7000",
		Handler: lb,
	}

	log.Println("Load balancer listening on :7000")

	//log.Fatal(server.ListenAndServeTLS(
	//	"/home/student/chat-ssl/cert.pem",
	//	"/home/student/chat-ssl/key.pem",
	//))

	log.Fatal(server.ListenAndServe())

	log.Println("Load balancer starting...")
}
