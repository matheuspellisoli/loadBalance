package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Attempts int = iota
	Retry
)

type Backend struct {
	URL           *url.URL
	Alive         bool
	mux           sync.RWMutex
	ReverserProxy *httputil.ReverseProxy
}

type ServerPool struct {
	backends []*Backend
	current  uint64
}

func (s *ServerPool) NextIndex() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len((s.backends))))
}

func (s *ServerPool) GetNextPeer() *Backend {
	next := s.NextIndex()

	l := len((s.backends)) + next

	for i := next; i < l; i++ {
		idx := i % len(s.backends)
		if s.backends[idx].IsAlive() {
			if i != next {
				atomic.StoreUint64(&s.current, uint64(idx))
			}

			return s.backends[idx]
		}
	}

	return nil
}

func (s *ServerPool) MarkBackendStatus(backendUrl *url.URL, alive bool) {
	for _, b := range s.backends {
		if b.URL.String() == backendUrl.String() {
			b.SetAlive(alive)
			break
		}
	}
}

func (s *ServerPool) HealthCheck() {
	for _, b := range s.backends {

		status := "up"

		alive := isBackendAlive(b.URL)

		b.SetAlive(alive)

		if !alive {
			status = "down"
		}

		log.Printf("%s [%s]", b.URL, status)
	}
}

func (s *ServerPool) AddBackend(backend *Backend) {
	s.backends = append(s.backends, backend)
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	b.Alive = alive
	b.mux.Unlock()
}

func (b *Backend) IsAlive() (alive bool) {
	b.mux.RLock()
	alive = b.Alive
	b.mux.RUnlock()
	return
}

func GetRetryFromConetex(r *http.Request) int {
	retry := r.Context().Value(Retry)
	if retry != nil {
		return retry.(int)
	}

	return 0
}

func GetAttemptsFromConetex(r *http.Request) int {
	attempts := r.Context().Value(Attempts)

	if attempts != nil {
		return attempts.(int)
	}

	return 0
}

func lb(w http.ResponseWriter, r *http.Request) {

	attempts := GetAttemptsFromConetex(r)

	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reacherd", r.RemoteAddr, r.URL.Path)
		http.Error(w, "Server not Available", http.StatusServiceUnavailable)
	}

	peer := serverPool.GetNextPeer()

	if peer != nil {
		peer.ReverserProxy.ServeHTTP(w, r)
		return
	}

	http.Error(w, "Server not Available", http.StatusServiceUnavailable)
}

func isBackendAlive(u *url.URL) bool {
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", u.Host, timeout)

	if err != nil {
		log.Println("Site unreachable, error: ", err)
		return false
	}

	defer conn.Close()
	return true
}

func HealthCheck() {
	t := time.NewTicker(time.Minute * 1)

	for {
		select {
		case <-t.C:
			log.Println("Start health check....")
			serverPool.HealthCheck()
			log.Println("health check completed")
		}
	}
}

var serverPool ServerPool

func main() {
	var severList string
	var port int

	flag.StringVar(&severList, "backends", "", "Load balanced backends, use commas to separate")
	flag.IntVar(&port, "port", 3030, "Port to server")
	flag.Parse()

	if len(severList) == 0 {
		log.Fatal("Please provide one or name backends to balance")
	}

	tokens := strings.Split(severList, ",")

	for _, tok := range tokens {
		serverUrl, err := url.Parse(tok)

		if err != nil {
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(serverUrl)

		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
			retries := GetRetryFromConetex(r)

			if retries < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(r.Context(), Retry, retries+1)
					proxy.ServeHTTP(w, r.WithContext(ctx))

				}
				return
			}

			serverPool.MarkBackendStatus(serverUrl, false)

			attempts := GetAttemptsFromConetex(r)

			log.Printf("%s(%s) Attempts Retry %d\n", r.RemoteAddr, r.URL.Path, attempts)
			ctx := context.WithValue(r.Context(), Retry, retries+1)
			lb(w, r.WithContext(ctx))
		}

		serverPool.AddBackend(&Backend{
			URL:           serverUrl,
			Alive:         true,
			ReverserProxy: proxy,
		})

		log.Printf("Confitgured server %s\n", serverUrl)
	}

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(lb),
	}

	go HealthCheck()

	log.Printf("Load Balance started at :%d\n", port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
