package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"gopkg.in/yaml.v2"
)

type Service struct {
	Port     int `yaml:"port"`
	Backends []struct {
		Type string `yaml:"type"`
		Port int    `yaml:"port"`
		Host string `yaml:"host"`
	} `yaml:"backends"`
	Rule struct {
		Type string `yaml:"type"`
		Key  string `yaml:"key"`
	} `yaml:"rule"`
}

type integer int

const (
	Attempts integer = iota
	Retry
)

const (
	SourceIP   = "ip"
	HTTPHeader = "header"
	QueryParam = "query"
)

type Backend struct {
	URL          *url.URL
	Alive        bool
	Mux          sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

type Strategy struct {
	name string
	key  string
}

type ServerPool struct {
	Backends []*Backend
	Current  int
	Strategy Strategy
	HR       *consistent.Consistent
}

type hasher struct{}

type member string

func (m member) String() string {
	return string(m)
}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

func (s *ServerPool) AddBackend(b *Backend) {
	s.Backends = append(s.Backends, b)
}

func (s *ServerPool) getKey(httpReq *http.Request) (string, error) {
	var hashKey string

	switch s.Strategy.name {
	case SourceIP:
		hashKey = httpReq.Host
	case HTTPHeader:
		hashKey = httpReq.Header.Get(s.Strategy.key)
	case QueryParam:
		hashKey = httpReq.URL.Query().Get(s.Strategy.key)
	default:
		return "", fmt.Errorf("can't find ConsistentHash fields")
	}

	return hashKey, nil
}

func lb(w http.ResponseWriter, r *http.Request) {
	peer := serverpool.GetNextPeer(r)

	if peer != nil {
		w.Header().Add("origin", peer.URL.Host)
		peer.ReverseProxy.ServeHTTP(w, r)
		return
	}
	http.Error(w, "Service not available", http.StatusServiceUnavailable)
}

func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0
}

func healthCheck() {
	for {
		log.Println("Starting health check...")
		serverpool.HealthCheck()
		log.Println("Health check completed")
		time.Sleep(time.Second * 30)
	}
}

func isBackendAlive(backend *url.URL) bool {
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", backend.Host, timeout)
	if err != nil {
		log.Printf("Error while connecting baceknd")
		return false
	}
	defer conn.Close()
	return true
}

func (s *ServerPool) HealthCheck() {
	for _, b := range s.Backends {
		status := "up"
		alive := isBackendAlive(b.URL)
		b.SetAlive(alive)
		if !alive {
			status = "down"
		}
		log.Printf("%s [%s]\n", b.URL, status)

	}
}

func GetAttemptsFromContext(r *http.Request) int {
	if attemp, ok := r.Context().Value(Attempts).(int); ok {
		return attemp
	}
	return 0
}

func (b *Backend) SetAlive(alive bool) {
	b.Mux.Lock()
	b.Alive = alive
	b.Mux.Unlock()
}

func (b *Backend) GetAlive() (alive bool) {
	b.Mux.RLock()
	alive = b.Alive
	b.Mux.RUnlock()
	return
}

func (s *ServerPool) MarkBackendStatus(url *url.URL, alive bool) {
	for _, backend := range s.Backends {
		if backend.URL.String() == url.String() {
			backend.SetAlive(alive)
			break
		}
	}
}

func (s *ServerPool) GetNextPeer(r *http.Request) *Backend {

	key, err := s.getKey(r)

	if err != nil {
		return nil
	}

	owner := s.HR.LocateKey([]byte(key))

	for _, backend := range s.Backends {
		if backend.URL.String() == owner.String() {
			if backend.Alive {
				return backend
			}
		}
	}

	nearByNodes, _ := s.HR.GetClosestN([]byte(key), len(s.Backends))

	var nextOwner consistent.Member
	for _, node := range nearByNodes {
		if owner.String() != node.String() {
			nextOwner = node
		}
	}

	for _, backend := range s.Backends {
		if backend.URL.String() == nextOwner.String() {
			if backend.Alive {
				return backend
			}
		}
	}

	return nil
}

var serverpool ServerPool

func main() {
	var serverlist string
	var port int
	flag.StringVar(&serverlist, "backends", "", "List of backends")
	flag.IntVar(&port, "port", 3031, "loadbalancer port")
	flag.Parse()
	var config Service
	servers := make(map[int]string)
	if len(serverlist) == 0 {
		filename, _ := filepath.Abs("./gobalancer.yml")
		yamlFile, err := ioutil.ReadFile(filename)

		if err != nil {
			panic(err)
		}

		err = yaml.Unmarshal(yamlFile, &config)
		log.Println(config)
		if err != nil {
			log.Println(err.Error())
		}
		for index, server := range config.Backends {
			servers[index] = server.Type + "://" + server.Host + ":" + strconv.Itoa(server.Port)
		}
		port = config.Port
	} else {
		tokens := strings.Split(serverlist, ",")

		for index, token := range tokens {
			servers[index] = token
		}
	}

	// Create a new consistent instance
	cfg := consistent.Config{
		PartitionCount:    100,
		ReplicationFactor: 10,
		Load:              1.25,
		Hasher:            hasher{},
	}

	c := consistent.New(nil, cfg)

	if len(servers) == 0 {
		log.Fatal("Please provide atleast one server")
	}

	for _, tok := range servers {
		serverUrl, err := url.Parse(tok)
		node := member(serverUrl.String())
		c.Add(node)

		if err != nil {
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(serverUrl)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			log.Printf("[%s] %s\n", serverUrl.Host, e.Error())
			retries := GetRetryFromContext(request)
			if retries < 3 {
				time.Sleep(10 * time.Millisecond)
				ctx := context.WithValue(request.Context(), Retry, retries+1)
				proxy.ServeHTTP(writer, request.WithContext(ctx))
				return
			}
			// after 3 retries, mark this backend as down
			serverpool.MarkBackendStatus(serverUrl, false)

			// if the same request routing for few attempts with different backends, increase the count
			attempts := GetAttemptsFromContext(request)
			log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
			ctx := context.WithValue(request.Context(), Attempts, attempts+1)
			lb(writer, request.WithContext(ctx))
		}

		serverpool.AddBackend(&Backend{
			URL:          serverUrl,
			Alive:        true,
			ReverseProxy: proxy,
		})

		log.Printf("Configured server: %s\n", serverUrl)
	}

	serverpool.HR = c
	serverpool.Strategy.name = "query"
	serverpool.Strategy.key = "key"

	if config.Backends != nil {
		serverpool.Strategy.name = config.Rule.Type
		serverpool.Strategy.key = config.Rule.Key
	}

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(lb),
	}

	go healthCheck()

	log.Printf("Load Balancer started at :%d\n", port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
