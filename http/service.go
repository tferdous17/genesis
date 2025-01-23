package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/tferdous17/genesis/utils"
)

type Store interface {
	Put(key *string, value *string) error
	Get(key string) (string, error)
	Delete(key string) error
	Close() bool
}

// not sure if this is the best way to go about this but it works
type Cluster interface {
	Open()
	Put(key string, value string) error
	Get(key string) (string, error)
	Delete(key string) error
	AddNode()
	RemoveNode(addr string)
	Close()
}

type Service struct {
	addr    string
	ln      net.Listener
	mux     *http.ServeMux
	cluster Cluster
}

// NewClusterService returns an unitialized HTTP service
func NewClusterService(addr string, cluster Cluster) *Service {
	return &Service{
		addr:    addr,
		cluster: cluster,
	}
}

func (s *Service) Start() error {
	s.mux = http.NewServeMux()
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.ln = ln
	s.mux.Handle("/", s)

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			utils.LogRED("serve err: %s", err)
		}
	}()

	return nil
}

func (s *Service) Close() error {
	s.ln.Close()
	return nil
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/key") {
		s.handleKeyRequest(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/add-node") && r.Method == "POST" {
		s.cluster.AddNode()
		return
	}

	if strings.HasPrefix(r.URL.Path, "/remove-node") {
		s.handleNodeRemoval(w, r)
		return
	}

	fmt.Println("prefix must be one of the following: /key, /add-node, /remove-node")
	w.WriteHeader(http.StatusNotFound)
}

func (s *Service) handleNodeRemoval(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		io.WriteString(w, "err: must be POST method")
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 3 {
		io.WriteString(w, "err: missing token")
		return
	}

	s.cluster.RemoveNode(parts[2])
}

func (s *Service) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	getKey := func() string {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 3 {
			return ""
		}
		return parts[2]
	}

	switch r.Method {
	case "POST":
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
		}
		m := map[string]string{}
		if err := json.Unmarshal(b, &m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
		}

		for k, v := range m {
			if err := s.cluster.Put(k, v); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

	case "GET":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
		}
		val, err := s.cluster.Get(k)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		io.WriteString(w, val)

	case "DELETE":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
		}
		err := s.cluster.Delete(k)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}

func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}
