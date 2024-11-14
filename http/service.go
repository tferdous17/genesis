package http

import (
	"bitcask-go/store"
	"bitcask-go/utils"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
)

type Service struct {
	addr string
	ln   net.Listener

	store store.Store
}

// NewService returns an unitialized HTTP service
func NewService(addr string, store store.Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
	}
}

func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.ln = ln
	http.Handle("/", s)

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
	} else {
		fmt.Println("does not have /key prefix")
		w.WriteHeader(http.StatusNotFound)
	}
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
			if err := s.store.Put(&k, &v); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

	case "GET":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
		}
		val, err := s.store.Get(k)
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
		err := s.store.Delete(k)
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
