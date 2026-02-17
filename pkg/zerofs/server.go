package zerofs

import (
	"context"
	"net/http"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type Server struct {
	k8sClient *kubernetes.Clientset
	namespace string
	server    *http.Server
}

func NewServer(k8sClient *kubernetes.Clientset, namespace string) *Server {
	return &Server{
		k8sClient: k8sClient,
		namespace: namespace,
	}
}

func (s *Server) Run(ctx context.Context) error {
	klog.Info("Starting ZeroFS server health endpoint on :8080")

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthzHandler)

	s.server = &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		if s.server != nil {
			s.server.Shutdown(context.Background())
		}
	}()

	return s.server.ListenAndServe()
}

func (s *Server) healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
