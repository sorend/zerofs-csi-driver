package driver

import (
	"net"
	"os"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

type NonBlockingGRPCServer struct {
	wg      sync.WaitGroup
	server  *grpc.Server
	network string
	address string
}

func NewNonBlockingGRPCServer(network, address string) *NonBlockingGRPCServer {
	return &NonBlockingGRPCServer{
		network: network,
		address: address,
	}
}

func (s *NonBlockingGRPCServer) Start(ids *IdentityServer, cs *ControllerServer, ns *NodeServer) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.serve(ids, cs, ns); err != nil {
			klog.Fatalf("Failed to serve: %v", err)
		}
	}()
}

func (s *NonBlockingGRPCServer) Wait() {
	s.wg.Wait()
}

func (s *NonBlockingGRPCServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
		klog.Info("gRPC server stopped gracefully")
	}
}

func (s *NonBlockingGRPCServer) ForceStop() {
	if s.server != nil {
		s.server.Stop()
		klog.Info("gRPC server stopped forcefully")
	}
}

func (s *NonBlockingGRPCServer) serve(ids *IdentityServer, cs *ControllerServer, ns *NodeServer) error {
	if s.network == "unix" {
		if err := os.Remove(s.address); err != nil && !os.IsNotExist(err) {
			klog.Warningf("Failed to remove socket file %s: %v", s.address, err)
		}
	}

	listener, err := net.Listen(s.network, s.address)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	csi.RegisterIdentityServer(s.server, ids)
	csi.RegisterControllerServer(s.server, cs)
	csi.RegisterNodeServer(s.server, ns)

	klog.Infof("Listening for connections on %s://%s", s.network, s.address)
	return s.server.Serve(listener)
}
