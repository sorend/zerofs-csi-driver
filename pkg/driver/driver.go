package driver

import (
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/zerofs/zerofs-csi-driver/pkg/zerofs"
)

const (
	DriverName    = "zerofs.csi.k8s.io"
	DriverVersion = "1.0.0"
)

type Driver struct {
	options *DriverOptions

	ids    *IdentityServer
	Cs     *ControllerServer
	ns     *NodeServer
	server *NonBlockingGRPCServer

	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
}

type DriverOptions struct {
	NodeID      string
	DriverName  string
	Endpoint    string
	Namespace   string
	Kubeconfig  string
	WorkDir     string
	ZerofsImage string
}

func NewDriver(options *DriverOptions) *Driver {
	if options.DriverName == "" {
		options.DriverName = DriverName
	}

	d := &Driver{
		options: options,
	}

	d.ids = NewIdentityServer(d)
	d.Cs = NewControllerServer(d)
	d.ns = NewNodeServer(d)

	return d
}

func (d *Driver) Run() error {
	scheme := "unix"
	addr := d.options.Endpoint
	if strings.HasPrefix(addr, "unix://") {
		scheme = "unix"
		addr = strings.TrimPrefix(addr, "unix://")
	}

	d.server = NewNonBlockingGRPCServer(scheme, addr)
	d.server.Start(d.ids, d.Cs, d.ns)
	d.server.Wait()

	return nil
}

func (d *Driver) Stop() {
	if d.server != nil {
		d.server.Stop()
	}
}

func (d *Driver) GetManager() *zerofs.Manager {
	if d.Cs != nil {
		return d.Cs.manager
	}
	return nil
}
