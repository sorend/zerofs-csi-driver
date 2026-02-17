package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/zerofs/zerofs-csi-driver/pkg/driver"
	"github.com/zerofs/zerofs-csi-driver/pkg/zerofs"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

var (
	driverName  string
	nodeID      string
	endpoint    string
	namespace   string
	kubeconfig  string
	workDir     string
	zerofsImage string
)

func init() {
	rootCmd.AddCommand(controllerCmd)
	rootCmd.AddCommand(nodeCmd)
	rootCmd.AddCommand(serverCmd)

	controllerCmd.Flags().StringVar(&driverName, "driver-name", driver.DriverName, "name of the CSI driver")
	controllerCmd.Flags().StringVar(&endpoint, "endpoint", "unix:///csi/csi.sock", "CSI endpoint")
	controllerCmd.Flags().StringVar(&namespace, "namespace", "default", "namespace to run in")
	controllerCmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig file")
	controllerCmd.Flags().StringVar(&workDir, "work-dir", "/var/lib/zerofs-csi", "working directory")
	controllerCmd.Flags().StringVar(&zerofsImage, "zerofs-image", "zerofs/zerofs:latest", "ZeroFS server container image")

	nodeCmd.Flags().StringVar(&driverName, "driver-name", driver.DriverName, "name of the CSI driver")
	nodeCmd.Flags().StringVar(&nodeID, "node-id", "", "node ID")
	nodeCmd.Flags().StringVar(&endpoint, "endpoint", "unix:///csi/csi.sock", "CSI endpoint")

	serverCmd.Flags().StringVar(&namespace, "namespace", "default", "namespace")
	serverCmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig file")
}

var rootCmd = &cobra.Command{
	Use:   "zerofs-csi-driver",
	Short: "ZeroFS CSI Driver for Kubernetes",
}

var controllerCmd = &cobra.Command{
	Use:   "controller",
	Short: "Run the CSI controller service",
	RunE: func(cmd *cobra.Command, args []string) error {
		klog.Infof("Starting ZeroFS CSI Controller (driver: %s)", driverName)

		config, err := getKubeConfig()
		if err != nil {
			return fmt.Errorf("failed to get kubernetes config: %w", err)
		}

		k8sClient, err := kubernetes.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("failed to create kubernetes client: %w", err)
		}

		drv := driver.NewDriver(&driver.DriverOptions{
			DriverName:  driverName,
			Endpoint:    endpoint,
			Namespace:   namespace,
			Kubeconfig:  kubeconfig,
			WorkDir:     workDir,
			ZerofsImage: zerofsImage,
		})

		manager := drv.GetManager()
		if manager != nil {
			manager.SetClient(k8sClient)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			<-sigCh
			klog.Info("Received termination signal, shutting down...")
			cancel()
		}()

		go func() {
			<-ctx.Done()
			drv.Stop()
		}()

		return drv.Run()
	},
}

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Run the CSI node service",
	RunE: func(cmd *cobra.Command, args []string) error {
		klog.Infof("Starting ZeroFS CSI Node Service (driver: %s, node: %s)", driverName, nodeID)

		drv := driver.NewDriver(&driver.DriverOptions{
			DriverName: driverName,
			NodeID:     nodeID,
			Endpoint:   endpoint,
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			<-sigCh
			klog.Info("Received termination signal, shutting down...")
			cancel()
		}()

		go func() {
			<-ctx.Done()
			drv.Stop()
		}()

		return drv.Run()
	},
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the ZeroFS server",
	RunE: func(cmd *cobra.Command, args []string) error {
		klog.Info("Starting ZeroFS server")

		config, err := getKubeConfig()
		if err != nil {
			return fmt.Errorf("failed to get kubernetes config: %w", err)
		}

		k8sClient, err := kubernetes.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("failed to create kubernetes client: %w", err)
		}

		srv := zerofs.NewServer(k8sClient, namespace)
		return srv.Run(context.Background())
	},
}

func getKubeConfig() (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
