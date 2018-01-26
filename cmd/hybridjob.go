package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/yarntime/hybridjob/pkg/client"
	c "github.com/yarntime/hybridjob/pkg/controller"
	"time"
)

var (
	apiserverAddress      string
	concurrentJobHandlers int
	resyncPeriod          time.Duration
	serveAddress          string
	servePort             int
)

func init() {
	flag.StringVar(&apiserverAddress, "apiserver_address", "", "Kubernetes apiserver address")
	flag.IntVar(&concurrentJobHandlers, "concurrent_job_handlers", 4, "Concurrent job handlers")
	flag.DurationVar(&resyncPeriod, "resync_period", time.Minute*30, "Resync period")
	flag.StringVar(&serveAddress, "serve_address", "0.0.0.0", "Serve address")
	flag.IntVar(&servePort, "serve_port", 8080, "Serve port")
	flag.Parse()
}

func main() {
	stop := make(chan struct{})
	config := &c.Config{
		Address:               apiserverAddress,
		ConcurrentJobHandlers: concurrentJobHandlers,
		ResyncPeriod:          resyncPeriod,
		StopCh:                stop,
		ServeAddress:          serveAddress,
		ServePort:             servePort,
		K8sClient:             client.NewK8sClint(apiserverAddress),
		HybridJobClient:       client.NewHybridJobClient(apiserverAddress),
	}
	hybridJobController := c.NewController(config)
	go hybridJobController.Run(stop)
	glog.Info("HybridJob controller started.")

	restServer := c.NewRestServer(config)
	go restServer.Run(stop)
	glog.Info("Rest server started.")
	// Wait forever
	select {}
}
