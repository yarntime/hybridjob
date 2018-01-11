package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/yarntime/hybridjob/pkg/client"
	c "github.com/yarntime/hybridjob/pkg/controller"
	"time"
)

var (
	apiserver_address     string
	concurrentJobHandlers int
	resyncPeriod          time.Duration
	serve_address         string
	serve_port            int
)

func init() {
	flag.StringVar(&apiserver_address, "apiServerAddress", "192.168.254.44:8080", "kubernetes apiserver address")
	flag.IntVar(&concurrentJobHandlers, "concurrentJobHandlers", 4, "Concurrent job handlers")
	flag.DurationVar(&resyncPeriod, "resync period", time.Minute*30, "resync period")
	flag.StringVar(&serve_address, "serve address", "0.0.0.0", "serve address")
	flag.IntVar(&serve_port, "serve port", 8080, "serve port")
	// TODO remove alsologtostderr and v later
	flag.Set("alsologtostderr", "true")
	flag.Set("v", "4")
	flag.Parse()
}

func main() {
	stop := make(chan struct{})
	config := &c.Config{
		Address:               apiserver_address,
		ConcurrentJobHandlers: concurrentJobHandlers,
		ResyncPeriod:          resyncPeriod,
		StopCh:                stop,
		ServeAddress:          serve_address,
		ServePort:             serve_port,
		K8sClient:             client.NewK8sClint(apiserver_address),
		HybridJobClient:       client.NewHybridJobClient(apiserver_address),
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
