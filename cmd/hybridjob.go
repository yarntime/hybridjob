package main

import (
	"flag"
	"github.com/golang/glog"
	c "github.com/yarntime/hybridjob/pkg/controller"
	"time"
)

var (
	address               string
	concurrentJobHandlers int
	resyncPeriod          time.Duration
)

func init() {
	flag.StringVar(&address, "address", "192.168.254.45:8080", "APIServer addr")
	flag.IntVar(&concurrentJobHandlers, "concurrentJobHandlers", 4, "Concurrent job handlers")
	flag.DurationVar(&resyncPeriod, "resync period", time.Second*15, "resync period")
	flag.Parse()
}

func main() {
	glog.Info("Controller started.")
	stop := make(chan struct{})
	config := &c.Config{
		Address:               address,
		ConcurrentJobHandlers: concurrentJobHandlers,
		ResyncPeriod:          resyncPeriod,
		StopCh:                stop,
	}
	controller := c.NewController(config)
	go controller.Run(stop)
	glog.Info("Controller started.")
	// Wait forever
	select {}
}
