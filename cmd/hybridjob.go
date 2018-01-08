package cmd

import (
	"flag"
	c "github.com/yarntime/hybridjob/pkg/controller"
	"time"
)

var (
	address               string
	concurrentJobHandlers int
	resyncPeriod          time.Duration
)

func init() {
	flag.StringVar(&address, "addr", "127.0.0.1:8080", "APIServer addr")
	flag.IntVar(&concurrentJobHandlers, "concurrentJobHandlers", 4, "Concurrent job handlers")
	flag.DurationVar(&resyncPeriod, "resync period", time.Second*15, "resunc period")
}

func main() {
	stop := make(chan struct{})
	config := &c.Config{
		Address:               address,
		ConcurrentJobHandlers: concurrentJobHandlers,
		ResyncPeriod:          resyncPeriod,
		StopCh:                stop,
	}
	controller := c.NewController(config)
	go controller.Run(stop)

	// Wait forever
	select {}
}
