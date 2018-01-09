package controller

import (
	"github.com/yarntime/hybridjob/pkg/client"
	k8s "k8s.io/client-go/kubernetes"
	"time"
)

type Config struct {
	Address               string
	ConcurrentJobHandlers int
	StopCh                chan struct{}
	ResyncPeriod          time.Duration
	ServeAddress          string
	ServePort             int
	K8sClient             *k8s.Clientset
	HybridJobClient       *client.HybridJobClient
}
