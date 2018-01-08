package client

import (
	"github.com/yarntime/hybridjob/pkg/tools"
	k8s "k8s.io/client-go/kubernetes"
)

func NewK8sClint(address string) *k8s.Clientset {

	config, err := tools.GetClientConfig(address)
	if err != nil {
		panic(err.Error())
	}

	// creates the clientSet
	clientSet, err := k8s.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	return clientSet
}
