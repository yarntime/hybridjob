package client

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func GetClientConfig(host string) (*rest.Config, error) {
	if host != "" {
		return clientcmd.BuildConfigFromFlags(host, "")
	}
	return rest.InClusterConfig()
}
