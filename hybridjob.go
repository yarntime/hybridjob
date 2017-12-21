package main

import (
	"fmt"
	"time"

	"github.com/yarntime/hybridjob/client"
	"github.com/yarntime/hybridjob/types"

	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

func GetClientConfig(host string) (*rest.Config, error) {
	if host != "" {
		return clientcmd.BuildConfigFromFlags(host, "")
	}
	return rest.InClusterConfig()
}

func main() {

	config, err := GetClientConfig("192.168.254.67:8080")
	if err != nil {
		panic(err.Error())
	}

	clientset, err := apiextcs.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	err = types.CreateHybridJob(clientset)
	if err != nil {
		panic(err)
	}

	crdcs, scheme, err := types.NewClient(config)
	if err != nil {
		panic(err)
	}

	hybridJobClient := client.NewHybridJobClient(crdcs, scheme, "default")

	// Create a new Example object and write to k8s
	job := &types.HybridJob{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:   "job",
			Labels: map[string]string{"mylabel": "test"},
		},
		Spec: types.HybridJobSpec{
			Min: 1,
			Max: 2,
		},
		Status: types.HybridJobStatus{
			State: "created",
		},
	}

	result, err := hybridJobClient.Create(job)
	if err != nil {
		fmt.Printf("Failed to create job: %v\n", err)
		return
	}

	fmt.Printf("Job created: %v\n", result)

	_, controller := cache.NewInformer(
		hybridJobClient.NewListWatch(),
		&types.HybridJob{},
		time.Minute*10,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				fmt.Printf("add: %s \n", obj)
			},
			DeleteFunc: func(obj interface{}) {
				fmt.Printf("delete: %s \n", obj)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				fmt.Printf("Update old: %s \n      New: %s\n", oldObj, newObj)
			},
		},
	)

	stop := make(chan struct{})
	go controller.Run(stop)

	// Wait forever
	select {}
}
