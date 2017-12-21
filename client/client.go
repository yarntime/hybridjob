/*
Copyright 2016 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package client

import (
	v1 "github.com/yarntime/hybridjob/types"

	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// This file implement all the (CRUD) client methods we need to access our CRD object

func NewHybridJobClient(cl *rest.RESTClient, scheme *runtime.Scheme, namespace string) *HybridJobClient {
	return &HybridJobClient{cl: cl, ns: namespace, plural: v1.HybridJobs,
		codec: runtime.NewParameterCodec(scheme)}
}

type HybridJobClient struct {
	cl     *rest.RESTClient
	ns     string
	plural string
	codec  runtime.ParameterCodec
}

func (f *HybridJobClient) Create(obj *v1.HybridJob) (*v1.HybridJob, error) {
	var result v1.HybridJob
	err := f.cl.Post().
		Namespace(f.ns).Resource(f.plural).
		Body(obj).Do().Into(&result)
	return &result, err
}

func (f *HybridJobClient) Update(obj *v1.HybridJob) (*v1.HybridJob, error) {
	var result v1.HybridJob
	err := f.cl.Put().
		Namespace(f.ns).Resource(f.plural).
		Body(obj).Do().Into(&result)
	return &result, err
}

func (f *HybridJobClient) Delete(name string, options *meta_v1.DeleteOptions) error {
	return f.cl.Delete().
		Namespace(f.ns).Resource(f.plural).
		Name(name).Body(options).Do().
		Error()
}

func (f *HybridJobClient) Get(name string) (*v1.HybridJob, error) {
	var result v1.HybridJob
	err := f.cl.Get().
		Namespace(f.ns).Resource(f.plural).
		Name(name).Do().Into(&result)
	return &result, err
}

func (f *HybridJobClient) List(opts meta_v1.ListOptions) (*v1.HybridJobList, error) {
	var result v1.HybridJobList
	err := f.cl.Get().
		Namespace(f.ns).Resource(f.plural).
		VersionedParams(&opts, f.codec).
		Do().Into(&result)
	return &result, err
}

// Create a new List watch for our TPR
func (f *HybridJobClient) NewListWatch() *cache.ListWatch {
	return cache.NewListWatchFromClient(f.cl, f.plural, f.ns, fields.Everything())
}
