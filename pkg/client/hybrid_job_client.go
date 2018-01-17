package client

import (
	v1 "github.com/yarntime/hybridjob/pkg/types"

	"github.com/golang/glog"
	"github.com/yarntime/hybridjob/pkg/tools"
	"github.com/yarntime/hybridjob/pkg/types"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

func NewHybridJobClient(address string) *HybridJobClient {

	config, err := tools.GetClientConfig(address)
	if err != nil {
		panic(err.Error())
	}

	c, _ := newClientset(config)

	err = types.CreateHybridJob(c)
	if err != nil {
		glog.Fatal("Failed to create hybirdjob crd: %v\n", err)
	}

	crdcs, scheme, err := types.NewClient(config)
	if err != nil {
		panic(err)
	}

	return &HybridJobClient{cl: crdcs, plural: v1.HybridJobs, scheme: scheme,
		codec: runtime.NewParameterCodec(scheme)}
}

func newClientset(config *rest.Config) (*apiextcs.Clientset, error) {
	c, err := apiextcs.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	return c, nil
}

type HybridJobClient struct {
	cl     *rest.RESTClient
	scheme *runtime.Scheme
	plural string
	codec  runtime.ParameterCodec
}

func (f *HybridJobClient) RESTClient() *rest.RESTClient {
	return f.cl
}

func (f *HybridJobClient) Scheme() *runtime.Scheme {
	return f.scheme
}

func (f *HybridJobClient) Create(obj *v1.HybridJob) (*v1.HybridJob, error) {
	var result v1.HybridJob
	err := f.cl.Post().
		Namespace(obj.Namespace).Resource(f.plural).
		Body(obj).Do().Into(&result)
	return &result, err
}

func (f *HybridJobClient) Update(obj *v1.HybridJob) (*v1.HybridJob, error) {
	var result v1.HybridJob
	err := f.cl.Put().
		Name(obj.Name).
		Namespace(obj.Namespace).Resource(f.plural).
		Body(obj).Do().Into(&result)
	return &result, err
}

func (f *HybridJobClient) Delete(name string, namespace string, options *meta_v1.DeleteOptions) error {
	return f.cl.Delete().
		Namespace(namespace).Resource(f.plural).
		Name(name).Body(options).Do().
		Error()
}

func (f *HybridJobClient) Get(name string, namespace string) (*v1.HybridJob, error) {
	var result v1.HybridJob
	err := f.cl.Get().
		Namespace(namespace).Resource(f.plural).
		Name(name).Do().Into(&result)
	return &result, err
}

func (f *HybridJobClient) Watch(opts meta_v1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return f.cl.Get().
		Namespace("").Resource(f.plural).
		VersionedParams(&opts, f.codec).
		Watch()
}

func (f *HybridJobClient) List(namespace string, opts meta_v1.ListOptions) (*v1.HybridJobList, error) {
	var result v1.HybridJobList
	err := f.cl.Get().
		Namespace(namespace).Resource(f.plural).
		VersionedParams(&opts, f.codec).
		Do().Into(&result)
	return &result, err
}

func (f *HybridJobClient) NewListWatch(namespace string) *cache.ListWatch {
	return cache.NewListWatchFromClient(f.cl, f.plural, namespace, fields.Everything())
}
