package types

import (
	"reflect"

	apiextv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
	"time"
)

const (
	HybridJobs  string = "hybridjobs"
	Group       string = "rivernet.io"
	Version     string = "v1"
	FullCRDName string = HybridJobs + "." + Group
)

// Create the CRD resource, ignore error if it already exists
func CreateHybridJob(clientset apiextcs.Interface) error {
	crd := &apiextv1beta1.CustomResourceDefinition{
		ObjectMeta: meta_v1.ObjectMeta{Name: FullCRDName},
		Spec: apiextv1beta1.CustomResourceDefinitionSpec{
			Group:   Group,
			Version: Version,
			Scope:   apiextv1beta1.NamespaceScoped,
			Names: apiextv1beta1.CustomResourceDefinitionNames{
				Plural:     HybridJobs,
				Singular:   "hybridjob",
				ShortNames: []string{"hj"},
				Kind:       reflect.TypeOf(HybridJob{}).Name(),
			},
		},
	}

	_, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil && apierrors.IsAlreadyExists(err) {
		return nil
	}

	for {
		_, err = clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(FullCRDName, meta_v1.GetOptions{})
		if err == nil {
			return nil
		}
		time.Sleep(2000)
	}

	return err

	// Note the original apiextensions example adds logic to wait for creation and exception handling
}

// Definition of our CRD Example class
type HybridJob struct {
	meta_v1.TypeMeta   `json:",inline"`
	meta_v1.ObjectMeta `json:"metadata"`
	Spec               HybridJobSpec   `json:"spec"`
	Status             HybridJobStatus `json:"status,omitempty"`
}
type HybridJobSpec struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

type HybridJobStatus struct {
	State      string `json:"state,omitempty"`
	PSHost     string `json:"pshost,omitempty"`
	WorkerHost string `json:"workerhost, omitempty"`
}

type HybridJobList struct {
	meta_v1.TypeMeta `json:",inline"`
	meta_v1.ListMeta `json:"metadata"`
	Items            []HybridJob `json:"items"`
}

// Create a  Rest client with the new CRD Schema
var SchemeGroupVersion = schema.GroupVersion{Group: Group, Version: Version}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&HybridJob{},
		&HybridJobList{},
	)
	meta_v1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

func NewClient(cfg *rest.Config) (*rest.RESTClient, *runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	SchemeBuilder := runtime.NewSchemeBuilder(addKnownTypes)
	if err := SchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}
	config := *cfg
	config.GroupVersion = &SchemeGroupVersion
	config.APIPath = "/apis"
	config.ContentType = runtime.ContentTypeJSON
	config.NegotiatedSerializer = serializer.DirectCodecFactory{
		CodecFactory: serializer.NewCodecFactory(scheme)}

	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, nil, err
	}
	return client, scheme, nil
}
