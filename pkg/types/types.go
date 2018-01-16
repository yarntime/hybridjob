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
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/rest"
	"time"
)

const (
	HybridJobs  string = "hybridjobs"
	Group       string = "rivernet.io"
	Version     string = "v1"
	FullCRDName string = HybridJobs + "." + Group
)

type TfReplicaType string

const (
	PS     TfReplicaType = "ps"
	WORKER TfReplicaType = "worker"
)

type JobPhase string

const (
	Creating  JobPhase = "Creating"
	Scheduled JobPhase = "Scheduled"
	Running   JobPhase = "Running"
	Ready     JobPhase = "Ready"
	UnReady   JobPhase = "UnReady"
	Finished  JobPhase = "Finished"
	Failed    JobPhase = "Failed"
)

type HybridJob struct {
	meta_v1.TypeMeta   `json:",inline"`
	meta_v1.ObjectMeta `json:"metadata"`
	Spec               HybridJobSpec   `json:"spec"`
	Status             HybridJobStatus `json:"status"`
}

type HybridJobSpec struct {
	Selector     *meta_v1.LabelSelector `json:"selector"`
	ReplicaSpecs []*TfReplicaSpec       `json:"replicaSpecs"`
}

type TfReplicaSpec struct {
	NodeName      string                 `json:"nodeName,omitempty"`
	MinReplicas   *int32                 `json:"min,omitempty"`
	MaxReplicas   *int32                 `json:"max,omitempty"`
	Priority      *int32                 `json:"priority,omitempty"`
	Selector      *meta_v1.LabelSelector `json:"selector"`
	Template      *v1.PodTemplateSpec    `json:"template,omitempty"`
	TfReplicaType `json:"tfReplicaType"`
}

type HybridJobStatus struct {
	Phase           JobPhase                           `json:"phase,omitempty"`
	StartTime       *meta_v1.Time                      `json:"startTime,omitempty"`
	Hosts           map[TfReplicaType]string           `json:"hosts,omitempty"`
	TfReplicaStatus map[TfReplicaType]*TfReplicaStatus `json:"tfReplicaStatus"`
	IsChanged       bool
}

type TfReplicaStatus struct {
	Phase     JobPhase `json:"phase,omitempty"`
	Active    int32    `json:"active,omitempty"`
	Succeeded int32    `json:"succeeded,omitempty"`
	Failed    int32    `json:"failed,omitempty"`
	Desired   int32    `json:"desired,omitempty"`
}

type HybridJobList struct {
	meta_v1.TypeMeta `json:",inline"`
	meta_v1.ListMeta `json:"metadata"`
	Items            []HybridJob `json:"items"`
}

type SchedulingGroup struct {
	Group       string `json:"group"`
	Role        string `json:"role"`
	RoleCount   int32  `json:"roleCount"`
	MinReplicas int32  `json:"minReplica"`
	MaxReplicas int32  `json:"maxReplica"`
	Priority    int32  `json:"priority"`
}

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
		time.Sleep(1000)
	}

	return err
}

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
