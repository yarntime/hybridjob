package tools

import (
	"encoding/json"
	"github.com/yarntime/hybridjob/pkg/types"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	"time"
)

const (
	SchedulingGroup = "ecp-scheduling-group"
)

func GetClientConfig(host string) (*rest.Config, error) {
	if host != "" {
		return clientcmd.BuildConfigFromFlags(host, "")
	}
	return rest.InClusterConfig()
}

type FixedItemIntervalRateLimiter struct {
	interval time.Duration
}

func NewFixedItemIntervalRateLimiter(interval time.Duration) workqueue.RateLimiter {
	return &FixedItemIntervalRateLimiter{
		interval: interval,
	}
}

func (r *FixedItemIntervalRateLimiter) When(item interface{}) time.Duration {
	return r.interval
}

func (r *FixedItemIntervalRateLimiter) NumRequeues(item interface{}) int {
	return 1
}

func (r *FixedItemIntervalRateLimiter) Forget(item interface{}) {
}

func GetPodsLabelSet(template *v1.PodTemplateSpec) labels.Set {
	desiredLabels := make(labels.Set)
	for k, v := range template.Labels {
		desiredLabels[k] = v
	}
	return desiredLabels
}

func GetPodsFinalizers(template *v1.PodTemplateSpec) []string {
	desiredFinalizers := make([]string, len(template.Finalizers))
	copy(desiredFinalizers, template.Finalizers)
	return desiredFinalizers
}

func GetPodsAnnotationSet(key string, tfReplicaSpec *types.TfReplicaSpec) (labels.Set, error) {
	desiredAnnotations := make(labels.Set)
	for k, v := range tfReplicaSpec.Template.Annotations {
		desiredAnnotations[k] = v
	}

	group := types.SchedulingGroup{
		Group:       key,
		Role:        string(tfReplicaSpec.TfReplicaType),
		MinReplicas: *tfReplicaSpec.MinReplicas,
		MaxReplicas: *tfReplicaSpec.MaxReplicas,
	}

	if tfReplicaSpec.Priority != nil {
		group.Priority = *tfReplicaSpec.Priority
	}
	data, _ := json.Marshal(&group)
	desiredAnnotations[SchedulingGroup] = string(data)
	return desiredAnnotations, nil
}

func GenerateHosts(pods []v1.Pod) string {
	hosts := ""
	for _, pod := range pods {
		hosts = hosts + pod.Status.PodIP + ","
	}
	rs := []rune(hosts)
	return string(rs[0 : len(rs)-1])
}

func GetKeyOfHybridJob(hybridJob *types.HybridJob) string {
	return hybridJob.Namespace + "/" + hybridJob.Name
}

func NewInt32(val int32) *int32 {
	p := new(int32)
	*p = val
	return p
}

func NewInt64(val int64) *int64 {
	p := new(int64)
	*p = val
	return p
}
