package tools

import (
	"encoding/json"
	"github.com/golang/glog"
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

func GetPodsAnnotationSet(key string, roleCount int32, replicaSpec *types.ReplicaSpec) (labels.Set, error) {
	desiredAnnotations := make(labels.Set)
	for k, v := range replicaSpec.Template.Annotations {
		desiredAnnotations[k] = v
	}

	group := types.SchedulingGroup{
		Group:       key,
		Role:        string(replicaSpec.ReplicaType),
		RoleCount:   roleCount,
		MinReplicas: *replicaSpec.MinReplicas,
		MaxReplicas: *replicaSpec.MaxReplicas,
	}

	if replicaSpec.Priority != nil {
		group.Priority = *replicaSpec.Priority
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

func FilterActivePods(pods []v1.Pod) []v1.Pod {
	var result []v1.Pod
	for _, p := range pods {
		if IsPodActive(p) {
			result = append(result, p)
		} else {
			glog.V(4).Infof("Ignoring inactive pod %v/%v in state %v, deletion time %v",
				p.Namespace, p.Name, p.Status.Phase, p.DeletionTimestamp)
		}
	}
	return result
}

func FilterAssignedPods(pods []v1.Pod) ([]v1.Pod, []v1.Pod) {
	assignedPods := []v1.Pod{}
	unassignedPods := []v1.Pod{}
	for _, pod := range pods {
		if pod.Spec.NodeName != "" {
			assignedPods = append(assignedPods, pod)
		} else {
			unassignedPods = append(unassignedPods, pod)
		}
	}
	return assignedPods, unassignedPods
}

func IsPodActive(p v1.Pod) bool {
	return v1.PodRunning == p.Status.Phase &&
		p.DeletionTimestamp == nil
}

func FilterPendingPods(pods []v1.Pod) []v1.Pod {
	var result []v1.Pod
	for _, p := range pods {
		if IsPodPending(p) {
			result = append(result, p)
		}
	}
	return result
}

func IsPodPending(p v1.Pod) bool {
	return v1.PodPending == p.Status.Phase &&
		p.DeletionTimestamp == nil
}

func IsJobFinished(hj *types.HybridJob) bool {
	if hj.Status.Phase == types.Finished || hj.Status.Phase == types.Failed {
		return true
	}
	return false
}

func IsJobScheduled(hj *types.HybridJob) bool {
	if hj.Status.Phase == types.Scheduled {
		return true
	}
	return false
}

func SetHybridJobPhase(hj *types.HybridJob, phase types.JobPhase) {
	hj.Status.Phase = phase
	hj.Status.IsChanged = true
}

func IsOwnerOfThePod(hj *types.HybridJob, pod v1.Pod) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.UID == hj.UID {
			return true
		}
	}
	return false
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
