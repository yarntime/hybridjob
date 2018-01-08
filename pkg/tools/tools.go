package tools

import (
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/util/workqueue"
	"time"
)

const (
	SchedulingGroup string = "schedulinggroup"
)

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

func GetPodsAnnotationSet(template *v1.PodTemplateSpec) (labels.Set, error) {
	desiredAnnotations := make(labels.Set)
	for k, v := range template.Annotations {
		desiredAnnotations[k] = v
	}

	// TODO add scheduling group info to pod's annotation
	desiredAnnotations[SchedulingGroup] = "test"
	return desiredAnnotations, nil
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
