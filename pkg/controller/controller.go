package controller

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/yarntime/hybridjob/pkg/client"
	"github.com/yarntime/hybridjob/pkg/tools"
	"github.com/yarntime/hybridjob/pkg/types"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	k8s "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/v1"
	clientv1 "k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"strconv"
	"strings"
	"time"
)

const (
	Role = "ecp-role"
)

type Config struct {
	Address               string
	ConcurrentJobHandlers int
	StopCh                chan struct{}
	ResyncPeriod          time.Duration
}

type HybridJobController struct {
	k8sClient *k8s.Clientset

	hybridJobClient *client.HybridJobClient

	recorder record.EventRecorder

	concurrentJobHandlers int

	resyncPeriod time.Duration

	// Jobs that need to be updated
	queue workqueue.RateLimitingInterface
}

func IsJobFinished(hj *types.HybridJob) bool {
	if hj.Status.Phase == types.Finished || hj.Status.Phase == types.Failed {
		return true
	}
	return false
}

func NewController(config *Config) *HybridJobController {

	k8cClient := client.NewK8sClint(config.Address)
	hybridJobClient := client.NewHybridJobClient(config.Address)

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(k8cClient.CoreV1().RESTClient()).Events("")})

	hjc := &HybridJobController{
		k8sClient:             k8cClient,
		hybridJobClient:       hybridJobClient,
		recorder:              eventBroadcaster.NewRecorder(runtime.NewScheme(), clientv1.EventSource{Component: "hybridjob-controller"}),
		concurrentJobHandlers: config.ConcurrentJobHandlers,
		queue:        workqueue.NewNamedRateLimitingQueue(tools.NewFixedItemIntervalRateLimiter(config.ResyncPeriod), "hybridjob"),
		resyncPeriod: config.ResyncPeriod,
	}

	_, c := cache.NewInformer(
		hybridJobClient.NewListWatch(""),
		&types.HybridJob{},
		time.Minute*10,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    hjc.enqueueController,
			DeleteFunc: hjc.enqueueController,
			UpdateFunc: func(old, cur interface{}) {
				if job := cur.(*types.HybridJob); !IsJobFinished(job) {
					hjc.enqueueController(job)
				}
			},
		},
	)

	go c.Run(config.StopCh)

	return hjc
}

func (hjc *HybridJobController) enqueueController(obj interface{}) {
	job := obj.(*types.HybridJob)
	key := job.Namespace + "/" + job.Name
	hjc.queue.Add(key)
}

func (hjc *HybridJobController) Run(stopCh chan struct{}) {
	for i := 0; i < hjc.concurrentJobHandlers; i++ {
		go wait.Until(hjc.startHandler, time.Second, stopCh)
	}

	<-stopCh
}

func (hjc HybridJobController) startHandler() {
	for hjc.processNextWorkItem() {
	}
}

func (hjc HybridJobController) processNextWorkItem() bool {
	key, quit := hjc.queue.Get()
	if quit {
		return false
	}
	defer hjc.queue.Done(key)

	err := hjc.processHybridJob(key.(string))
	if err == nil {
		hjc.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("Error syncing hybrid job: %v", err))
	hjc.queue.AddRateLimited(key)

	return true
}

func (hjc HybridJobController) processHybridJob(key string) error {

	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing job %q (%v)", key, time.Now().Sub(startTime))
	}()

	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if len(ns) == 0 || len(name) == 0 {
		return fmt.Errorf("invalid job key %q: either namespace or name is missing", key)
	}

	hybridJob, err := hjc.hybridJobClient.Get(name, ns)
	if err != nil {
		glog.Warningf("Finished syncing hybrid job %q (%v)", key, time.Now().Sub(startTime))
		return err
	}

	changed := false
	job := *hybridJob

	if job.Status.StartTime == nil {
		now := meta_v1.Now()
		job.Status.StartTime = &now
		job.Status.Phase = types.Creating
		// only create pods
		err := hjc.createAllPods(hybridJob)
		if err != nil {
			glog.Errorf("Failed to create pods to hybridjob %s/%s", hybridJob.Namespace, hybridJob.Name)
		}
		return hjc.updateHybridJob(hybridJob)
	}

	// if job was finished previously, we don't want to redo the termination
	if IsJobFinished(&job) {
		return nil
	}

	isRunning := 0
	targetRunning := len(job.Spec.ReplicaSpecs)

	hosts := map[types.TfReplicaType]string{}

	for _, tfReplica := range job.Spec.ReplicaSpecs {

		preStatus, ok := job.Status.TfReplicaStatus[tfReplica.TfReplicaType]
		if !ok {
			preStatus = &types.TfReplicaStatus{}
		}

		if preStatus.Phase == types.Finished || preStatus.Phase == types.Failed {
			continue
		}

		pods, err := hjc.getPodsForTfReplica(job.Namespace, tfReplica)
		if err != nil {
			return err
		}

		activePods := FilterActivePods(pods)
		active := int32(len(activePods))
		desired := preStatus.Desired
		succeeded, failed := getStatus(pods)
		phase := preStatus.Phase

		if active >= *tfReplica.MinReplicas && active <= *tfReplica.MaxReplicas && preStatus.Phase != types.Running {
			phase = types.Running
			preStatus.Desired = active
			glog.V(4).Infof("Hybridjob(%s/%s:%s) is running, delete unnecessary pods", hybridJob.Namespace, hybridJob.Name, string(tfReplica.TfReplicaType))
			pendingPods := FilterPendingPods(pods)
			for _, pod := range pendingPods {
				err = hjc.k8sClient.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &meta_v1.DeleteOptions{GracePeriodSeconds: tools.NewInt64(0)})
				if err != nil {
					glog.Errorf("Failed to delete pod %s/%s.", pod.Namespace, pod.Name)
				}
			}
			isRunning++
			hosts[tfReplica.TfReplicaType] = tools.GenerateHosts(activePods)
		} else if desired == succeeded && preStatus.Phase != types.Finished {
			glog.V(4).Infof("Hybridjob(%s/%s:%s) is Finished", hybridJob.Namespace, hybridJob.Name, string(tfReplica.TfReplicaType))
			phase = types.Finished
		} else if desired == failed && preStatus.Phase != types.Failed {
			glog.V(4).Infof("Hybridjob(%s/%s:%s) is Failed", hybridJob.Namespace, hybridJob.Name, string(tfReplica.TfReplicaType))
			phase = types.Failed
		}

		if preStatus.Phase != phase {
			changed = true
			preStatus.Phase = phase
			preStatus.Failed = failed
			preStatus.Succeeded = succeeded
			preStatus.Active = active
		}
	}

	if isRunning == targetRunning {
		changed = true
		glog.V(4).Infof("Hybridjob(%s/%s) is Ready", hybridJob.Namespace, hybridJob.Name)
		hybridJob.Status.Phase = types.Ready
		hybridJob.Status.PSHosts = hosts[types.PS]
		hybridJob.Status.WorkerHosts = hosts[types.WORKER]
	}

	if changed {
		return hjc.updateHybridJob(hybridJob)
	}

	return nil
}

func (hjc *HybridJobController) createAllPods(hj *types.HybridJob) error {
	commonLabels := hj.ObjectMeta.Labels
	for _, tfReplicaSpec := range hj.Spec.ReplicaSpecs {
		selector := &meta_v1.LabelSelector{
			MatchLabels: commonLabels,
		}
		selector.MatchLabels[Role] = string(tfReplicaSpec.TfReplicaType)
		tfReplicaSpec.Selector = selector
		for index := int32(0); index < *tfReplicaSpec.MaxReplicas; index++ {
			hjc.createPod(tfReplicaSpec, hj, index)
		}
	}
	return nil
}

func (hjc *HybridJobController) getPodsForTfReplica(namespace string, tfr *types.TfReplicaSpec) ([]v1.Pod, error) {
	selector, err := meta_v1.LabelSelectorAsSelector(tfr.Selector)
	if err != nil {
		return nil, fmt.Errorf("couldn't convert Job selector: %v", err)
	}

	pods, err := hjc.k8sClient.CoreV1().Pods(namespace).List(meta_v1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return nil, err
	}

	return pods.Items, nil
}

func (hjc *HybridJobController) updateHybridJob(hybridJob *types.HybridJob) error {
	if _, err := hjc.hybridJobClient.Update(hybridJob); err != nil {
		return err
	}
	return nil
}

func (hjc *HybridJobController) createPod(tfReplicaSpec *types.TfReplicaSpec, hybridJob *types.HybridJob, index int32) error {
	owner := hybridJob.Namespace + "/" + hybridJob.Name + ":" + string(tfReplicaSpec.TfReplicaType)

	pod, err := GetPodFromTemplate(tfReplicaSpec.Template, owner)
	if err != nil {
		return err
	}
	pod.Name = hybridJob.Name + "-" + strings.ToLower(string(tfReplicaSpec.TfReplicaType)) + "-" + strconv.Itoa(int(index))
	if len(tfReplicaSpec.NodeName) != 0 {
		pod.Spec.NodeName = tfReplicaSpec.NodeName
	}

	// add selector to pod's label
	for k, v := range tfReplicaSpec.Selector.MatchLabels {
		pod.ObjectMeta.Labels[k] = v
	}

	if labels.Set(pod.Labels).AsSelectorPreValidated().Empty() {
		return fmt.Errorf("unable to create pods, no labels")
	}

	if newPod, err := hjc.k8sClient.CoreV1().Pods(hybridJob.Namespace).Create(pod); err != nil {
		hjc.recorder.Eventf(hybridJob, v1.EventTypeWarning, "FailedCreate", "Error creating: %v", err)
		return fmt.Errorf("unable to create pods: %v", err)
	} else {
		glog.V(4).Infof("Controller %v created pod %v", types.HybridJobs, newPod.Name)
		hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "SuccessfulCreate", "Created pod: %v", newPod.Name)
	}
	return nil
}

func GetPodFromTemplate(template *v1.PodTemplateSpec, owner string) (*v1.Pod, error) {
	desiredLabels := tools.GetPodsLabelSet(template)
	desiredFinalizers := tools.GetPodsFinalizers(template)
	desiredAnnotations, err := tools.GetPodsAnnotationSet(template)
	if err != nil {
		return nil, err
	}

	pod := &v1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Labels:      desiredLabels,
			Annotations: desiredAnnotations,
			Finalizers:  desiredFinalizers,
		},
	}

	if owner != "" {
		// TODO: set pod owner here
	}

	clone, err := api.Scheme.DeepCopy(&template.Spec)
	if err != nil {
		return nil, err
	}
	pod.Spec = *clone.(*v1.PodSpec)
	return pod, nil
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

func IsPodActive(p v1.Pod) bool {
	return v1.PodSucceeded != p.Status.Phase &&
		v1.PodFailed != p.Status.Phase &&
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

// filterPods returns pods based on their phase.
func filterPods(pods []v1.Pod, phase v1.PodPhase) int {
	result := 0
	for i := range pods {
		if phase == pods[i].Status.Phase {
			result++
		}
	}
	return result
}

func getStatus(pods []v1.Pod) (succeeded, failed int32) {
	succeeded = int32(filterPods(pods, v1.PodSucceeded))
	failed = int32(filterPods(pods, v1.PodFailed))
	return
}
