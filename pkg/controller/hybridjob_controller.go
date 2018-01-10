package controller

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/yarntime/hybridjob/pkg/client"
	"github.com/yarntime/hybridjob/pkg/tools"
	"github.com/yarntime/hybridjob/pkg/types"
	"k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
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

	k8sClient := config.K8sClient
	hybridJobClient := config.HybridJobClient

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(k8sClient.CoreV1().RESTClient()).Events("")})

	hjc := &HybridJobController{
		k8sClient:             k8sClient,
		hybridJobClient:       hybridJobClient,
		recorder:              eventBroadcaster.NewRecorder(hybridJobClient.Scheme(), clientv1.EventSource{Component: "hybridjob-controller"}),
		concurrentJobHandlers: config.ConcurrentJobHandlers,
		queue:        workqueue.NewNamedRateLimitingQueue(tools.NewFixedItemIntervalRateLimiter(config.ResyncPeriod), "hybridjob"),
		resyncPeriod: config.ResyncPeriod,
	}

	_, hjlw := cache.NewInformer(
		hybridJobClient.NewListWatch(""),
		&types.HybridJob{},
		hjc.resyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc: hjc.enqueueController,
			DeleteFunc: func(obj interface{}) {
				job := obj.(*types.HybridJob)
				hjc.deleteAllPods(job)
			},
			UpdateFunc: func(old, cur interface{}) {
				if job := cur.(*types.HybridJob); !IsJobFinished(job) {
					hjc.enqueueController(job)
				}
			},
		},
	)

	_, plw := cache.NewInformer(
		cache.NewListWatchFromClient(k8sClient.CoreV1().RESTClient(), "pods", meta_v1.NamespaceAll, fields.Everything()),
		&v1.Pod{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    hjc.addPod,
			UpdateFunc: hjc.updatePod,
			DeleteFunc: hjc.deletePod,
		},
	)

	go hjlw.Run(config.StopCh)
	go plw.Run(config.StopCh)

	return hjc
}

func (hjc *HybridJobController) addPod(obj interface{}) {
	pod := obj.(*v1.Pod)
	if pod.DeletionTimestamp != nil {
		hjc.deletePod(pod)
		return
	}
	key := resolveControllerRef(pod.OwnerReferences)
	if key != "" {
		hjc.queue.Add(key)
	}
}

func (hjc *HybridJobController) updatePod(old, cur interface{}) {
	curPod := cur.(*v1.Pod)
	oldPod := old.(*v1.Pod)

	if curPod.ResourceVersion == oldPod.ResourceVersion {
		return
	}
	if curPod.DeletionTimestamp != nil {
		hjc.deletePod(curPod)
		return
	}

	curkey := resolveControllerRef(curPod.OwnerReferences)
	if curkey != "" {
		hjc.queue.Add(curkey)
	}
}

func (hjc *HybridJobController) deletePod(obj interface{}) {
	pod := obj.(*v1.Pod)
	key := resolveControllerRef(pod.OwnerReferences)
	if key != "" {
		hjc.queue.Add(key)
	}
}

func resolveControllerRef(rfs []meta_v1.OwnerReference) (key string) {
	for _, rf := range rfs {
		if rf.Kind == types.HybridJobs {
			return rf.Name
		}
	}
	return ""
}

func (hjc *HybridJobController) enqueueController(obj interface{}) {
	job := obj.(*types.HybridJob)
	key := tools.GetKeyOfHybridJob(job)
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
		glog.Warningf("Failed get hybrid job %q (%v) from kubernetes", key, time.Now().Sub(startTime))
		if errors.IsNotFound(err) {
			glog.V(4).Infof("Hybridjob has been deleted: %v", key)
			return nil
		}
		return err
	}

	changed := false

	if hybridJob.Status.StartTime == nil {
		now := meta_v1.Now()
		hybridJob.Status.StartTime = &now
		hybridJob.Status.Phase = types.Creating
		// only create pods
		err := hjc.createAllPods(hybridJob)
		if err != nil {
			hybridJob.Status.Phase = types.Failed
			glog.Errorf("Failed to create pods in hybridjob %s/%s", hybridJob.Namespace, hybridJob.Name)
			hjc.recorder.Event(hybridJob, v1.EventTypeWarning, "HybridJobFailed", "Failed to create all pods")
		}
		return hjc.updateHybridJob(hybridJob)
	}

	// if job was finished previously, we don't want to redo the termination
	if IsJobFinished(hybridJob) {
		return nil
	}

	isRunning := 0
	isFinished := 0
	isFailed := 0
	targetNum := len(hybridJob.Spec.ReplicaSpecs)

	for _, tfReplica := range hybridJob.Spec.ReplicaSpecs {

		preStatus, ok := hybridJob.Status.TfReplicaStatus[tfReplica.TfReplicaType]
		if !ok {
			preStatus = &types.TfReplicaStatus{}
		}

		if preStatus.Phase == types.Finished {
			isFinished++
			continue
		}

		if preStatus.Phase == types.Failed {
			continue
		}

		pods, err := hjc.getPodsForTfReplica(hybridJob.Namespace, tfReplica)
		if err != nil {
			return err
		}

		activePods := FilterActivePods(pods)
		active := int32(len(activePods))
		desired := preStatus.Desired
		succeeded, failed := getStatus(pods)
		phase := preStatus.Phase

		// TODO: handle other cases
		if active >= *tfReplica.MinReplicas && active <= *tfReplica.MaxReplicas && preStatus.Phase == types.Creating {
			phase = types.Running
			preStatus.Desired = active
			glog.V(4).Infof("TfReplica(%s/%s:%s) is running, delete unnecessary pods", hybridJob.Namespace, hybridJob.Name, string(tfReplica.TfReplicaType))
			pendingPods := FilterPendingPods(pods)
			for _, pod := range pendingPods {
				err = hjc.k8sClient.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &meta_v1.DeleteOptions{GracePeriodSeconds: tools.NewInt64(0)})
				if err != nil {
					glog.Errorf("Failed to delete pod %s/%s.", pod.Namespace, pod.Name)
				}
			}
			if hybridJob.Status.Hosts == nil {
				hybridJob.Status.Hosts = make(map[types.TfReplicaType]string)
			}
			hybridJob.Status.Hosts[tfReplica.TfReplicaType] = tools.GenerateHosts(activePods)
			hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "TfReplicaRunning", "Successful to start all containers in %s", string(tfReplica.TfReplicaType))
		} else if desired != 0 && desired == succeeded && preStatus.Phase == types.Running {
			glog.V(4).Infof("TfReplica(%s/%s:%s) is Finished", hybridJob.Namespace, hybridJob.Name, string(tfReplica.TfReplicaType))
			phase = types.Finished
			hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "TfReplicaFinished", "Successful to run all containers in %s", string(tfReplica.TfReplicaType))
		} else if desired != 0 && desired == failed && preStatus.Phase == types.Running {
			glog.V(4).Infof("TfReplica(%s/%s:%s) is Failed", hybridJob.Namespace, hybridJob.Name, string(tfReplica.TfReplicaType))
			phase = types.Failed
			isFailed++
			hjc.recorder.Eventf(hybridJob, v1.EventTypeWarning, "TfReplicaFailed", "Failed to run container in %s", string(tfReplica.TfReplicaType))
		}

		if phase == types.Running {
			isRunning++
		}

		if phase == types.Finished {
			isFinished++
		}

		if preStatus.Phase != phase {
			changed = true
			preStatus.Phase = phase
			preStatus.Failed = failed
			preStatus.Succeeded = succeeded
			preStatus.Active = active
		}
	}

	if isRunning == targetNum && hybridJob.Status.Phase != types.Ready {
		changed = true
		glog.V(4).Infof("Hybridjob(%s/%s) is Ready", hybridJob.Namespace, hybridJob.Name)
		hybridJob.Status.Phase = types.Ready
		time.Sleep(50 * time.Microsecond)
		hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "HybridJobReady", "Successful to start all containers in %s", tools.GetKeyOfHybridJob(hybridJob))
	}

	if isFinished == targetNum && hybridJob.Status.Phase != types.Finished {
		changed = true
		glog.V(4).Infof("Hybridjob(%s/%s) is Finished", hybridJob.Namespace, hybridJob.Name)
		hybridJob.Status.Phase = types.Finished
		time.Sleep(50 * time.Microsecond)
		hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "HybridJobFinished", "Finished to run all containers in %s", tools.GetKeyOfHybridJob(hybridJob))
	}

	if isFailed > 0 && hybridJob.Status.Phase != types.Failed {
		changed = true
		glog.V(4).Infof("Hybridjob(%s/%s) is Failed", hybridJob.Namespace, hybridJob.Name)
		hybridJob.Status.Phase = types.Failed
		time.Sleep(50 * time.Microsecond)
		hjc.recorder.Eventf(hybridJob, v1.EventTypeWarning, "HybridJobFailed", "Failed to run all containers in %s", tools.GetKeyOfHybridJob(hybridJob))
		hjc.deleteAllPods(hybridJob)
	}

	if changed {
		return hjc.updateHybridJob(hybridJob)
	}

	return nil
}

func (hjc *HybridJobController) createAllPods(hj *types.HybridJob) error {
	commonLabels := hj.ObjectMeta.Labels
	hj.Status.TfReplicaStatus = make(map[types.TfReplicaType]*types.TfReplicaStatus)
	createdPods := []*v1.Pod{}
	for _, tfReplicaSpec := range hj.Spec.ReplicaSpecs {
		selector := &meta_v1.LabelSelector{
			MatchLabels: make(map[string]string),
		}
		for k, v := range commonLabels {
			selector.MatchLabels[k] = v
		}
		selector.MatchLabels[Role] = string(tfReplicaSpec.TfReplicaType)
		tfReplicaSpec.Selector = selector
		for index := int32(0); index < *tfReplicaSpec.MaxReplicas; index++ {
			pod, err := hjc.createPod(tfReplicaSpec, hj, index)
			if err != nil {
				hjc.deleteCreatedPods(hj, createdPods)
				return err
			}
			createdPods = append(createdPods, pod)
		}
		hj.Status.TfReplicaStatus[tfReplicaSpec.TfReplicaType] = &types.TfReplicaStatus{
			Phase: types.Creating,
		}
	}
	return nil
}

func (hjc *HybridJobController) deleteCreatedPods(hj *types.HybridJob, pods []*v1.Pod) {
	for _, pod := range pods {
		hjc.k8sClient.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &meta_v1.DeleteOptions{GracePeriodSeconds: tools.NewInt64(0)})
		hjc.recorder.Eventf(hj, v1.EventTypeNormal, "SuccessfulDelete", "Deleted pod: %v", pod.Name)
	}
}

func (hjc *HybridJobController) deleteAllPods(hybridJob *types.HybridJob) error {
	for _, tfReplica := range hybridJob.Spec.ReplicaSpecs {
		pods, err := hjc.getPodsForTfReplica(hybridJob.Namespace, tfReplica)
		if err != nil {
			return err
		}
		for _, pod := range pods {
			hjc.k8sClient.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &meta_v1.DeleteOptions{})
			hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "SuccessfulDelete", "Deleted pod: %v", pod.Name)
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

func (hjc *HybridJobController) createPod(tfReplicaSpec *types.TfReplicaSpec, hybridJob *types.HybridJob, index int32) (*v1.Pod, error) {
	pod, err := GetPodFromTemplate(tfReplicaSpec, hybridJob)
	if err != nil {
		return nil, err
	}

	pod.Name = hybridJob.Name + "-" + strings.ToLower(string(tfReplicaSpec.TfReplicaType)) + "-" + strconv.Itoa(int(index))
	pod.OwnerReferences[0].UID = *meta_v1.NewUIDPreconditions(pod.Name).UID

	if len(tfReplicaSpec.NodeName) != 0 {
		pod.Spec.NodeName = tfReplicaSpec.NodeName
	}

	// add selector to pod's label
	for k, v := range tfReplicaSpec.Selector.MatchLabels {
		pod.ObjectMeta.Labels[k] = v
	}

	if labels.Set(pod.Labels).AsSelectorPreValidated().Empty() {
		return nil, fmt.Errorf("unable to create pods, no labels")
	}

	newPod, err := hjc.k8sClient.CoreV1().Pods(hybridJob.Namespace).Create(pod)

	if err != nil {
		hjc.recorder.Eventf(hybridJob, v1.EventTypeWarning, "FailedCreate", "Error creating: %v", err)
		return nil, fmt.Errorf("unable to create pods: %v", err)
	} else {
		glog.V(4).Infof("Controller %v created pod %v", types.HybridJobs, newPod.Name)
		hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "SuccessfulCreate", "Created pod: %v", newPod.Name)
	}
	return newPod, nil
}

func GetPodFromTemplate(tfReplicaSpec *types.TfReplicaSpec, hybridJob *types.HybridJob) (*v1.Pod, error) {
	template := tfReplicaSpec.Template
	key := tools.GetKeyOfHybridJob(hybridJob)
	desiredLabels := tools.GetPodsLabelSet(template)
	desiredFinalizers := tools.GetPodsFinalizers(template)
	desiredAnnotations, err := tools.GetPodsAnnotationSet(key, tfReplicaSpec)
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

	pod.ObjectMeta.OwnerReferences = []meta_v1.OwnerReference{
		{
			APIVersion: types.Version,
			Kind:       types.HybridJobs,
			Name:       key,
		},
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
