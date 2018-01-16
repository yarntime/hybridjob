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
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"strconv"
	"strings"
	"time"
)

const (
	Role           = "ecp-role"
	Scheduled      = "scheduled"
	Cause          = "cause"
	OwnerReference = "ecp-owner-reference"
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

func NewController(config *Config) *HybridJobController {

	k8sClient := config.K8sClient
	hybridJobClient := config.HybridJobClient

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(k8sClient.CoreV1().RESTClient()).Events("")})

	hjc := &HybridJobController{
		k8sClient:             k8sClient,
		hybridJobClient:       hybridJobClient,
		recorder:              eventBroadcaster.NewRecorder(hybridJobClient.Scheme(), v1.EventSource{Component: "hybridjob-controller"}),
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
				hjc.deleteAllResources(job)
			},
			UpdateFunc: func(old, cur interface{}) {
				if job := cur.(*types.HybridJob); !tools.IsJobFinished(job) {
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

	_, clw := cache.NewInformer(
		cache.NewListWatchFromClient(k8sClient.CoreV1().RESTClient(), "configmaps", meta_v1.NamespaceAll, fields.Everything()),
		&v1.ConfigMap{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    hjc.addConfigMap,
			UpdateFunc: hjc.updateConfigMap,
			DeleteFunc: hjc.deleteConfigMap,
		},
	)

	go hjlw.Run(config.StopCh)
	go plw.Run(config.StopCh)
	go clw.Run(config.StopCh)

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

func (hjc *HybridJobController) addConfigMap(obj interface{}) {
	configMap := obj.(*v1.ConfigMap)
	key := resolveControllerRef(configMap.OwnerReferences)
	if key != "" {
		hjc.queue.Add(key)
	}
	return
}

func (hjc *HybridJobController) updateConfigMap(old, cur interface{}) {
	curConfigMap := cur.(*v1.ConfigMap)
	oldConfigMap := old.(*v1.ConfigMap)

	if curConfigMap.ResourceVersion == oldConfigMap.ResourceVersion {
		return
	}
	if curConfigMap.DeletionTimestamp != nil {
		return
	}

	curkey := resolveControllerRef(curConfigMap.OwnerReferences)
	if curkey != "" {
		hjc.queue.Add(curkey)
	}
}

func (hjc *HybridJobController) deleteConfigMap(obj interface{}) {
	configMap := obj.(*v1.ConfigMap)
	key := resolveControllerRef(configMap.OwnerReferences)
	if key != "" {
		hjc.queue.Add(key)
	}
	return
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

	if hybridJob.Status.StartTime == nil {
		now := meta_v1.Now()
		hybridJob.Spec.Selector = &meta_v1.LabelSelector{
			MatchLabels: hybridJob.ObjectMeta.Labels,
		}
		hybridJob.Status.StartTime = &now
		hybridJob.Status.Phase = types.Creating

		hjc.createAllResources(hybridJob)

		return hjc.updateHybridJob(hybridJob)
	}

	// if job was finished previously, we don't want to redo the termination
	if tools.IsJobFinished(hybridJob) {
		return nil
	}

	hybridJob.Status.IsChanged = false

	if !tools.IsJobScheduled(hybridJob) {
		isScheduled, err := hjc.isJobScheduled(hybridJob)
		if err != nil {
			if errors.IsNotFound(err) {
				glog.Warningf("ConfigMap has been deleted: %v", key)
				hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "Error", "ConfigMap has been deleted %s", key)
				hjc.deleteAllResources(hybridJob)
				return nil
			}
			return nil
		}

		if !isScheduled {
			glog.V(4).Infof("Hybrid job is not scheduled.")
			return nil
		} else {
			tools.SetHybridJobPhase(hybridJob, types.Scheduled)
		}
	}

	isRunning := 0
	isFinished := 0
	isFailed := 0
	targetNum := len(hybridJob.Spec.ReplicaSpecs)

	for _, tfReplica := range hybridJob.Spec.ReplicaSpecs {
		phase, err := hjc.processTfReplica(tfReplica, hybridJob)
		if err != nil {
			return err
		}
		if phase == types.Finished {
			isFinished++
		} else if phase == types.Failed {
			isFailed++
		} else if phase == types.Running {
			isRunning++
		}
	}

	if isRunning == targetNum && hybridJob.Status.Phase != types.Ready {
		glog.V(4).Infof("Hybridjob(%s/%s) is Ready", hybridJob.Namespace, hybridJob.Name)
		tools.SetHybridJobPhase(hybridJob, types.Ready)
		time.Sleep(50 * time.Microsecond)
		hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "HybridJobReady", "Successful to start all containers in %s", tools.GetKeyOfHybridJob(hybridJob))
	}

	if isFinished == targetNum && hybridJob.Status.Phase != types.Finished {
		glog.V(4).Infof("Hybridjob(%s/%s) is Finished", hybridJob.Namespace, hybridJob.Name)
		tools.SetHybridJobPhase(hybridJob, types.Finished)
		time.Sleep(50 * time.Microsecond)
		hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "HybridJobFinished", "Finished to run all containers in %s", tools.GetKeyOfHybridJob(hybridJob))
	}

	if isFailed > 0 && hybridJob.Status.Phase != types.Failed {
		glog.V(4).Infof("Hybridjob(%s/%s) is Failed", hybridJob.Namespace, hybridJob.Name)
		tools.SetHybridJobPhase(hybridJob, types.Failed)
		time.Sleep(50 * time.Microsecond)
		hjc.recorder.Eventf(hybridJob, v1.EventTypeWarning, "HybridJobFailed", "Failed to run all containers in %s", tools.GetKeyOfHybridJob(hybridJob))
		hjc.deleteAllResources(hybridJob)
	}

	if hybridJob.Status.IsChanged {
		return hjc.updateHybridJob(hybridJob)
	}

	return nil
}

func (hjc *HybridJobController) isJobScheduled(hj *types.HybridJob) (bool, error) {
	conf, err := hjc.k8sClient.CoreV1().ConfigMaps(hj.Namespace).Get(hj.Name, meta_v1.GetOptions{})
	if err != nil {
		glog.Warningf("Failed to get config from kubernetes: %s/%s", hj.Namespace, hj.Name)
		return false, err
	}
	scheduled := conf.Data[Scheduled]
	result, err := strconv.ParseBool(scheduled)
	if err != nil {
		glog.Warningf("Failed to parse scheduled to bool %s", scheduled)
		return false, err
	}

	if !result {
		cause, ok := conf.Data[Cause]
		if ok {
			hjc.recorder.Event(hj, v1.EventTypeWarning, "ScheduleFailCause", cause)
		}
	}
	return result, nil
}

func (hjc *HybridJobController) createAllResources(hj *types.HybridJob) error {
	err := hjc.createConfigMap(hj)
	if err != nil {
		return err
	} else {
		err = hjc.createPods(hj)
		if err != nil {
			hj.Status.Phase = types.Failed
			glog.Errorf("Failed to create pods in hybridjob %s/%s", hj.Namespace, hj.Name)
			hjc.recorder.Event(hj, v1.EventTypeWarning, "HybridJobFailed", "Failed to create all pods")
		}
	}
	return err
}

func (hjc *HybridJobController) createConfigMap(hj *types.HybridJob) error {
	conf := &v1.ConfigMap{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      hj.Name,
			Namespace: hj.Namespace,
			OwnerReferences: []meta_v1.OwnerReference{
				{
					APIVersion: types.Version,
					Kind:       types.HybridJobs,
					Name:       tools.GetKeyOfHybridJob(hj),
					UID:        *meta_v1.NewUIDPreconditions(hj.Name).UID,
				},
			},
		},
		Data: map[string]string{Scheduled: "false"},
	}

	_, err := hjc.k8sClient.CoreV1().ConfigMaps(conf.Namespace).Create(conf)
	if err != nil {
		hjc.recorder.Eventf(hj, v1.EventTypeWarning, "FailedCreate", "ConfigMap: %s/%s", conf.Namespace, conf.Name)
	}
	return err
}

func (hjc *HybridJobController) createPods(hj *types.HybridJob) error {
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

func (hjc *HybridJobController) processTfReplica(tfReplica *types.TfReplicaSpec, hybridJob *types.HybridJob) (types.JobPhase, error) {

	preStatus, ok := hybridJob.Status.TfReplicaStatus[tfReplica.TfReplicaType]
	if !ok {
		preStatus = &types.TfReplicaStatus{}
	}

	phase := preStatus.Phase

	if phase == types.Failed || phase == types.Finished {
		return phase, nil
	}

	pods, err := hjc.getPodsForTfReplica(hybridJob.Namespace, tfReplica)
	if err != nil {
		return phase, err
	}

	activePods := tools.FilterActivePods(pods)
	active := int32(len(activePods))
	desired := preStatus.Desired
	succeeded, failed := getStatus(pods)

	if desired == 0 {
		assignedPods, unassignedPods := tools.FilterAssignedPods(pods)
		desired = int32(len(assignedPods))
		glog.V(4).Infof("TfReplica(%s/%s:%s) is assigned, delete unnecessary pods", hybridJob.Namespace, hybridJob.Name, string(tfReplica.TfReplicaType))

		for _, pod := range unassignedPods {
			err = hjc.k8sClient.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &meta_v1.DeleteOptions{GracePeriodSeconds: tools.NewInt64(0)})
			if err != nil {
				glog.Errorf("Failed to delete pod %s/%s.", pod.Namespace, pod.Name)
			}
			hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "SuccessfulDelete", "Delete unassigned pod %s", pod.Name)
		}
	}

	// TODO: handle other cases
	if desired != 0 && active == desired && phase != types.Running {
		phase = types.Running
		preStatus.Desired = active
		if hybridJob.Status.Hosts == nil {
			hybridJob.Status.Hosts = make(map[types.TfReplicaType]string)
		}
		hybridJob.Status.Hosts[tfReplica.TfReplicaType] = tools.GenerateHosts(activePods)
		hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "TfReplicaRunning", "Successful to start all containers in %s", string(tfReplica.TfReplicaType))
	} else if desired != 0 && desired == succeeded && preStatus.Phase != types.Finished {
		glog.V(4).Infof("TfReplica(%s/%s:%s) is Finished", hybridJob.Namespace, hybridJob.Name, string(tfReplica.TfReplicaType))
		phase = types.Finished
		hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "TfReplicaFinished", "Successful to run all containers in %s", string(tfReplica.TfReplicaType))
	} else if desired != 0 && desired == failed && preStatus.Phase != types.Failed {
		glog.V(4).Infof("TfReplica(%s/%s:%s) is Failed", hybridJob.Namespace, hybridJob.Name, string(tfReplica.TfReplicaType))
		phase = types.Failed
		hjc.recorder.Eventf(hybridJob, v1.EventTypeWarning, "TfReplicaFailed", "Failed to run container in %s", string(tfReplica.TfReplicaType))
	}

	if preStatus.Phase != phase || preStatus.Failed != failed || preStatus.Succeeded != succeeded || preStatus.Desired != desired || preStatus.Active != active {
		hybridJob.Status.IsChanged = true
		preStatus.Phase = phase
		preStatus.Failed = failed
		preStatus.Succeeded = succeeded
		preStatus.Active = active
		preStatus.Desired = desired
	}

	return phase, nil
}

func (hjc *HybridJobController) deleteCreatedPods(hj *types.HybridJob, pods []*v1.Pod) {
	for _, pod := range pods {
		hjc.k8sClient.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &meta_v1.DeleteOptions{GracePeriodSeconds: tools.NewInt64(0)})
		hjc.recorder.Eventf(hj, v1.EventTypeNormal, "SuccessfulDelete", "Deleted pod: %v", pod.Name)
	}
}

func (hjc *HybridJobController) deleteAllResources(hybridJob *types.HybridJob) error {

	err := hjc.k8sClient.ConfigMaps(hybridJob.Namespace).Delete(hybridJob.Name, &meta_v1.DeleteOptions{})

	if err == nil {
		hjc.recorder.Eventf(hybridJob, v1.EventTypeNormal, "SuccessfulDelete", "Deleted ConfigMap: %s", hybridJob.Name)
	}

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
	desiredAnnotations, err := tools.GetPodsAnnotationSet(key, int32(len(hybridJob.Spec.ReplicaSpecs)), tfReplicaSpec)
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
			UID:        *meta_v1.NewUIDPreconditions(hybridJob.Name).UID,
		},
	}

	clone, err := api.Scheme.DeepCopy(&template.Spec)
	if err != nil {
		return nil, err
	}
	pod.Spec = *clone.(*v1.PodSpec)
	return pod, nil
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
