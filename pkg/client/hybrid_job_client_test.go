package client

import (
	"github.com/yarntime/hybridjob/pkg/tools"
	"github.com/yarntime/hybridjob/pkg/types"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/client-go/pkg/api/v1"
	"testing"
)

func NewClient() *HybridJobClient {
	return NewHybridJobClient("192.168.254.45:8080")
}

var hybridJobClient *HybridJobClient

func init() {
	hybridJobClient = NewClient()
}

func TestCreateResource(t *testing.T) {
	job := &types.HybridJob{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      "job",
			Namespace: "default",
			Labels:    map[string]string{"mylabel": "test"},
		},
		Spec: types.HybridJobSpec{
			ReplicaSpecs: []*types.TfReplicaSpec{
				{
					MinReplicas:   tools.NewInt32(1),
					MaxReplicas:   tools.NewInt32(1),
					TfReplicaType: types.PS,
					Template: &corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{
								{
									Name:  "ps",
									Image: "nginx:1.3",
									Ports: []corev1.ContainerPort{
										{
											Name:          "http",
											Protocol:      corev1.ProtocolTCP,
											ContainerPort: 80,
										},
									},
								},
							},
						},
					},
				},
				{
					MinReplicas:   tools.NewInt32(1),
					MaxReplicas:   tools.NewInt32(1),
					TfReplicaType: types.WORKER,
					Template: &corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{
								{
									Name:  "worker",
									Image: "nginx:1.3",
									Ports: []corev1.ContainerPort{
										{
											Name:          "http",
											Protocol:      corev1.ProtocolTCP,
											ContainerPort: 80,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := hybridJobClient.Create(job)
	if err != nil {
		t.Errorf("Failed to create job: %v\n", err)
	}
}

func TestGetResource(t *testing.T) {
	_, err := hybridJobClient.Get("job", "default")
	if err != nil {
		t.Errorf("Failed to get job: %v\n", err)
	}
}

func TestUpdateResource(t *testing.T) {
	job, err := hybridJobClient.Get("job", "default")
	if err != nil {
		t.Errorf("Failed to get job: %v\n", err)
	}
	job.Labels["update"] = "true"
	_, err = hybridJobClient.Update(job)
	if err != nil {
		t.Errorf("Failed to update job: %v\n", err)
	}
}

func TestDeleteResource(t *testing.T) {
	err := hybridJobClient.Delete("job", "default", &meta_v1.DeleteOptions{})
	if err != nil {
		t.Errorf("Failed to delete job: %v\n", err)
	}
}
