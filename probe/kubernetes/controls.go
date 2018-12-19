package kubernetes

import (
	"io"
	"io/ioutil"

	"github.com/weaveworks/scope/common/xfer"
	"github.com/weaveworks/scope/probe/controls"
	"github.com/weaveworks/scope/report"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Control IDs used by the kubernetes integration.
const (
	CloneVolumeSnapshot  = report.KubernetesCloneVolumeSnapshot
	CreateVolumeSnapshot = report.KubernetesCreateVolumeSnapshot
	GetLogs              = report.KubernetesGetLogs
	DescribePod          = report.KubernetesDescribePod
	DescribeService      = report.KubernetesDescribeService
	DescribeCronJob      = report.KubernetesCronjob
	DescribeDeployment   = report.KubernetesDescribeDeployment
	DescribeDaemonSet    = report.KubernetesDescribeDaemonSet
	DescribePVC          = report.KubernetesDescribePVC
	DescribePV           = report.KubernetesDescribePV
	DescribeSC           = report.KubernetesDescribeSC
	DescribeStatefulSet  = report.KubernetesDescribeStatefulSet
	DeletePod            = report.KubernetesDeletePod
	DeleteVolumeSnapshot = report.KubernetesDeleteVolumeSnapshot
	ScaleUp              = report.KubernetesScaleUp
	ScaleDown            = report.KubernetesScaleDown
)

// GetLogs is the control to get the logs for a kubernetes pod
func (r *Reporter) GetLogs(req xfer.Request, namespaceID, podID string, containerNames []string, _ schema.GroupKind) xfer.Response {
	readCloser, err := r.client.GetLogs(namespaceID, podID, containerNames)
	if err != nil {
		return xfer.ResponseError(err)
	}

	readWriter := struct {
		io.Reader
		io.Writer
	}{
		readCloser,
		ioutil.Discard,
	}
	id, pipe, err := controls.NewPipeFromEnds(nil, readWriter, r.pipes, req.AppID)
	if err != nil {
		return xfer.ResponseError(err)
	}
	pipe.OnClose(func() {
		readCloser.Close()
	})
	return xfer.Response{
		Pipe: id,
	}
}

func (r *Reporter) describePod(req xfer.Request, namespaceID, podID string, _ []string, groupKind schema.GroupKind) xfer.Response {
	return r.describe(req, namespaceID, podID, groupKind)
}

func (r *Reporter) describePVC(req xfer.Request, namespaceID, pvcID, _ string, groupKind schema.GroupKind) xfer.Response {
	return r.describe(req, namespaceID, pvcID, groupKind)
}

// GetLogs is the control to get the logs for a kubernetes pod
func (r *Reporter) describe(req xfer.Request, namespaceID, resourceID string, groupKind schema.GroupKind) xfer.Response {
	readCloser, err := r.client.Describe(namespaceID, resourceID, groupKind)
	if err != nil {
		return xfer.ResponseError(err)
	}

	readWriter := struct {
		io.Reader
		io.Writer
	}{
		readCloser,
		ioutil.Discard,
	}
	id, pipe, err := controls.NewPipeFromEnds(nil, readWriter, r.pipes, req.AppID)
	if err != nil {
		return xfer.ResponseError(err)
	}
	pipe.OnClose(func() {
		readCloser.Close()
	})
	return xfer.Response{
		Pipe: id,
	}
}

func (r *Reporter) cloneVolumeSnapshot(req xfer.Request, namespaceID, volumeSnapshotID, persistentVolumeClaimID, capacity string) xfer.Response {
	err := r.client.CloneVolumeSnapshot(namespaceID, volumeSnapshotID, persistentVolumeClaimID, capacity)
	if err != nil {
		return xfer.ResponseError(err)
	}
	return xfer.Response{}
}

func (r *Reporter) createVolumeSnapshot(req xfer.Request, namespaceID, persistentVolumeClaimID, capacity string, _ schema.GroupKind) xfer.Response {
	err := r.client.CreateVolumeSnapshot(namespaceID, persistentVolumeClaimID, capacity)
	if err != nil {
		return xfer.ResponseError(err)
	}
	return xfer.Response{}
}

func (r *Reporter) deletePod(req xfer.Request, namespaceID, podID string, _ []string, _ schema.GroupKind) xfer.Response {
	if err := r.client.DeletePod(namespaceID, podID); err != nil {
		return xfer.ResponseError(err)
	}
	return xfer.Response{
		RemovedNode: req.NodeID,
	}
}

func (r *Reporter) deleteVolumeSnapshot(req xfer.Request, namespaceID, volumeSnapshotID, _, _ string) xfer.Response {
	if err := r.client.DeleteVolumeSnapshot(namespaceID, volumeSnapshotID); err != nil {
		return xfer.ResponseError(err)
	}
	return xfer.Response{
		RemovedNode: req.NodeID,
	}
}

// CapturePod is exported for testing
func (r *Reporter) CapturePod(f func(xfer.Request, string, string, []string, schema.GroupKind) xfer.Response) func(xfer.Request) xfer.Response {
	return func(req xfer.Request) xfer.Response {
		uid, ok := report.ParsePodNodeID(req.NodeID)
		if !ok {
			return xfer.ResponseErrorf("Invalid ID: %s", req.NodeID)
		}
		// find pod by UID
		var pod Pod
		r.client.WalkPods(func(p Pod) error {
			if p.UID() == uid {
				pod = p
			}
			return nil
		})
		if pod == nil {
			return xfer.ResponseErrorf("Pod not found: %s", uid)
		}
		return f(req, pod.Namespace(), pod.Name(), pod.ContainerNames(), ResourceMap["Pod"])
	}
}

// CaptureDeployment is exported for testing
func (r *Reporter) CaptureDeployment(f func(xfer.Request, string, string, schema.GroupKind) xfer.Response) func(xfer.Request) xfer.Response {
	return func(req xfer.Request) xfer.Response {
		uid, ok := report.ParseDeploymentNodeID(req.NodeID)
		if !ok {
			return xfer.ResponseErrorf("Invalid ID: %s", req.NodeID)
		}
		var deployment Deployment
		r.client.WalkDeployments(func(d Deployment) error {
			if d.UID() == uid {
				deployment = d
			}
			return nil
		})
		if deployment == nil {
			return xfer.ResponseErrorf("Deployment not found: %s", uid)
		}
		return f(req, deployment.Namespace(), deployment.Name(), ResourceMap["Deployment"])
	}
}

// CapturePersistentVolumeClaim will return name, namespace and capacity of PVC
func (r *Reporter) CapturePersistentVolumeClaim(f func(xfer.Request, string, string, string, schema.GroupKind) xfer.Response) func(xfer.Request) xfer.Response {
	return func(req xfer.Request) xfer.Response {
		uid, ok := report.ParsePersistentVolumeClaimNodeID(req.NodeID)
		if !ok {
			return xfer.ResponseErrorf("Invalid ID: %s", req.NodeID)
		}
		// find persistentVolumeClaim by UID
		var persistentVolumeClaim PersistentVolumeClaim
		r.client.WalkPersistentVolumeClaims(func(p PersistentVolumeClaim) error {
			if p.UID() == uid {
				persistentVolumeClaim = p
			}
			return nil
		})
		if persistentVolumeClaim == nil {
			return xfer.ResponseErrorf("Persistent volume claim not found: %s", uid)
		}
		return f(req, persistentVolumeClaim.Namespace(), persistentVolumeClaim.Name(), persistentVolumeClaim.GetCapacity(), ResourceMap["PersistentVolumeClaim"])
	}
}

// CaptureVolumeSnapshot will return name, pvc name, namespace and capacity of volume snapshot
func (r *Reporter) CaptureVolumeSnapshot(f func(xfer.Request, string, string, string, string) xfer.Response) func(xfer.Request) xfer.Response {
	return func(req xfer.Request) xfer.Response {
		uid, ok := report.ParseVolumeSnapshotNodeID(req.NodeID)
		if !ok {
			return xfer.ResponseErrorf("Invalid ID: %s", req.NodeID)
		}
		// find volume snapshot by UID
		var volumeSnapshot VolumeSnapshot
		r.client.WalkVolumeSnapshots(func(p VolumeSnapshot) error {
			if p.UID() == uid {
				volumeSnapshot = p
			}
			return nil
		})
		if volumeSnapshot == nil {
			return xfer.ResponseErrorf("Volume snapshot not found: %s", uid)
		}
		return f(req, volumeSnapshot.Namespace(), volumeSnapshot.Name(), volumeSnapshot.GetVolumeName(), volumeSnapshot.GetCapacity())
	}
}

// CaptureService is exported for testing
func (r *Reporter) CaptureService(f func(xfer.Request, string, string, schema.GroupKind) xfer.Response) func(xfer.Request) xfer.Response {
	return func(req xfer.Request) xfer.Response {
		uid, ok := report.ParseServiceNodeID(req.NodeID)
		if !ok {
			return xfer.ResponseErrorf("Invalid ID: %s", req.NodeID)
		}
		var service Service
		r.client.WalkServices(func(s Service) error {
			if s.UID() == uid {
				service = s
			}
			return nil
		})
		if service == nil {
			return xfer.ResponseErrorf("Service not found: %s", uid)
		}
		return f(req, service.Namespace(), service.Name(), ResourceMap["Service"])
	}
}

// CaptureDaemonSet is exported for testing
func (r *Reporter) CaptureDaemonSet(f func(xfer.Request, string, string, schema.GroupKind) xfer.Response) func(xfer.Request) xfer.Response {
	return func(req xfer.Request) xfer.Response {
		uid, ok := report.ParseDaemonSetNodeID(req.NodeID)
		if !ok {
			return xfer.ResponseErrorf("Invalid ID: %s", req.NodeID)
		}
		var daemonSet DaemonSet
		r.client.WalkDaemonSets(func(d DaemonSet) error {
			if d.UID() == uid {
				daemonSet = d
			}
			return nil
		})
		if daemonSet == nil {
			return xfer.ResponseErrorf("Daemon Set not found: %s", uid)
		}
		return f(req, daemonSet.Namespace(), daemonSet.Name(), ResourceMap["DaemonSet"])
	}
}

// CaptureCronJob is exported for testing
func (r *Reporter) CaptureCronJob(f func(xfer.Request, string, string, schema.GroupKind) xfer.Response) func(xfer.Request) xfer.Response {
	return func(req xfer.Request) xfer.Response {
		uid, ok := report.ParseCronJobNodeID(req.NodeID)
		if !ok {
			return xfer.ResponseErrorf("Invalid ID: %s", req.NodeID)
		}
		var cronJob CronJob
		r.client.WalkCronJobs(func(c CronJob) error {
			if c.UID() == uid {
				cronJob = c
			}
			return nil
		})
		if cronJob == nil {
			return xfer.ResponseErrorf("Cron Job not found: %s", uid)
		}
		return f(req, cronJob.Namespace(), cronJob.Name(), ResourceMap["CronJob"])
	}
}

// CaptureStatefulSet is exported for testing
func (r *Reporter) CaptureStatefulSet(f func(xfer.Request, string, string, schema.GroupKind) xfer.Response) func(xfer.Request) xfer.Response {
	return func(req xfer.Request) xfer.Response {
		uid, ok := report.ParseStatefulSetNodeID(req.NodeID)
		if !ok {
			return xfer.ResponseErrorf("Invalid ID: %s", req.NodeID)
		}
		var statefulSet StatefulSet
		r.client.WalkStatefulSets(func(s StatefulSet) error {
			if s.UID() == uid {
				statefulSet = s
			}
			return nil
		})
		if statefulSet == nil {
			return xfer.ResponseErrorf("Stateful Set not found: %s", uid)
		}
		return f(req, statefulSet.Namespace(), statefulSet.Name(), ResourceMap["StatefulSet"])
	}
}

// CaptureStorageClass is exported for testing
func (r *Reporter) CaptureStorageClass(f func(xfer.Request, string, string, schema.GroupKind) xfer.Response) func(xfer.Request) xfer.Response {
	return func(req xfer.Request) xfer.Response {
		uid, ok := report.ParseStorageClassNodeID(req.NodeID)
		if !ok {
			return xfer.ResponseErrorf("Invalid ID: %s", req.NodeID)
		}
		var storageClass StorageClass
		r.client.WalkStorageClasses(func(s StorageClass) error {
			if s.UID() == uid {
				storageClass = s
			}
			return nil
		})
		if storageClass == nil {
			return xfer.ResponseErrorf("StorageClass not found: %s", uid)
		}
		return f(req, "", storageClass.Name(), ResourceMap["StorageClass"])
	}
}

// CapturePersistentVolume will return name, namespace and capacity of PVC
func (r *Reporter) CapturePersistentVolume(f func(xfer.Request, string, string, schema.GroupKind) xfer.Response) func(xfer.Request) xfer.Response {
	return func(req xfer.Request) xfer.Response {
		uid, ok := report.ParsePersistentVolumeNodeID(req.NodeID)
		if !ok {
			return xfer.ResponseErrorf("Invalid ID: %s", req.NodeID)
		}
		// find persistentVolume by UID
		var persistentVolume PersistentVolume
		r.client.WalkPersistentVolumes(func(p PersistentVolume) error {
			if p.UID() == uid {
				persistentVolume = p
			}
			return nil
		})
		if persistentVolume == nil {
			return xfer.ResponseErrorf("Persistent volume  not found: %s", uid)
		}
		return f(req, "", persistentVolume.Name(), ResourceMap["PersistentVolume"])
	}
}

// ScaleUp is the control to scale up a deployment
func (r *Reporter) ScaleUp(req xfer.Request, namespace, id string, _ schema.GroupKind) xfer.Response {
	return xfer.ResponseError(r.client.ScaleUp(report.Deployment, namespace, id))
}

// ScaleDown is the control to scale up a deployment
func (r *Reporter) ScaleDown(req xfer.Request, namespace, id string, _ schema.GroupKind) xfer.Response {
	return xfer.ResponseError(r.client.ScaleDown(report.Deployment, namespace, id))
}

func (r *Reporter) registerControls() {
	controls := map[string]xfer.ControlHandlerFunc{
		CloneVolumeSnapshot:  r.CaptureVolumeSnapshot(r.cloneVolumeSnapshot),
		CreateVolumeSnapshot: r.CapturePersistentVolumeClaim(r.createVolumeSnapshot),
		GetLogs:              r.CapturePod(r.GetLogs),
		DescribePod:          r.CapturePod(r.describePod),
		DescribeService:      r.CaptureService(r.describe),
		DescribeCronJob:      r.CaptureCronJob(r.describe),
		DescribeDeployment:   r.CaptureDeployment(r.describe),
		DescribeDaemonSet:    r.CaptureDaemonSet(r.describe),
		DescribePVC:          r.CapturePersistentVolumeClaim(r.describePVC),
		DescribePV:           r.CapturePersistentVolume(r.describe),
		DescribeSC:           r.CaptureStorageClass(r.describe),
		DescribeStatefulSet:  r.CaptureStatefulSet(r.describe),
		DeletePod:            r.CapturePod(r.deletePod),
		DeleteVolumeSnapshot: r.CaptureVolumeSnapshot(r.deleteVolumeSnapshot),
		ScaleUp:              r.CaptureDeployment(r.ScaleUp),
		ScaleDown:            r.CaptureDeployment(r.ScaleDown),
	}
	r.handlerRegistry.Batch(nil, controls)
}

func (r *Reporter) deregisterControls() {
	controls := []string{
		CloneVolumeSnapshot,
		CreateVolumeSnapshot,
		GetLogs,
		DeletePod,
		DeleteVolumeSnapshot,
		ScaleUp,
		ScaleDown,
	}
	r.handlerRegistry.Batch(controls, nil)
}
