package kubernetes

import (
	volumesnapshotv1 "github.com/openebs/external-storage/snapshot/pkg/apis/volumesnapshot/v1"
	"github.com/weaveworks/scope/report"
)

// VolumeSnapshot represent kubernetes VolumeSnapshot interface
type VolumeSnapshot interface {
	Meta
	GetNode(probeID string) report.Node
}

// volumeSnapshot represents kubernetes volume snapshots
type volumeSnapshot struct {
	*volumesnapshotv1.VolumeSnapshot
	Meta
}

// NewVolumeSnapshot returns new Volume Snapshot type
func NewVolumeSnapshot(p *volumesnapshotv1.VolumeSnapshot) VolumeSnapshot {
	return &volumeSnapshot{VolumeSnapshot: p, Meta: meta{p.ObjectMeta}}
}

// GetNode returns VolumeSnapshot as Node
func (p *volumeSnapshot) GetNode(probeID string) report.Node {
	return p.MetaNode(report.MakeVolumeSnapshotNodeID(p.UID())).WithLatests(map[string]string{
		report.ControlProbeID: probeID,
		NodeType:              "Volume Snapshot",
		Name:                  p.GetName(),
	}).WithLatestActiveControls(DeleteVolumeSnapshot)
}
