package manager

import (
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func (m *Manager) RemoveFromDebrid(placement *storage.Placement) error {
	client := m.DebridClient(placement.Debrid)
	if client == nil {
		return nil
	}
	return client.DeleteTorrent(placement.ID)
}

func (m *Manager) RemoveTorrentPlacements(t *storage.Torrent) {
	for _, placement := range t.Placements {
		_ = m.RemoveFromDebrid(placement)
	}
}
