package manager

import (
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func (m *Manager) RemoveFromProvider(providerEntry *storage.ProviderEntry) error {
	if providerEntry == nil {
		return nil
	}
	if providerEntry.Provider == "usenet" {
		if m.usenet != nil {
			return m.usenet.Delete(providerEntry.ID)
		}
		return nil
	}

	client := m.ProviderClient(providerEntry.Provider)
	if client == nil {
		return nil
	}
	return client.DeleteTorrent(providerEntry.ID)
}

func (m *Manager) RemoveTorrentPlacements(t *storage.Entry) {
	for _, placement := range t.Providers {
		if IsRemoteUsenetEntry(t) {
			client := m.ProviderClient(placement.Provider)
			if client != nil && client.SupportsUsenet() {
				_ = client.DeleteNZB(placement.ID)
			}
			continue
		}
		_ = m.RemoveFromProvider(placement)
	}
}
