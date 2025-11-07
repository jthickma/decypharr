package external

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/rclone"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

type Manager struct {
	mu      sync.RWMutex
	mounts  map[string]*Mount
	manager *manager.Manager
	client  *rclone.Client
	logger  zerolog.Logger
}

// NewManager creates a new external rclone manager
// This does nothing, just a placeholder to satisfy the interface
func NewManager(manager *manager.Manager) *Manager {
	_logger := logger.New("external")
	cfg := config.Get()
	rcloneClient := rclone.NewClient(
		cfg.Mount.ExternalRclone.RCUrl,
		cfg.Mount.ExternalRclone.RCUsername,
		cfg.Mount.ExternalRclone.RCPassword,
		_logger,
	)
	m := &Manager{
		manager: manager,
		logger:  _logger,
		client:  rcloneClient,
	}
	m.registerMounts()
	return m
}

func (m *Manager) registerMounts() {
	mounts := make(map[string]*Mount)
	for mountName := range m.manager.MountPaths() {
		mnt, err := NewMount(mountName, m.manager)
		if err != nil {
			m.logger.Error().Err(err).Msgf("Failed to create FUSE mount for debrid: %s", mountName)
			continue
		}
		mounts[mountName] = mnt
	}
	m.mu.Lock()
	m.mounts = mounts
	m.mu.Unlock()
}

func (m *Manager) Start(ctx context.Context) error {
	return nil
}

func (m *Manager) Stop() error {
	return nil
}

func (m *Manager) IsReady() bool {
	return true
}

func (m *Manager) Type() string {
	return "external"
}
