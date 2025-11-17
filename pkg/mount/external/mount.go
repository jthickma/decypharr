package external

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/rclone"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

type Mount struct {
	client *rclone.Client
	logger zerolog.Logger
}

func NewMount(mgr *manager.Manager, rcloneClient *rclone.Client) (*Mount, error) {
	m := &Mount{
		client: rcloneClient,
		logger: logger.New("external-mount"),
	}

	mgr.SetEventHandler(manager.NewEventHandlers(m))
	return m, nil
}

func (m *Mount) Start(ctx context.Context) error {
	return nil
}

func (m *Mount) Stop() error {
	return nil
}

func (m *Mount) Refresh(dirs []string) error {
	return m.client.Refresh(dirs, "")
}

func (m *Mount) Type() string {
	return "rcloneExternal"
}
