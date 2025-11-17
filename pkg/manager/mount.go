package manager

import (
	"context"
	"strings"
)

type MountManager interface {
	Start(ctx context.Context) error
	Stop() error
	Stats() map[string]interface{}
	IsReady() bool
	Type() string
}

type Mount interface {
	Start(ctx context.Context) error
	Stop() error
	Refresh(dirs []string) error
	Type() string
}

type EventHandler struct {
	Refresh func(dirs []string) error
}

func (m *Manager) SetEventHandler(e *EventHandler) {
	m.event = e
}

func (m *Manager) RefreshEntries(refreshMount bool) {
	// Refresh entries
	m.entry.Refresh()

	// Refresh mount if needed
	if refreshMount {
		go func() {
			_ = m.RefreshMount()
		}()
	}
}

func (m *Manager) RefreshMount() error {
	dirs := strings.FieldsFunc(m.config.RefreshDirs, func(r rune) bool {
		return r == ',' || r == '&'
	})
	if len(dirs) == 0 {
		dirs = []string{"__all__"}
	}

	// Call event handler if set
	if m.event != nil && m.event.Refresh != nil {
		return m.event.Refresh(dirs)
	}
	return nil
}

func NewEventHandlers(mounter Mount) *EventHandler {
	return &EventHandler{
		Refresh: mounter.Refresh,
	}
}

type stubMountManager struct{}

func NewStubMountManager() MountManager {
	return &stubMountManager{}
}

func (s *stubMountManager) Start(ctx context.Context) error {
	return nil
}
func (s *stubMountManager) Stop() error {
	return nil
}
func (s *stubMountManager) Stats() map[string]interface{} {
	return map[string]interface{}{
		"message": "no mount configured",
	}
}
func (s *stubMountManager) IsReady() bool {
	return false
}
func (s *stubMountManager) Type() string {
	return "none"
}
