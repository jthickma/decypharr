package arr

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/imroc/req/v3"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/httpclient"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// Type is a type of arr
type Type string

var sharedClient = httpclient.DefaultClient()

const (
	Sonarr  Type = "sonarr"
	Radarr  Type = "radarr"
	Lidarr  Type = "lidarr"
	Readarr Type = "readarr"
	Others  Type = "others"
)

type Arr struct {
	Name  string `json:"name"`
	Host  string `json:"host"`
	Token string `json:"token"`

	Type             Type   `json:"type"`
	Cleanup          bool   `json:"cleanup"`
	SkipRepair       bool   `json:"skip_repair"`
	DownloadUncached *bool  `json:"download_uncached"`
	SelectedDebrid   string `json:"selected_debrid,omitempty"` // The debrid service selected for this arr
	Source           string `json:"source,omitempty"`          // The source of the arr, e.g. "auto", "manual". Auto means it was automatically detected from the arr
}

func New(name, host, token string, cleanup, skipRepair bool, downloadUncached *bool, selectedDebrid, source string) *Arr {
	return &Arr{
		Name:             name,
		Host:             host,
		Token:            strings.TrimSpace(token),
		Type:             inferType(host, name),
		Cleanup:          cleanup,
		SkipRepair:       skipRepair,
		DownloadUncached: downloadUncached,
		SelectedDebrid:   selectedDebrid,
		Source:           source,
	}
}

func (a *Arr) Request(method, endpoint string, payload interface{}, res any) (*req.Response, error) {
	if a.Token == "" || a.Host == "" {
		return nil, fmt.Errorf("arr not configured")
	}
	url, err := utils.JoinURL(a.Host, endpoint)
	if err != nil {
		return nil, err
	}

	switch method {
	case http.MethodGet:
		return sharedClient.R().
			SetRetryCount(5).
			SetBody(payload).
			SetHeader("Content-Type", "application/json").
			SetHeader("X-Api-Key", a.Token).
			SetRetryBackoffInterval(100*time.Millisecond, 2*time.Second).
			SetSuccessResult(res).
			Get(url)
	case http.MethodPost:
		return sharedClient.R().
			SetRetryCount(5).
			SetBody(payload).
			SetHeader("Content-Type", "application/json").
			SetHeader("X-Api-Key", a.Token).
			SetRetryBackoffInterval(100*time.Millisecond, 2*time.Second).
			SetSuccessResult(res).
			Post(url)
	case http.MethodPut:
		return sharedClient.R().
			SetRetryCount(5).
			SetBody(payload).
			SetHeader("Content-Type", "application/json").
			SetHeader("X-Api-Key", a.Token).
			SetRetryBackoffInterval(100*time.Millisecond, 2*time.Second).
			SetSuccessResult(res).
			Put(url)
	case http.MethodDelete:
		return sharedClient.R().
			SetRetryCount(5).
			SetBody(payload).
			SetHeader("Content-Type", "application/json").
			SetHeader("X-Api-Key", a.Token).
			SetRetryBackoffInterval(100*time.Millisecond, 2*time.Second).
			SetSuccessResult(res).
			Delete(url)
	default:
		return nil, fmt.Errorf("unsupported method: %s", method)
	}
}

func (a *Arr) Validate() error {
	if a.Token == "" || a.Host == "" {
		return fmt.Errorf("arr not configured")
	}

	if utils.ValidateURL(a.Host) != nil {
		return fmt.Errorf("invalid arr host URL")
	}
	resp, err := a.Request("GET", "/api/v3/health", nil, nil)
	if err != nil {
		return err
	}
	// If response is not 200 or 404(this is the case for Lidarr, etc), return an error
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("failed to validate arr %s: %s", a.Name, resp.Status)
	}
	return nil
}

type Storage struct {
	Arrs   map[string]*Arr // name -> arr
	mu     sync.Mutex
	logger zerolog.Logger
}

func (s *Storage) Cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Arrs = make(map[string]*Arr)
}

func NewStorage() *Storage {
	arrs := make(map[string]*Arr)
	for _, a := range config.Get().Arrs {
		if a.Host == "" || a.Token == "" || a.Name == "" {
			continue // Skip if host or token is not set
		}
		name := a.Name
		as := New(name, a.Host, a.Token, a.Cleanup, a.SkipRepair, a.DownloadUncached, a.SelectedDebrid, a.Source)
		if utils.ValidateURL(as.Host) != nil {
			continue
		}
		arrs[a.Name] = as
	}
	return &Storage{
		Arrs:   arrs,
		logger: logger.New("arr"),
	}
}

func (s *Storage) AddOrUpdate(arr *Arr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if arr.Host == "" || arr.Token == "" || arr.Name == "" {
		return
	}

	// Check the host URL
	if utils.ValidateURL(arr.Host) != nil {
		return
	}
	s.Arrs[arr.Name] = arr
}

func (s *Storage) GetOrCreate(name string) *Arr {
	if name == "" {
		name = "uncategorized"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	arr, exists := s.Arrs[name]
	if !exists {
		return New(name, "", "", false, false, nil, "", "manual")
	}
	return arr
}

func (s *Storage) Get(name string) *Arr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Arrs[name]
}

func (s *Storage) GetAll() []*Arr {
	s.mu.Lock()
	defer s.mu.Unlock()
	arrs := make([]*Arr, 0, len(s.Arrs))
	for _, arr := range s.Arrs {
		arrs = append(arrs, arr)
	}
	return arrs
}

func (s *Storage) SyncToConfig() []config.Arr {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := config.Get()
	arrConfigs := make(map[string]config.Arr)
	for _, a := range cfg.Arrs {
		if a.Host == "" || a.Token == "" {
			continue // Skip empty arrs
		}
		arrConfigs[a.Name] = a
	}

	for name, arr := range s.Arrs {
		exists, ok := arrConfigs[name]
		if ok {
			// Update existing arr config
			// Check if the host URL is valid
			if utils.ValidateURL(arr.Host) == nil {
				exists.Host = arr.Host
			}
			exists.Token = cmp.Or(exists.Token, arr.Token)
			exists.Cleanup = arr.Cleanup
			exists.SkipRepair = arr.SkipRepair
			exists.DownloadUncached = arr.DownloadUncached
			exists.SelectedDebrid = arr.SelectedDebrid
			arrConfigs[name] = exists
		} else {
			// AddOrUpdate new arr config
			arrConfigs[name] = config.Arr{
				Name:             arr.Name,
				Host:             arr.Host,
				Token:            arr.Token,
				Cleanup:          arr.Cleanup,
				SkipRepair:       arr.SkipRepair,
				DownloadUncached: arr.DownloadUncached,
				SelectedDebrid:   arr.SelectedDebrid,
				Source:           arr.Source,
			}
		}
	}
	// Convert map to slice
	arrs := make([]config.Arr, 0, len(arrConfigs))
	for _, a := range arrConfigs {
		arrs = append(arrs, a)
	}
	return arrs
}

func (s *Storage) SyncFromConfig(arrs []config.Arr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arrConfigs := make(map[string]*Arr)
	for _, a := range arrs {
		arrConfigs[a.Name] = New(a.Name, a.Host, a.Token, a.Cleanup, a.SkipRepair, a.DownloadUncached, a.SelectedDebrid, a.Source)
	}

	// AddOrUpdate or update arrs from config
	for name, arr := range s.Arrs {
		if ac, ok := arrConfigs[name]; ok {
			// Update existing arr
			// is the host URL valid?
			if utils.ValidateURL(ac.Host) == nil {
				ac.Host = arr.Host
			}
			ac.Token = cmp.Or(ac.Token, arr.Token)
			ac.Cleanup = arr.Cleanup
			ac.SkipRepair = arr.SkipRepair
			ac.DownloadUncached = arr.DownloadUncached
			ac.SelectedDebrid = arr.SelectedDebrid
			ac.Source = arr.Source
			arrConfigs[name] = ac
		} else {
			arrConfigs[name] = arr
		}
	}

	// Replace the arrs map
	s.Arrs = arrConfigs

}

func (s *Storage) StartWorker(ctx context.Context) error {

	ticker := time.NewTicker(10 * time.Second)

	select {
	case <-ticker.C:
		s.cleanupArrsQueue()
	case <-ctx.Done():
		ticker.Stop()
		return nil
	}
	return nil
}

func (s *Storage) cleanupArrsQueue() {
	arrs := make([]*Arr, 0)
	for _, arr := range s.Arrs {
		if !arr.Cleanup {
			continue
		}
		arrs = append(arrs, arr)
	}
	if len(arrs) > 0 {
		for _, arr := range arrs {
			if err := arr.CleanupQueue(); err != nil {
				s.logger.Error().Err(err).Msgf("Failed to cleanup arr %s", arr.Name)
			}
		}
	}
}

func (a *Arr) Refresh() {
	payload := struct {
		Name string `json:"name"`
	}{
		Name: "RefreshMonitoredDownloads",
	}

	_, _ = a.Request(http.MethodPost, "api/v3/command", payload, nil)
}

func inferType(host, name string) Type {
	switch {
	case strings.Contains(host, "sonarr") || strings.Contains(name, "sonarr"):
		return Sonarr
	case strings.Contains(host, "radarr") || strings.Contains(name, "radarr"):
		return Radarr
	case strings.Contains(host, "lidarr") || strings.Contains(name, "lidarr"):
		return Lidarr
	case strings.Contains(host, "readarr") || strings.Contains(name, "readarr"):
		return Readarr
	default:
		return Others
	}
}
