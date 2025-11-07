package rclone

import (
	"fmt"
	"time"

	"github.com/imroc/req/v3"
	"github.com/rs/zerolog"
)

type Client struct {
	client *req.Client
	logger zerolog.Logger
}

type Request struct {
	Command string                 `json:"command"`
	Args    map[string]interface{} `json:"args,omitempty"`
}

func NewClient(url, username, password string, logger zerolog.Logger) *Client {
	client := req.C().
		SetBaseURL(url).
		SetTimeout(60*time.Second).
		SetCommonHeader("Content-Type", "application/json").
		SetCommonRetryCount(3)

	if username != "" && password != "" {
		client.SetCommonBasicAuth(username, password)
	}
	return &Client{
		client: client,
		logger: logger,
	}
}

func (r *Client) Do(req Request, res any) error {
	response, err := r.client.R().
		SetBody(req.Args).
		SetSuccessResult(res).
		Post(req.Command)
	if err != nil {
		return err
	}
	if response.IsErrorState() {
		return response.Err
	}
	return nil
}

func (r *Client) Refresh(dirs []string, fs string) error {
	args := map[string]interface{}{
		"fs": fs,
	}
	for i, dir := range dirs {
		if dir != "" {
			if i == 0 {
				args["dir"] = dir
			} else {
				args[fmt.Sprintf("dir%d", i+1)] = dir
			}
		}
	}
	request := Request{
		Command: "vfs/forget",
		Args:    args,
	}

	err := r.Do(request, nil)
	if err != nil {
		return fmt.Errorf("failed to refresh directory %s for fs %s: %w", dirs, fs, err)
	}
	request = Request{
		Command: "vfs/refresh",
		Args:    args,
	}
	err = r.Do(request, nil)
	if err != nil {
		return fmt.Errorf("failed to refresh directory %s for fs %s: %w", dirs, fs, err)
	}
	return nil
}

func (r *Client) CheckMountHealth(fs string) error {
	request := Request{
		Command: "operations/list",
		Args: map[string]interface{}{
			"fs":     fs,
			"remote": "",
		},
	}
	err := r.Do(request, nil)
	return err
}

func (r *Client) Ping() error {
	request := Request{Command: "core/version"}
	err := r.Do(request, nil)
	return err
}

func (r *Client) Unmount(mountPoint string) error {
	request := Request{
		Command: "mount/unmount",
		Args: map[string]interface{}{
			"mountPoint": mountPoint,
		},
	}
	err := r.Do(request, nil)
	if err != nil {
		return fmt.Errorf("failed to unmount %s via RC: %w", mountPoint, err)
	}
	return nil
}

func (r *Client) Mount(mountArgs map[string]interface{}) error {
	request := Request{
		Command: "mount/mount",
		Args:    mountArgs,
	}
	err := r.Do(request, nil)
	if err != nil {
		return fmt.Errorf("failed to mount via RC: %w", err)
	}
	return nil
}

func (r *Client) CreateConfig(args map[string]interface{}) error {
	request := Request{
		Command: "config/create",
		Args:    args,
	}
	err := r.Do(request, nil)
	if err != nil {
		return fmt.Errorf("failed to create config: %w", err)
	}
	return nil
}

func (r *Client) GetCoreStats() (*CoreStatsResponse, error) {
	request := Request{
		Command: "core/stats",
	}
	var coreStats CoreStatsResponse
	err := r.Do(request, &coreStats)
	if err != nil {
		return nil, fmt.Errorf("failed to get core stats: %w", err)
	}
	return &coreStats, nil
}

// GetMemoryUsage returns memory usage statistics
func (r *Client) GetMemoryUsage() (*MemoryStats, error) {
	request := Request{
		Command: "core/memstats",
	}

	var memStats MemoryStats
	err := r.Do(request, &memStats)
	if err != nil {
		return nil, fmt.Errorf("failed to get memory stats: %w", err)
	}
	return &memStats, nil
}

// GetBandwidthStats returns bandwidth usage for all transfers
func (r *Client) GetBandwidthStats() (*BandwidthStats, error) {
	request := Request{
		Command: "core/bwlimit",
	}

	var bwStats BandwidthStats
	err := r.Do(request, &bwStats)
	if err != nil {
		return nil, fmt.Errorf("failed to get bandwidth stats: %w", err)
	}
	return &bwStats, nil
}

// GetVersion returns rclone version information
func (r *Client) GetVersion() (*VersionResponse, error) {
	request := Request{
		Command: "core/version",
	}
	var versionResp VersionResponse

	err := r.Do(request, &versionResp)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}
	return &versionResp, nil
}
