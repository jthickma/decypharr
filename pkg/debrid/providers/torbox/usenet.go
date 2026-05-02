package torbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"time"

	json "github.com/bytedance/sonic"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func (tb *Torbox) SupportsUsenet() bool {
	return true
}

func (tb *Torbox) mapUsenetState(state string) types.UsenetStatus {
	switch state {
	case "queued", "pausedUP", "queuedUP", "queuedDL":
		return types.UsenetStatusQueued
	case "downloading", "allocating", "checkingDL", "forcedDL", "checkingResumeData", "moving":
		return types.UsenetStatusDownloading
	case "completed", "cached", "downloaded", "download_finished":
		return types.UsenetStatusCompleted
	case "failed", "error":
		return types.UsenetStatusFailed
	case "paused", "pausedDL":
		return types.UsenetStatusPaused
	default:
		// Default to downloading if state is unknown but active
		return types.UsenetStatusDownloading
	}
}

func (tb *Torbox) submitUsenet(ctx context.Context, endpoint string, body io.Reader, contentType string) (*types.UsenetSubmitResult, error) {
	if tb.usenetCreateLimiter != nil {
		tb.usenetCreateLimiter.Take()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tb.Host+endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := tb.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	var data UsenetSubmitResponse
	if err := json.ConfigDefault.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	if !data.Success || data.Data == nil {
		if data.Detail != "" {
			return nil, fmt.Errorf("torbox API error: %s (%s)", data.Error, data.Detail)
		}
		return nil, fmt.Errorf("error adding usenet download: %s", data.Error)
	}

	return &types.UsenetSubmitResult{
		Id:     strconv.Itoa(data.Data.UsenetDownloadId),
		Hash:   data.Data.Hash,
		AuthId: data.Data.AuthId,
	}, nil
}

func (tb *Torbox) SubmitNZB(ctx context.Context, nzb []byte, name string, opts types.UsenetSubmitOpts) (*types.UsenetSubmitResult, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	part, err := writer.CreateFormFile("file", name)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(part, bytes.NewReader(nzb)); err != nil {
		return nil, err
	}

	if opts.Password != "" {
		_ = writer.WriteField("password", opts.Password)
	}
	_ = writer.WriteField("post_processing", strconv.Itoa(opts.PostProcessing))

	if opts.AsQueued {
		_ = writer.WriteField("as_queued", "true")
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return tb.submitUsenet(ctx, "/api/usenet/createusenetdownload", &body, writer.FormDataContentType())
}

func (tb *Torbox) SubmitNZBLink(ctx context.Context, link, name string, opts types.UsenetSubmitOpts) (*types.UsenetSubmitResult, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	_ = writer.WriteField("link", link)
	if name != "" {
		_ = writer.WriteField("name", name)
	}

	if opts.Password != "" {
		_ = writer.WriteField("password", opts.Password)
	}
	_ = writer.WriteField("post_processing", strconv.Itoa(opts.PostProcessing))

	if opts.AsQueued {
		_ = writer.WriteField("as_queued", "true")
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return tb.submitUsenet(ctx, "/api/usenet/createusenetdownload", &body, writer.FormDataContentType())
}

func (tb *Torbox) convertToUsenetEntry(data *UsenetInfo) *types.UsenetEntry {
	files := make([]types.File, 0, len(data.Files))
	for _, f := range data.Files {
		files = append(files, types.File{
			Id:   strconv.Itoa(f.Id),
			Name: filepath.Base(f.Name),
			Size: f.Size,
			Path: f.Name,
		})
	}

	return &types.UsenetEntry{
		Id:               strconv.Itoa(data.Id),
		AuthId:           data.AuthId,
		Hash:             data.Hash,
		Name:             data.Name,
		Size:             data.Size,
		Active:           data.Active,
		CreatedAt:        data.CreatedAt,
		UpdatedAt:        data.UpdatedAt,
		Status:           tb.mapUsenetState(data.DownloadState),
		Progress:         data.Progress * 100,
		DownloadSpeed:    data.DownloadSpeed,
		Eta:              data.Eta,
		ExpiresAt:        data.ExpiresAt,
		DownloadPresent:  data.DownloadPresent,
		DownloadFinished: data.DownloadFinished,
		Cached:           data.Cached,
		Files:            files,
	}
}

func (tb *Torbox) GetNZBStatus(id string) (*types.UsenetEntry, error) {
	var res UsenetInfoResponse

	resp, err := tb.doGet("/api/usenet/mylist", map[string]string{
		"id":           id,
		"bypass_cache": "true",
	}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	if !res.Success || res.Data == nil {
		return nil, fmt.Errorf("torbox API error: %v", res.Error)
	}

	return tb.convertToUsenetEntry(res.Data), nil
}

func (tb *Torbox) DeleteNZB(id string) error {
	payload := map[string]any{
		"usenet_id": id,
		"operation": "delete",
		"all":       false,
	}

	resp, err := tb.doPostJSON("/api/usenet/controlusenetdownload", payload, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	tb.logger.Info().Msgf("Usenet download %s deleted from Torbox", id)
	return nil
}

func (tb *Torbox) GetNZBDownloadLink(id string, fileID string) (types.DownloadLink, error) {
	query := url.Values{}
	query.Set("token", tb.APIKey)
	query.Set("usenet_id", id)
	query.Set("file_id", fileID)
	query.Set("redirect", "true")

	downloadURL := fmt.Sprintf("%s/api/usenet/requestdl?%s", tb.Host, query.Encode())

	now := time.Now()

	dl := types.DownloadLink{
		Token:        tb.APIKey,
		DownloadLink: downloadURL,
		Debrid:       tb.config.Name,
		Id:           fileID,
		Generated:    now,
		ExpiresAt:    now.Add(tb.autoExpiresLinksAfter),
	}
	return dl, nil
}

func (tb *Torbox) GetUsenetDownloads(offset int) ([]*types.UsenetEntry, error) {
	var res UsenetListResponse

	resp, err := tb.doGet("/api/usenet/mylist", map[string]string{
		"offset":       strconv.Itoa(offset),
		"bypass_cache": "true",
	}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	if !res.Success || res.Data == nil {
		return nil, fmt.Errorf("torbox API error: %v", res.Error)
	}

	entries := make([]*types.UsenetEntry, 0, len(*res.Data))
	for _, data := range *res.Data {
		// we must pass address of data to convertToUsenetEntry, or it might get copied incorrectly,
		// but since it's a loop variable we should copy it first.
		d := data
		entries = append(entries, tb.convertToUsenetEntry(&d))
	}

	return entries, nil
}
