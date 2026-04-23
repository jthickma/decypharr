package torbox

import "time"

type UsenetSubmitResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Detail  string `json:"detail,omitempty"`
	Data    *struct {
		UsenetDownloadId int    `json:"usenetdownload_id"`
		Hash             string `json:"hash"`
		AuthId           string `json:"auth_id"`
	} `json:"data,omitempty"`
}

type UsenetInfo struct {
	Id               int       `json:"id"`
	AuthId           string    `json:"auth_id"`
	Hash             string    `json:"hash"`
	Name             string    `json:"name"`
	Size             int64     `json:"size"`
	Active           bool      `json:"active"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	DownloadState    string    `json:"download_state"`
	Progress         float64   `json:"progress"`
	DownloadSpeed    int64     `json:"download_speed"`
	Eta              int       `json:"eta"`
	ExpiresAt        any       `json:"expires_at"`
	DownloadPresent  bool      `json:"download_present"`
	DownloadFinished bool      `json:"download_finished"`
	Cached           bool      `json:"cached"`
	Files            []struct {
		Id           int    `json:"id"`
		Name         string `json:"name"`
		Size         int64  `json:"size"`
		Mimetype     string `json:"mimetype"`
		ShortName    string `json:"short_name"`
		AbsolutePath string `json:"absolute_path"`
		S3Path       string `json:"s3_path"`
	} `json:"files"`
}

type UsenetInfoResponse struct {
	Success bool        `json:"success"`
	Error   string      `json:"error,omitempty"`
	Detail  string      `json:"detail,omitempty"`
	Data    *UsenetInfo `json:"data,omitempty"`
}

type UsenetListResponse struct {
	Success bool         `json:"success"`
	Error   string       `json:"error,omitempty"`
	Detail  string       `json:"detail,omitempty"`
	Data    *[]UsenetInfo `json:"data,omitempty"`
}

type ControlUsenetResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Detail  string `json:"detail,omitempty"`
	Data    any    `json:"data,omitempty"`
}
