package types

import (
	"time"
)

type UsenetStatus string

const (
	UsenetStatusQueued      UsenetStatus = "queued"
	UsenetStatusDownloading UsenetStatus = "downloading"
	UsenetStatusCompleted   UsenetStatus = "completed"
	UsenetStatusFailed      UsenetStatus = "failed"
	UsenetStatusPaused      UsenetStatus = "paused"
)

type UsenetSubmitOpts struct {
	Password       string
	PostProcessing int
	AsQueued       bool
}

type UsenetSubmitResult struct {
	Id     string
	Hash   string
	AuthId string
}

type UsenetEntry struct {
	Id               string
	AuthId           string
	Hash             string
	Name             string
	Size             int64
	Active           bool
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Status           UsenetStatus
	Progress         float64
	DownloadSpeed    int64
	Eta              int
	ExpiresAt        any
	DownloadPresent  bool
	DownloadFinished bool
	Cached           bool
	Files            []File
}
