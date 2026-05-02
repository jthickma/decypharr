package manager

import (
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

const LocalUsenetProvider = "usenet"

func RemoteUsenetInfoHash(id string) string {
	return "torbox-usenet-" + id
}

func IsRemoteUsenetEntry(entry *storage.Entry) bool {
	return entry != nil &&
		entry.Protocol == config.ProtocolNZB &&
		entry.ActiveProvider != "" &&
		entry.ActiveProvider != LocalUsenetProvider
}

func NewRemoteUsenetEntry(
	client debrid.Client,
	result *debridTypes.UsenetSubmitResult,
	name string,
	downloadFolder string,
	arrCfg *arr.Arr,
	action config.DownloadAction,
	callbackURL string,
	skipMultiSeason bool,
) *storage.Entry {
	now := time.Now()
	providerName := client.Config().Name
	remoteID := result.Id
	entry := &storage.Entry{
		InfoHash:         RemoteUsenetInfoHash(remoteID),
		Name:             name,
		OriginalFilename: name,
		Protocol:         config.ProtocolNZB,
		Size:             0,
		Bytes:            0,
		Category:         arrCfg.Name,
		SavePath:         filepath.Join(downloadFolder, arrCfg.Name),
		Status:           debridTypes.TorrentStatusDownloading,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		Action:           action,
		CallbackURL:      callbackURL,
		SkipMultiSeason:  skipMultiSeason,
		CreatedAt:        now,
		UpdatedAt:        now,
		AddedOn:          now,
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{"torbox_usenet"},
		ActiveProvider:   providerName,
	}
	entry.ContentPath = entry.DownloadPath()
	entry.Providers[providerName] = &storage.ProviderEntry{
		Provider: providerName,
		ID:       remoteID,
		AddedAt:  now,
		Status:   debridTypes.TorrentStatusDownloading,
		Progress: 0,
		Files:    make(map[string]*storage.ProviderFile),
	}
	return entry
}
