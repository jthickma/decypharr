package account

import (
	"sync/atomic"

	"github.com/imroc/req/v3"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

type Account struct {
	Debrid      string                                 `json:"debrid"` // The debrid service name, e.g. "realdebrid"
	links       *xsync.Map[string, types.DownloadLink] // key is the sliced file link
	Index       int                                    `json:"index"` // The index of the account in the config
	Disabled    atomic.Bool                            `json:"disabled"`
	Token       string                                 `json:"token"`
	TrafficUsed atomic.Int64                           `json:"traffic_used"` // Traffic used in bytes
	Username    string                                 `json:"username"`     // Username for the account
	httpClient  *req.Client

	// Account reactivation tracking
	DisableCount atomic.Int32 `json:"disable_count"`
}

func (a *Account) Equals(other *Account) bool {
	if other == nil {
		return false
	}
	return a.Token == other.Token && a.Debrid == other.Debrid
}

func (a *Account) Client() *req.Client {
	return a.httpClient
}

// slice download link
func (a *Account) sliceFileLink(fileLink string) string {
	if a.Debrid != "realdebrid" {
		return fileLink
	}
	if len(fileLink) < 39 {
		return fileLink
	}
	return fileLink[0:39]
}

func (a *Account) GetDownloadLink(fileLink string) (types.DownloadLink, error) {
	slicedLink := a.sliceFileLink(fileLink)
	dl, ok := a.links.Load(slicedLink)
	if !ok {
		return types.DownloadLink{}, types.ErrDownloadLinkNotFound
	}
	if err := dl.Valid(); err != nil {
		return types.DownloadLink{}, err
	}
	return dl, nil
}

func (a *Account) storeLink(dl types.DownloadLink) {
	slicedLink := a.sliceFileLink(dl.Link)
	a.links.Store(slicedLink, dl)
}
func (a *Account) DeleteDownloadLink(fileLink string) {
	slicedLink := a.sliceFileLink(fileLink)
	a.links.Delete(slicedLink)
}
func (a *Account) ClearDownloadLinks() {
	a.links.Clear()
}
func (a *Account) DownloadLinksCount() int {
	return a.links.Size()
}

func (a *Account) StoreDownloadLinks(dls map[string]*types.DownloadLink) {
	for _, dl := range dls {
		a.storeLink(*dl)
	}
}

// MarkDisabled marks the account as disabled and increments the disable count
func (a *Account) MarkDisabled() {
	a.Disabled.Store(true)
	a.DisableCount.Add(1)
}

func (a *Account) Reset() {
	a.DisableCount.Store(0)
	a.Disabled.Store(false)
}
