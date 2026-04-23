package common

import (
	"context"
	"fmt"

	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

var ErrUsenetNotSupported = fmt.Errorf("usenet is not supported by this provider")

type UnsupportedUsenet struct{}

func (u *UnsupportedUsenet) SupportsUsenet() bool {
	return false
}

func (u *UnsupportedUsenet) SubmitNZB(ctx context.Context, nzb []byte, name string, opts types.UsenetSubmitOpts) (*types.UsenetSubmitResult, error) {
	return nil, ErrUsenetNotSupported
}

func (u *UnsupportedUsenet) SubmitNZBLink(ctx context.Context, link, name string, opts types.UsenetSubmitOpts) (*types.UsenetSubmitResult, error) {
	return nil, ErrUsenetNotSupported
}

func (u *UnsupportedUsenet) GetNZBStatus(id string) (*types.UsenetEntry, error) {
	return nil, ErrUsenetNotSupported
}

func (u *UnsupportedUsenet) DeleteNZB(id string) error {
	return ErrUsenetNotSupported
}

func (u *UnsupportedUsenet) GetNZBDownloadLink(id string, fileID string) (types.DownloadLink, error) {
	return types.DownloadLink{}, ErrUsenetNotSupported
}

func (u *UnsupportedUsenet) GetUsenetDownloads(offset int) ([]*types.UsenetEntry, error) {
	return nil, ErrUsenetNotSupported
}
