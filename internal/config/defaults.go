package config

var (
	DefaultPort     = "8282"
	DefaultLogLevel = "info"

	DefaultRateLimit                = "250/minute"
	DefaultTorrentsRefreshInterval  = "10m"
	DefaultDownloadsRefreshInterval = "5m"
	DefaultAutoExpireLinksAfter     = "3d"

	DefaultRclonePort = "5572"

	DefaultDFSChunkSize         = "8MB"
	DefaultDFSReadAheadSize     = "16MB"
	DefaultDFSMaxConcurrentRead = 4
	DefaultDFSCacheExpiry       = "24h"
	DefaultDFSDiskCacheSize     = "500MB"

	DefaultAccountSyncInterval = "10m"
)
