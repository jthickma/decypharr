# Decypharr Fork: TorBox Usenet Support

Add NZB submission to TorBox Pro's Usenet API as an alternative to Decypharr's direct-NNTP backend. Closes upstream issue [#7](https://github.com/sirrobot01/decypharr/issues/7).

## Goal

Let users submit NZB files and URLs from Sonarr/Radarr/Lidarr/Readarr/Whisparr into TorBox's Usenet queue through Decypharr's existing SABnzbd-compatible server, so Decypharr can be the single download client for both torrents (via TorBox API) and NZBs (via TorBox API) — no separate NNTP provider required.

## Non-goals

- Don't touch `pkg/usenet/` (the direct NNTP engine). That path stays as-is for users with real Usenet providers.
- Don't change the mount/WebDAV layer. TorBox-Usenet downloads surface through TorBox's existing WebDAV mount the same way torrents do — just under a different subdirectory.
- No new UI beyond a config field and a small status badge. Reuse the existing queue/history views.

## Architecture decision: Option A (provider-level routing)

Decypharr today has two parallel pipelines:

- **Torrents**: SABnzbd/qBit handler → `pkg/debrid/common.Client` → TorBox/RD/AD/DL → symlinks into TorBox WebDAV mount
- **NZBs**: SABnzbd handler → `pkg/usenet/` → direct NNTP → yEnc decode → segment stream

Add a third path that reuses (1)'s infrastructure for NZBs:

- **NZBs via TorBox**: SABnzbd handler → `pkg/debrid/common.Client` (new NZB methods) → TorBox Usenet API → symlinks into TorBox WebDAV mount

Routing decision lives in the SABnzbd handler. A per-debrid-provider config flag (`usenet_backend: "torbox" | "nntp"`) selects the path at request time.

This keeps the NNTP engine untouched, reuses the battle-tested torrent worker/poller/symlink logic, and lets users mix-and-match (e.g., use TorBox for torrents AND Usenet).

---

## TorBox Usenet API surface

All endpoints live under `https://api.torbox.app/v1`. Auth is `Authorization: Bearer <APIKEY>` — same as the existing torrent path.

### Endpoints we need

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/usenet/createusenetdownload` | Submit NZB (file upload OR URL) |
| GET  | `/api/usenet/mylist` | List all Usenet downloads (optionally `?id=<n>` for one) |
| GET  | `/api/usenet/requestdl` | Get a signed download link for a finished file |
| POST | `/api/usenet/controlusenetdownload` | Pause/resume/delete |
| GET  | `/api/usenet/getqueued` | List items still in queue state |
| POST | `/api/usenet/controlqueuedusenet` | Control queued items |

### `createusenetdownload` form fields

Multipart/form-data POST. Send either `file` OR `link`, not both:

- `file` *(multipart file)* — raw NZB bytes
- `link` *(string)* — direct URL to an NZB
- `name` *(string, optional)* — custom display name
- `password` *(string, optional)* — RAR password applied post-extract
- `post_processing` *(int, optional)* — `-1` default (repair+unpack+cleanup), `0` none, `1` repair only, `2` repair+unpack, `3` repair+unpack+delete-sources
- `as_queued` *(bool, optional)* — force into the queue even if slots available

**Success response** (pattern matches torrent side):
```json
{
  "success": true,
  "detail": "...",
  "data": {
    "usenetdownload_id": 12345,
    "hash": "md5-of-nzb-content",
    "auth_id": "..."
  }
}
```

### `mylist` response — `UsenetInfo`

Parallel to `torboxInfo` but with different field names. Key fields:

- `id`, `auth_id`, `hash`, `name`, `size`, `created_at`, `updated_at`
- `download_state` — string (`"queued"`, `"downloading"`, `"completed"`, `"failed"`, etc.)
- `progress`, `download_speed`, `eta`
- `active`, `cached`, `download_present`, `download_finished`
- `files[]` — each with `id`, `name`, `size`, `mimetype`, `short_name`, `absolute_path`, `s3_path`

### Rate limits

- Create endpoints (`createusenetdownload`, `createtorrent`, `createwebdownload`) are **60/hour with a 10/min edge**
- All other `/usenet/*` endpoints default to 300/min per API key
- Rate limits are synchronized across all TorBox servers per API key
- Existing code sets up `ratelimit.Limiter` map — add a `"usenet_create"` key with tighter limits

---

## File-by-file changes

### 1. `pkg/debrid/providers/torbox/usenet.go` *(new)*

New file, ~400 LOC. Mirrors the structure of `torbox.go`'s torrent methods. Each method is a near-duplicate with endpoint paths swapped and response field names adjusted.

Methods:

```go
func (tb *Torbox) SubmitNZB(ctx context.Context, nzb []byte, name string, opts SubmitOpts) (*UsenetSubmitResult, error)
func (tb *Torbox) SubmitNZBLink(ctx context.Context, link, name string, opts SubmitOpts) (*UsenetSubmitResult, error)
func (tb *Torbox) GetUsenetDownload(id int) (*UsenetInfo, error)
func (tb *Torbox) GetUsenetDownloads(offset int) ([]UsenetInfo, error)
func (tb *Torbox) RequestUsenetDL(downloadID, fileID int) (types.DownloadLink, error)
func (tb *Torbox) ControlUsenetDownload(id int, op string) error // "pause"|"resume"|"delete"|"reannounce"
func (tb *Torbox) DeleteUsenetDownload(id int) error
```

Shape `SubmitOpts`:
```go
type SubmitOpts struct {
    Password       string
    PostProcessing int  // -1 default, 0 none, 1 repair, 2 repair+unpack, 3 repair+unpack+delete
    AsQueued       bool
}
```

### 2. `pkg/debrid/providers/torbox/usenet_types.go` *(new)*

```go
type UsenetSubmitResponse APIResponse[struct {
    Id   int    `json:"usenetdownload_id"`
    Hash string `json:"hash"`
    AuthId string `json:"auth_id"`
}]

type usenetInfo struct {
    Id                int       `json:"id"`
    AuthId            string    `json:"auth_id"`
    Hash              string    `json:"hash"`
    Name              string    `json:"name"`
    Size              int64     `json:"size"`
    Active            bool      `json:"active"`
    CreatedAt         time.Time `json:"created_at"`
    UpdatedAt         time.Time `json:"updated_at"`
    DownloadState     string    `json:"download_state"`
    Progress          float64   `json:"progress"`
    DownloadSpeed     int64     `json:"download_speed"`
    Eta               int       `json:"eta"`
    ExpiresAt         any       `json:"expires_at"`
    DownloadPresent   bool      `json:"download_present"`
    DownloadFinished  bool      `json:"download_finished"`
    Cached            bool      `json:"cached"`
    Files             []struct {
        Id           int    `json:"id"`
        Name         string `json:"name"`
        Size         int64  `json:"size"`
        Mimetype     string `json:"mimetype"`
        ShortName    string `json:"short_name"`
        AbsolutePath string `json:"absolute_path"`
        S3Path       string `json:"s3_path"`
    } `json:"files"`
}

type UsenetInfoResponse APIResponse[usenetInfo]
type UsenetListResponse APIResponse[[]usenetInfo]
```

### 3. `pkg/debrid/common/interface.go` *(modify)*

Extend the `Client` interface — but gate via a capability method so non-supporting providers don't need stubs:

```go
type Client interface {
    // ... existing 20 methods ...

    // Usenet capability - providers that don't support return false + no-op stubs
    SupportsUsenet() bool
    SubmitNZB(ctx context.Context, nzb []byte, name string, opts UsenetOpts) (UsenetSubmitResult, error)
    GetNZBStatus(id string) (*types.UsenetEntry, error)
    DeleteNZB(id string) error
    GetNZBDownloadLink(id string, fileID string) (types.DownloadLink, error)
}
```

Add corresponding no-op stubs to `realdebrid`, `alldebrid`, `debridlink`, and `torbox.go` (the torrent-only surface). Their `SupportsUsenet()` returns `false`.

### 4. `pkg/debrid/types/` *(extend)*

- `usenet.go` *(new)* — `UsenetEntry`, `UsenetSubmitResult`, `UsenetStatus` enum
- Map TorBox states to shared states: `queued`, `downloading`, `completed`, `failed`, `paused`

### 5. `internal/config/config.go` *(modify)*

Add to the `Debrid` struct:

```go
type Debrid struct {
    // ... existing fields ...
    UsenetBackend    string `json:"usenet_backend,omitempty"`     // "torbox" | "nntp" | "" (disabled)
    UsenetPostProcess int   `json:"usenet_post_process,omitempty"` // default -1
}
```

Validation: if `UsenetBackend == "torbox"`, enforce provider name is `"torbox"`.

### 6. `pkg/server/sabnzbd/handlers.go` *(modify)*

Only two handlers change. Both currently route to `pkg/usenet/`; add branching.

**`handleAddFile`** (around line 347-404):
```go
// Pseudocode - real impl uses the manager/store pattern
debrid := s.manager.GetDebridForUsenet()  // returns first client where SupportsUsenet() && config.UsenetBackend == "torbox"
if debrid != nil {
    result, err := debrid.SubmitNZB(ctx, content, fileHeader.Filename, opts)
    if err != nil { /* error response */ }
    nzoID := fmt.Sprintf("torbox-usenet-%d", result.Id)
    // insert into queue storage as Protocol=NZB entry tagged with backend=torbox
    // return SAB-compatible response
    return
}
// fall through to existing NNTP path
nzbID, err := s.addNZBFile(ctx, content, fileHeader.Filename, _arr, action)
```

**`handleAddURL`** (around line 273-334): same branching, calling `SubmitNZBLink` instead.

### 7. `pkg/manager/` *(modify)*

The manager currently tracks torrent lifecycle. Add a usenet-job worker mirroring the torrent worker:

- On submit: store entry with `Protocol=NZB`, `Backend=torbox`, `State=queued`
- Background poller: every ~10s, call `GetUsenetDownloads` for active jobs, update progress/state
- On `download_finished`: call `RequestUsenetDL` for each wanted file, create symlinks from TorBox WebDAV mount into arr category folder
- On completion: update SAB `history` response so arr marks as imported

Reuse `pkg/debrid/storage.go` patterns — usenet entries are just a protocol-tagged variant of debrid entries.

### 8. `pkg/server/sabnzbd/handlers.go` — history/queue serialization

`convertToSABnzbdNZB` needs to handle the new backend:
- Map TorBox `download_state` strings to SAB states (`Downloading`, `Queued`, `Completed`, `Failed`)
- Surface TorBox ETA/speed/progress
- Keep NZO ID format stable across the two backends (prefix disambiguation: `torbox-usenet-<id>` vs existing NNTP IDs)

### 9. Symlink/filesystem

TorBox exposes Usenet downloads through the same WebDAV mount as torrents, under a separate path (typically `/usenet/<name>/` vs `/torrents/<name>/`). Symlink resolver needs to:

1. Know which subpath to check based on entry protocol
2. Wait for `download_present: true` before creating symlinks (matches torrent behavior)
3. Handle TorBox's PAR2/RAR post-processing — wait for `download_finished: true` to avoid catching intermediate files

If the WebDAV path for Usenet differs from the torrent layout, update `pkg/debrid/common/` path helpers.

### 10. Rate limit wiring

In `pkg/debrid/providers/torbox/torbox.go` `New()`:
```go
// existing
"main":     ratelimit.New(300, ratelimit.Per(time.Minute)),
"download": ratelimit.New(...),
// add
"usenet_create": ratelimit.New(10, ratelimit.Per(time.Minute)),  // 60/hr with 10/min edge
```

The `SubmitNZB` / `SubmitNZBLink` methods use `ratelimits["usenet_create"]`. Other Usenet reads use `ratelimits["main"]`.

### 11. Logging & observability

- Add structured log fields: `backend=torbox_usenet`, `usenet_id`
- Surface TorBox's `detail` field on errors (the API returns user-friendly messages there, safe to forward)
- Update Beszel/Prometheus exporters if present to include usenet job counts

### 12. Documentation

- `docs/src/content/docs/guides/usenet/torbox.md` *(new)* — setup guide
- Update `docs/src/content/docs/guides/usenet/` index page
- README: add TorBox-Usenet to the Usenet bullet in feature list
- Config reference page — document `usenet_backend`, `usenet_post_process`

---

## Testing

### Unit tests

- `pkg/debrid/providers/torbox/usenet_test.go` — mock HTTP server fixtures for each method
- State mapping tests: every TorBox `download_state` string → shared state enum
- Rate limiter: verify `usenet_create` limiter is consumed and doesn't share budget with `main`

### Integration tests (behind build tag, requires real API key)

- Submit a small public-domain NZB (e.g., Linux ISO from a test indexer)
- Poll to completion
- Fetch download link
- Delete download

Use `//go:build integration` and a `TORBOX_API_KEY` env guard.

### Manual smoke test path

1. Build: `go build ./cmd/decypharr`
2. Bind-mount binary into existing container: `-v $(pwd)/decypharr:/usr/bin/decypharr`
3. Config: set `usenet_backend: torbox` on the TorBox provider
4. Prowlarr → add NZBGeek → sync to Sonarr
5. Sonarr → Search → pick a small file → verify it shows in TorBox dashboard under Usenet, then appears in arr queue, then imports

---

## Rollout plan

### Phase 1: Proof of concept (1 evening)
- Just `SubmitNZB` + `GetUsenetDownload` + a curl-based manual test
- No SABnzbd integration yet, no poller
- Goal: confirm the API actually works end-to-end with an NZBGeek NZB

### Phase 2: Minimum viable integration (2 evenings)
- All TorBox Usenet methods implemented
- Config flag + handler routing
- Basic poller (no fancy symlinks, just prove arr → TorBox flow)
- Manual import works

### Phase 3: Polish (2-3 evenings)
- Symlink automation matches torrent-side UX
- Queue/history views render correctly
- Unit tests
- Documentation
- Migration notes for existing users

### Phase 4: Upstream PR (optional)
- Open draft PR referencing issue #7
- Ask sirrobot01 about preferred interface shape (bolt-on vs separate `UsenetClient`)
- Iterate based on review

---

## Open questions to resolve before writing code

1. **Exact WebDAV path layout for Usenet on TorBox.** Need to confirm whether it's `/usenet/<name>/` or something else. Test with an existing download via `curl https://webdav.torbox.app/` with basic auth.
2. **Does `/api/usenet/mylist` support the same pagination/offset as torrents?** If not, adjust `GetUsenetDownloads` signature.
3. **How does TorBox surface "cached" NZBs?** Torrents have `/checkcached`; is there a Usenet equivalent for instant-hit NZBs, or are all submissions queued?
4. **Hash field for dedup.** Torrents use infohash; Usenet uses md5 of NZB content. Confirm Decypharr's dedup logic (`IsAvailable` checks) can handle the switch.
5. **Does sirrobot01 want this at all?** Quick issue comment on #7 before doing significant work, so the PR doesn't get rejected architecturally.

---

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| API shape drift — TorBox changes field names | Keep types in one file; one-shot fix when it breaks |
| Rate limit on create endpoint chokes bulk grabs | Queue submissions in worker; respect 10/min edge; fall back to `as_queued=true` when near limit |
| NZB upload size | TorBox accepts multi-MB NZBs; ensure `http.MaxBytesReader` / body limits aren't set low |
| Interface change breaks other providers | Add no-op default methods via an embedded `unsupportedUsenet` struct; compile fails if any provider missed |
| Upstream rejects the PR | Ship as a fork — `ghcr.io/jickman/decypharr:torbox-usenet` tag; rebase weekly |

---

## Time estimate

- **MVP (Phases 1-2):** 10-15 hours
- **Polished PR (all phases):** 25-40 hours

Most time sinks are in the poller/symlink glue (Phase 3), not the TorBox API calls themselves. The API work is mechanical — the existing `torbox.go` is a near-template.

---

## Reference links

- Upstream issue: https://github.com/sirrobot01/decypharr/issues/7
- TorBox API docs: https://api-docs.torbox.app/
- TorBox API (Swagger): https://api.torbox.app/docs
- TorBox rate limits: https://support.torbox.app/en/articles/13726368-api-rate-limits
- Reference Python SDK (for field shapes): https://github.com/TorBox-App/torbox-sdk-py
- Existing TorBox provider in Decypharr: `pkg/debrid/providers/torbox/torbox.go`
- Existing SABnzbd server: `pkg/server/sabnzbd/`
