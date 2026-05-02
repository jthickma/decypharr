package torbox

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	json "github.com/bytedance/sonic"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "decypharr-torbox-test-*")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func newTestTorbox(host string) *Torbox {
	return &Torbox{
		Host:   host,
		APIKey: "test-token",
		client: request.New(
			request.WithHeaders(map[string]string{"Authorization": "Bearer test-token"}),
			request.WithMaxRetries(0),
		),
		config: config.Debrid{Name: "torbox"},
	}
}

func TestSubmitNZBSendsMultipartFileAndPostProcessing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/usenet/createusenetdownload" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatalf("ParseMultipartForm: %v", err)
		}
		if got := r.FormValue("post_processing"); got != "-1" {
			t.Fatalf("post_processing = %q, want -1", got)
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			t.Fatalf("FormFile: %v", err)
		}
		defer file.Close()
		if header.Filename != "upload.nzb" {
			t.Fatalf("filename = %q, want upload.nzb", header.Filename)
		}
		body, err := io.ReadAll(file)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(body) != "<nzb />" {
			t.Fatalf("file body = %q", string(body))
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"usenetdownload_id":42,"hash":"abc","auth_id":"auth"}}`))
	}))
	defer srv.Close()

	result, err := newTestTorbox(srv.URL).SubmitNZB(context.Background(), []byte("<nzb />"), "upload.nzb", types.UsenetSubmitOpts{
		PostProcessing: -1,
	})
	if err != nil {
		t.Fatalf("SubmitNZB: %v", err)
	}
	if result.Id != "42" || result.Hash != "abc" || result.AuthId != "auth" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestDeleteNZBUsesControlEndpointJSONOperation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/usenet/controlusenetdownload" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
			t.Fatalf("content type = %q", got)
		}
		var payload map[string]any
		if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if payload["usenet_id"] != "42" || payload["operation"] != "delete" || payload["all"] != false {
			t.Fatalf("unexpected payload: %#v", payload)
		}
		_, _ = w.Write([]byte(`{"success":true,"data":null}`))
	}))
	defer srv.Close()

	if err := newTestTorbox(srv.URL).DeleteNZB("42"); err != nil {
		t.Fatalf("DeleteNZB: %v", err)
	}
}

func TestGetNZBDownloadLinkBuildsUsenetPermalink(t *testing.T) {
	tb := newTestTorbox("https://api.torbox.app/v1")
	link, err := tb.GetNZBDownloadLink("42", "7")
	if err != nil {
		t.Fatalf("GetNZBDownloadLink: %v", err)
	}
	want := "https://api.torbox.app/v1/api/usenet/requestdl?file_id=7&redirect=true&token=test-token&usenet_id=42"
	if link.DownloadLink != want {
		t.Fatalf("download link = %q, want %q", link.DownloadLink, want)
	}
	if link.Debrid != "torbox" || link.Id != "7" {
		t.Fatalf("unexpected metadata: %+v", link)
	}
}
