package workflow

import (
	"net/http"
	"net/http/httptest"
	"net/url"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// setupWorkflowTest sets up a test HTTP server for workflow testing
func setupWorkflowTest() (client *td.Client, mux *http.ServeMux, teardown func()) {
	mux = http.NewServeMux()
	server := httptest.NewServer(mux)

	client, _ = td.NewClient("test-api-key")
	url, _ := url.Parse(server.URL + "/")
	client.BaseURL = url
	client.WorkflowURL = url

	return client, mux, func() {
		server.Close()
	}
}
