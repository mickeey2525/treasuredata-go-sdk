package treasuredata

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// setup sets up a test HTTP server along with a treasuredata.Client that is
// configured to talk to that test server. Tests should register handlers on
// mux which provide mock responses for the API method being tested.
func setup() (client *Client, mux *http.ServeMux, teardown func()) {
	// mux is the HTTP request multiplexer used with the test server.
	mux = http.NewServeMux()

	// server is a test HTTP server used to provide mock API responses.
	server := httptest.NewServer(mux)

	// client is the Treasure Data client being tested.
	client, _ = NewClient("test-api-key")
	url, _ := url.Parse(server.URL + "/")
	client.BaseURL = url
	client.WorkflowURL = url

	return client, mux, func() {
		server.Close()
	}
}

// testMethod is a helper function to test that the HTTP method used is correct.
func testMethod(t *testing.T, r *http.Request, want string) {
	t.Helper()
	if got := r.Method; got != want {
		t.Errorf("Request method: %v, want %v", got, want)
	}
}

// testURL is a helper function to test that the URL path is correct.
func testURL(t *testing.T, r *http.Request, want string) {
	t.Helper()
	if got := r.URL.String(); got != want {
		t.Errorf("Request URL: %v, want %v", got, want)
	}
}

// setupCDP sets up a test HTTP server along with a treasuredata.Client that is
// configured to talk to that test server for CDP endpoints. Tests should register handlers on
// mux which provide mock responses for the CDP API method being tested.
func setupCDP() (client *Client, mux *http.ServeMux, teardown func()) {
	// mux is the HTTP request multiplexer used with the test server.
	mux = http.NewServeMux()

	// server is a test HTTP server used to provide mock API responses.
	server := httptest.NewServer(mux)

	// client is the Treasure Data client being tested.
	client, _ = NewClient("test-api-key")
	url, _ := url.Parse(server.URL + "/")
	client.CDPURL = url

	return client, mux, func() {
		server.Close()
	}
}
