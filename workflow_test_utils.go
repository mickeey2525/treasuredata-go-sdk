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
	// mux is the HTTP request multiplexer used for handling requests.
	mux = http.NewServeMux()

	// client is the Treasure Data client being tested.
	client, _ = NewClient("test-api-key")

	// Use a dummy base URL; requests are handled in-memory via custom transport.
	u, _ := url.Parse("http://example/")
	client.BaseURL = u
	client.WorkflowURL = u

	// Install an in-memory transport that routes requests to mux without listening on a port.
    client.httpClient = &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
        // Convert client request to a server-style request so handlers see path-only URLs
        r2 := req.Clone(req.Context())
        r2.URL = &url.URL{Path: req.URL.Path, RawQuery: req.URL.RawQuery}
        r2.RequestURI = r2.URL.RequestURI()
        r2.Host = ""

        rr := httptest.NewRecorder()
        mux.ServeHTTP(rr, r2)
        resp := rr.Result()
        resp.Request = req
        return resp, nil
    })}

	return client, mux, func() {}
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
	// mux is the HTTP request multiplexer used for handling requests.
	mux = http.NewServeMux()

	// client is the Treasure Data client being tested.
	client, _ = NewClient("test-api-key")

	// Use a dummy base URL; requests are handled in-memory via custom transport.
	u, _ := url.Parse("http://example/")
	client.CDPURL = u

	// Install an in-memory transport that routes requests to mux without listening on a port.
    client.httpClient = &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
        // Convert to server-style request
        r2 := req.Clone(req.Context())
        r2.URL = &url.URL{Path: req.URL.Path, RawQuery: req.URL.RawQuery}
        r2.RequestURI = r2.URL.RequestURI()
        r2.Host = ""

        rr := httptest.NewRecorder()
        mux.ServeHTTP(rr, r2)
        resp := rr.Result()
        resp.Request = req
        return resp, nil
    })}

	return client, mux, func() {}
}

// roundTripperFunc is a helper to create http.RoundTripper from a function.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
