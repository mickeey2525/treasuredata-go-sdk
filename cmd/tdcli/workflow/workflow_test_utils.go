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

	client, _ = td.NewClient("test-api-key")
	u, _ := url.Parse("http://example/")
	client.BaseURL = u
	client.WorkflowURL = u

	// In-memory transport routing to mux
	clientHTTP := &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		// Convert to server-style request so handlers see path-only URLs
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
	// Use client option to set the http client if available; otherwise assign directly
	// but since we are in tests, assigning directly is fine.
	clientHTTP.Timeout = 0
	clientHTTP.CheckRedirect = nil
	// Assign to underlying client
	// Note: td.Client has an unexported httpClient; we use WithHTTPClient normally, but here directly assign isn't possible.
	// However, td.NewClient initializes an httpClient we can replace via option; since we can't call option now, set field via method.
	// We expose it via the td package's WithHTTPClient in tests where needed. For this utility, we can rely on assigned transport.
	// So we set the transport on the existing httpClient.
	if clientHTTP.Transport != nil {
		if clientHTTP.Transport != nil {
			// set transport on existing client
		}
	}
	// Safer: set transport on client.httpClient directly via method on exported API
	// Since not available, use a workaround by creating a request to ensure transport is used from internal client.
	// Direct assignment to private field isn't possible here; instead, use the NewClient option in tests that call this utility.

	// As a fallback, set a default transport on the URL so tests using NewWorkflowRequest will still route via mux
	// by replacing the default client's transport globally is not desired; thus, we rely on WithHTTPClient in callers.

	// Assign transport on the internal client through exported option
	_ = td.WithHTTPClient(clientHTTP)(client)

	return client, mux, func() {}
}

// roundTripperFunc allows creating a RoundTripper from a function.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
