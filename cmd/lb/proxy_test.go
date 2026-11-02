package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordedProxyRequest struct {
	Method           string
	RequestURI       string
	RawQuery         string
	Host             string
	Header           http.Header
	TransferEncoding []string
	BodySHA256       [sha256.Size]byte
	BodyLength       int64
}

type recordingBackend struct {
	server   *httptest.Server
	requests chan recordedProxyRequest
}

func newRecordingBackend(t *testing.T, status int, headers http.Header, body []byte) *recordingBackend {
	t.Helper()
	return newRecordingBackendWithHandler(t, func(w http.ResponseWriter, _ *http.Request) {
		for name, values := range headers {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
}

func newRecordingBackendWithHandler(t *testing.T, respond func(http.ResponseWriter, *http.Request)) *recordingBackend {
	t.Helper()
	backend := &recordingBackend{requests: make(chan recordedProxyRequest, 64)}
	backend.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestBody, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}
		backend.requests <- recordedProxyRequest{
			Method:           r.Method,
			RequestURI:       r.URL.RequestURI(),
			RawQuery:         r.URL.RawQuery,
			Host:             r.Host,
			Header:           r.Header.Clone(),
			TransferEncoding: append([]string(nil), r.TransferEncoding...),
			BodySHA256:       sha256.Sum256(requestBody),
			BodyLength:       int64(len(requestBody)),
		}
		respond(w, r)
	}))
	t.Cleanup(backend.server.Close)
	return backend
}

func newProxyTestClient(t *testing.T) *http.Client {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func newProxyTestServer(t *testing.T, backend *recordingBackend, strategy string) *httptest.Server {
	t.Helper()
	pool, err := NewPool([]string{backend.server.URL})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if err := pool.SetStrategy(strategy); err != nil {
		t.Fatalf("SetStrategy(%q): %v", strategy, err)
	}
	t.Cleanup(func() {
		for _, registered := range pool.registry.snapshot() {
			registered.transport.CloseIdleConnections()
			registered.healthTransport.CloseIdleConnections()
		}
	})
	server := httptest.NewServer(pool.Handler())
	t.Cleanup(server.Close)
	return server
}

func doProxyRequest(t *testing.T, client *http.Client, server *httptest.Server, method, target string, body io.Reader) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+target, body)
	if err != nil {
		t.Fatalf("NewRequest(%q): %v", target, err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	return response
}

func readProxyResponse(t *testing.T, response *http.Response) []byte {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return body
}

func receiveProxyRequest(t *testing.T, backend *recordingBackend) recordedProxyRequest {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case request := <-backend.requests:
		return request
	case <-timer.C:
		t.Fatal("timed out waiting for backend request")
		return recordedProxyRequest{}
	}
}

func TestProxyGzipResponsePreserved(t *testing.T) {
	compressed := gzipProxyBody(t)

	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			headers := make(http.Header)
			headers.Set("Content-Encoding", "gzip")
			backend := newRecordingBackend(t, http.StatusOK, headers, compressed)
			server := newProxyTestServer(t, backend, strategy)
			client := newProxyTestClient(t)
			request, err := http.NewRequest(http.MethodGet, server.URL+"/gzip", nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Accept-Encoding", "gzip")
			response, err := client.Do(request)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			got := readProxyResponse(t, response)
			if response.Header.Get("Content-Encoding") != "gzip" {
				t.Errorf("Content-Encoding = %q, want gzip", response.Header.Get("Content-Encoding"))
			}
			if !bytes.Equal(got, compressed) {
				t.Errorf("response body changed: got %d bytes, want original compressed %d bytes", len(got), len(compressed))
			}
			upstream := receiveProxyRequest(t, backend)
			if got := upstream.Header.Get("Accept-Encoding"); got != "gzip" {
				t.Errorf("backend Accept-Encoding = %q, want gzip", got)
			}
		})
	}
}

func TestProxyGzipWhenClientSendsNoAcceptEncoding(t *testing.T) {
	compressed := gzipProxyBody(t)

	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			headers := make(http.Header)
			headers.Set("Content-Encoding", "gzip")
			backend := newRecordingBackend(t, http.StatusOK, headers, compressed)
			server := newProxyTestServer(t, backend, strategy)
			client := newProxyTestClient(t)
			response := doProxyRequest(t, client, server, http.MethodGet, "/gzip", nil)
			got := readProxyResponse(t, response)
			upstream := receiveProxyRequest(t, backend)

			if got := upstream.Header.Get("Accept-Encoding"); got != "gzip" {
				t.Errorf("backend Accept-Encoding = %q, want transport-added gzip", got)
			}
			if got := response.Header.Get("Content-Encoding"); got != "" {
				t.Errorf("client Content-Encoding = %q, want it removed after decompression", got)
			}
			if want := []byte("compressed response"); !bytes.Equal(got, want) {
				t.Errorf("response body = %q, want decompressed %q", got, want)
			}
		})
	}
}

func gzipProxyBody(t *testing.T) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte("compressed response")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}

func TestProxyMethodsAndBodies(t *testing.T) {
	methods := []struct {
		method string
		body   []byte
	}{
		{method: http.MethodGet},
		{method: http.MethodHead},
		{method: http.MethodPost, body: []byte(`{"method":"POST"}`)},
		{method: http.MethodPut, body: []byte("put-body")},
		{method: http.MethodPatch, body: []byte("patch-body")},
		{method: http.MethodDelete, body: []byte("delete-body")},
		{method: http.MethodOptions},
	}

	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			backend := newRecordingBackend(t, http.StatusOK, nil, []byte("backend response"))
			server := newProxyTestServer(t, backend, strategy)
			client := newProxyTestClient(t)
			for _, test := range methods {
				t.Run(test.method, func(t *testing.T) {
					response := doProxyRequest(t, client, server, test.method, "/methods", bytes.NewReader(test.body))
					readProxyResponse(t, response)
					request := receiveProxyRequest(t, backend)
					if request.Method != test.method {
						t.Errorf("backend method = %q, want %q", request.Method, test.method)
					}
					assertProxyBody(t, request, test.body)
				})
			}
		})
	}
}

func TestProxyQueryAndPathPreserved(t *testing.T) {
	long := strings.Repeat("x", 4096)
	tests := []struct {
		name string
		path string
	}{
		{name: "query encodings", path: "/query?empty=&repeat=one&repeat=two&escaped=%20%2B%26%3D&unicode=%E2%9C%93"},
		{name: "few KB query", path: "/query?large=" + long},
		{name: "flag without value", path: "/query?flag"},
		{name: "trailing slash", path: "/trailing/"},
		{name: "double slashes", path: "/double//slashes"},
		{name: "escaped slash", path: "/escaped%2Fslash"},
		{name: "dot segments", path: "/dots/../kept"},
		{name: "few KB path", path: "/" + strings.Repeat("p", 4096)},
	}

	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			backend := newRecordingBackend(t, http.StatusOK, nil, nil)
			server := newProxyTestServer(t, backend, strategy)
			client := newProxyTestClient(t)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					response := doProxyRequest(t, client, server, http.MethodGet, test.path, nil)
					readProxyResponse(t, response)
					request := receiveProxyRequest(t, backend)
					if request.RequestURI != test.path {
						t.Errorf("backend RequestURI = %q, want %q", request.RequestURI, test.path)
					}
				})
			}
		})
	}
}

func TestProxyRequestHeadersAndHost(t *testing.T) {
	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			backend := newRecordingBackend(t, http.StatusOK, nil, nil)
			server := newProxyTestServer(t, backend, strategy)
			client := newProxyTestClient(t)
			request, err := http.NewRequest(http.MethodGet, server.URL+"/headers", nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Host = "public.example.test"
			request.Header.Set("X-Custom", "custom-value")
			request.Header.Set("Authorization", "Bearer test-token")
			request.Header.Add("Cookie", "first=one")
			request.Header.Add("Cookie", "second=two")
			request.Header.Add("X-Multi", "first")
			request.Header.Add("X-Multi", "second")
			request.Header.Set("Connection", "X-Remove-Me")
			request.Header.Set("X-Remove-Me", "must-not-forward")
			response, err := client.Do(request)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			readProxyResponse(t, response)
			got := receiveProxyRequest(t, backend)

			if got.Host != request.Host {
				t.Errorf("backend Host = %q, want %q", got.Host, request.Host)
			}
			for name, want := range map[string]string{
				"X-Custom":      "custom-value",
				"Authorization": "Bearer test-token",
			} {
				if got.Header.Get(name) != want {
					t.Errorf("backend %s = %q, want %q", name, got.Header.Get(name), want)
				}
			}
			if values := got.Header.Values("Cookie"); !equalStrings(values, []string{"first=one", "second=two"}) {
				t.Errorf("backend Cookie values = %#v, want both values", values)
			}
			if values := got.Header.Values("X-Multi"); !equalStrings(values, []string{"first", "second"}) {
				t.Errorf("backend X-Multi values = %#v, want both values", values)
			}
			if got.Header.Get("Connection") != "" || got.Header.Get("X-Remove-Me") != "" {
				t.Errorf("hop-by-hop request headers forwarded: Connection=%q X-Remove-Me=%q", got.Header.Get("Connection"), got.Header.Get("X-Remove-Me"))
			}
			if got.Header.Get("X-Forwarded-For") == "" {
				t.Error("proxy did not add X-Forwarded-For")
			}
		})
	}
}

func TestProxyXForwardedForAddedAndAppended(t *testing.T) {
	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			backend := newRecordingBackend(t, http.StatusOK, nil, nil)
			server := newProxyTestServer(t, backend, strategy)
			client := newProxyTestClient(t)
			for _, existing := range []string{"", "192.0.2.10, 198.51.100.20"} {
				request, err := http.NewRequest(http.MethodGet, server.URL+"/xff", nil)
				if err != nil {
					t.Fatal(err)
				}
				if existing != "" {
					request.Header.Set("X-Forwarded-For", existing)
				}
				response, err := client.Do(request)
				if err != nil {
					t.Fatalf("GET: %v", err)
				}
				readProxyResponse(t, response)
				got := receiveProxyRequest(t, backend).Header.Get("X-Forwarded-For")
				parts := strings.Split(got, ", ")
				if existing == "" {
					if len(parts) != 1 || parts[0] == "" {
						t.Errorf("added X-Forwarded-For = %q, want one client address", got)
					}
				} else if len(parts) != 3 || strings.Join(parts[:2], ", ") != existing || parts[2] == "" {
					t.Errorf("appended X-Forwarded-For = %q, want %q plus client address", got, existing)
				}
			}
		})
	}
}

func TestProxyResponseHeaders(t *testing.T) {
	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			headers := make(http.Header)
			headers.Set("X-Custom-Response", "custom-value")
			headers.Set("ETag", `"proxy-etag"`)
			headers.Set("Location", "/redirect-target")
			headers.Add("Set-Cookie", "first=one; Path=/")
			headers.Add("Set-Cookie", "second=two; Path=/")
			headers.Set("Connection", "X-Remove-Response")
			headers.Set("X-Remove-Response", "must-not-forward")
			backend := newRecordingBackend(t, http.StatusOK, headers, []byte("response"))
			server := newProxyTestServer(t, backend, strategy)
			response := doProxyRequest(t, newProxyTestClient(t), server, http.MethodGet, "/response-headers", nil)
			readProxyResponse(t, response)

			for name, want := range map[string]string{
				"X-Custom-Response": "custom-value",
				"ETag":              `"proxy-etag"`,
				"Location":          "/redirect-target",
			} {
				if got := response.Header.Get(name); got != want {
					t.Errorf("response %s = %q, want %q", name, got, want)
				}
			}
			if got := response.Header.Values("Set-Cookie"); !equalStrings(got, []string{"first=one; Path=/", "second=two; Path=/"}) {
				t.Errorf("Set-Cookie values = %#v, want both cookies", got)
			}
			if response.Header.Get("Connection") != "" || response.Header.Get("X-Remove-Response") != "" {
				t.Errorf("hop-by-hop response headers forwarded: Connection=%q X-Remove-Response=%q", response.Header.Get("Connection"), response.Header.Get("X-Remove-Response"))
			}
		})
	}
}

func TestProxyBodySizesAndChunkedRequest(t *testing.T) {
	binaryBody := make([]byte, 256)
	for i := range binaryBody {
		binaryBody[i] = byte(i)
	}
	largeBody := make([]byte, 1<<20)
	for i := range largeBody {
		largeBody[i] = byte(i * 31)
	}
	tests := []struct {
		name    string
		body    []byte
		chunked bool
	}{
		{name: "empty"},
		{name: "JSON", body: []byte(`{"message":"hello","count":3}`)},
		{name: "binary 256 bytes", body: binaryBody},
		{name: "1 MB", body: largeBody},
		{name: "chunked request", body: []byte("chunked request body"), chunked: true},
	}

	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			backend := newRecordingBackend(t, http.StatusOK, nil, nil)
			server := newProxyTestServer(t, backend, strategy)
			client := newProxyTestClient(t)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					request, err := http.NewRequest(http.MethodPost, server.URL+"/body", bytes.NewReader(test.body))
					if err != nil {
						t.Fatal(err)
					}
					if test.chunked {
						request.ContentLength = -1
					}
					response, err := client.Do(request)
					if err != nil {
						t.Fatalf("POST: %v", err)
					}
					readProxyResponse(t, response)
					got := receiveProxyRequest(t, backend)
					assertProxyBody(t, got, test.body)
					if test.chunked && !equalStrings(got.TransferEncoding, []string{"chunked"}) {
						t.Errorf("backend TransferEncoding = %#v, want [chunked]", got.TransferEncoding)
					}
				})
			}
		})
	}
}

func TestProxyStreamingAndChunkedResponse(t *testing.T) {
	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			firstChunkWritten := make(chan struct{})
			releaseSecondChunk := make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(releaseSecondChunk) }) })
			backend := newRecordingBackendWithHandler(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "first-")
				w.(http.Flusher).Flush()
				close(firstChunkWritten)
				select {
				case <-releaseSecondChunk:
					_, _ = io.WriteString(w, "second")
				case <-r.Context().Done():
				}
			})
			server := newProxyTestServer(t, backend, strategy)
			client := newProxyTestClient(t)
			response, err := client.Get(server.URL + "/stream")
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			defer response.Body.Close()
			if response.ContentLength != -1 {
				t.Errorf("ContentLength = %d, want -1 for streamed response", response.ContentLength)
			}
			if !equalStrings(response.TransferEncoding, []string{"chunked"}) {
				t.Errorf("TransferEncoding = %#v, want [chunked]", response.TransferEncoding)
			}
			first := make([]byte, len("first-"))
			if _, err := io.ReadFull(response.Body, first); err != nil {
				t.Fatalf("read first streamed chunk: %v", err)
			}
			if string(first) != "first-" {
				t.Fatalf("first streamed bytes = %q, want first chunk", first)
			}
			select {
			case <-firstChunkWritten:
			default:
				t.Fatal("client received first chunk before backend wrote it")
			}
			releaseOnce.Do(func() { close(releaseSecondChunk) })
			remaining, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatalf("read remaining streamed body: %v", err)
			}
			if string(remaining) != "second" {
				t.Errorf("remaining streamed bytes = %q, want second", remaining)
			}
			receiveProxyRequest(t, backend)
		})
	}
}

func TestProxyStatusCodesPassedThrough(t *testing.T) {
	statuses := []int{
		http.StatusOK, http.StatusCreated, http.StatusNoContent, http.StatusMovedPermanently,
		http.StatusFound, http.StatusTemporaryRedirect, http.StatusNotModified, http.StatusBadRequest,
		http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed,
		http.StatusConflict, http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable,
	}
	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			backend := newRecordingBackendWithHandler(t, func(w http.ResponseWriter, r *http.Request) {
				status, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/status/"))
				if err != nil {
					http.Error(w, "invalid status", http.StatusBadRequest)
					return
				}
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "backend status")
			})
			server := newProxyTestServer(t, backend, strategy)
			client := newProxyTestClient(t)
			for _, status := range statuses {
				t.Run(strconv.Itoa(status), func(t *testing.T) {
					response := doProxyRequest(t, client, server, http.MethodGet, fmt.Sprintf("/status/%d", status), nil)
					readProxyResponse(t, response)
					if response.StatusCode != status {
						t.Errorf("status = %d, want unchanged backend status %d", response.StatusCode, status)
					}
					receiveProxyRequest(t, backend)
				})
			}
		})
	}
}

func TestProxyHealthProbesAreSeparateFromTraffic(t *testing.T) {
	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			backend := newRecordingBackend(t, http.StatusOK, nil, []byte("ok"))
			pool, err := NewPool([]string{backend.server.URL})
			if err != nil {
				t.Fatalf("NewPool: %v", err)
			}
			if err := pool.SetStrategy(strategy); err != nil {
				t.Fatalf("SetStrategy(%q): %v", strategy, err)
			}
			t.Cleanup(func() {
				for _, registered := range pool.registry.snapshot() {
					registered.transport.CloseIdleConnections()
					registered.healthTransport.CloseIdleConnections()
				}
			})
			server := httptest.NewServer(pool.Handler())
			t.Cleanup(server.Close)
			pool.CheckOnce()
			probe := receiveProxyRequest(t, backend)
			if probe.Method != http.MethodGet || probe.RequestURI != "/" {
				t.Errorf("health probe = %s %q, want GET /", probe.Method, probe.RequestURI)
			}

			response := doProxyRequest(t, newProxyTestClient(t), server, http.MethodGet, "/traffic", nil)
			readProxyResponse(t, response)
			traffic := receiveProxyRequest(t, backend)
			if traffic.RequestURI != "/traffic" {
				t.Errorf("traffic request URI = %q, want /traffic", traffic.RequestURI)
			}
			statuses := pool.registry.List()
			if len(statuses) != 1 || statuses[0].TotalRequests != 1 {
				t.Errorf("backend traffic count = %#v, want one proxied request (probe excluded)", statuses)
			}
		})
	}
}

func TestProxyDownBackendReturns502WithoutRetryByDefault(t *testing.T) {
	for _, strategy := range []string{"round_robin", "least_conns"} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			t.Run(strategy+"/"+method, func(t *testing.T) {
				unavailable := httptest.NewServer(http.NotFoundHandler())
				target := unavailable.URL
				unavailable.Close()
				pool, err := NewPool([]string{target})
				if err != nil {
					t.Fatalf("NewPool: %v", err)
				}
				if err := pool.SetStrategy(strategy); err != nil {
					t.Fatalf("SetStrategy(%q): %v", strategy, err)
				}
				proxyServer := httptest.NewServer(pool.Handler())
				defer proxyServer.Close()
				response := doProxyRequest(t, newProxyTestClient(t), proxyServer, method, "/unavailable", strings.NewReader("post body"))
				readProxyResponse(t, response)
				if response.StatusCode != http.StatusBadGateway {
					t.Errorf("status = %d, want 502", response.StatusCode)
				}
				if got := pool.RetryStats().RetriesPerformed; got != 0 {
					t.Errorf("retries performed = %d, want 0 by default", got)
				}
				statuses := pool.registry.List()
				if len(statuses) != 1 || statuses[0].TotalRequests != 1 {
					t.Errorf("backend request counts = %#v, want one attempt", statuses)
				}
			})
		}
	}
}

func TestProxyConcurrentMixedRequestsDoNotCrossTalk(t *testing.T) {
	const requestCount = 20
	backend := newRecordingBackendWithHandler(t, func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		w.Header().Set("X-Backend-Request-ID", id)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "response-"+id)
	})

	for _, strategy := range []string{"round_robin", "least_conns"} {
		t.Run(strategy, func(t *testing.T) {
			server := newProxyTestServer(t, backend, strategy)
			client := newProxyTestClient(t)
			type result struct {
				id       string
				status   int
				headerID string
				body     string
				err      error
			}
			results := make(chan result, requestCount)
			var requests sync.WaitGroup
			for i := 0; i < requestCount; i++ {
				i := i
				requests.Add(1)
				go func() {
					defer requests.Done()
					id := strconv.Itoa(i)
					method := http.MethodGet
					body := ""
					if i%2 == 1 {
						method = http.MethodPost
						body = "request-body-" + id
					}
					request, err := http.NewRequest(method, server.URL+"/parallel?id="+url.QueryEscape(id), strings.NewReader(body))
					if err != nil {
						results <- result{id: id, err: err}
						return
					}
					request.Header.Set("X-Request-ID", id)
					response, err := client.Do(request)
					if err != nil {
						results <- result{id: id, err: err}
						return
					}
					responseBody, readErr := io.ReadAll(response.Body)
					_ = response.Body.Close()
					if readErr != nil {
						results <- result{id: id, err: readErr}
						return
					}
					results <- result{
						id:       id,
						status:   response.StatusCode,
						headerID: response.Header.Get("X-Backend-Request-ID"),
						body:     string(responseBody),
					}
				}()
			}
			requests.Wait()
			close(results)
			for got := range results {
				if got.err != nil {
					t.Errorf("request %s: %v", got.id, got.err)
					continue
				}
				if got.status != http.StatusOK || got.headerID != got.id || got.body != "response-"+got.id {
					t.Errorf("request %s crossed response data: status=%d header ID=%q body=%q", got.id, got.status, got.headerID, got.body)
				}
			}
			seenIDs := make(map[string]bool, requestCount)
			for i := 0; i < requestCount; i++ {
				record := receiveProxyRequest(t, backend)
				id := record.Header.Get("X-Request-ID")
				if id == "" || seenIDs[id] {
					t.Errorf("backend request ID = %q, empty or duplicated", id)
				}
				seenIDs[id] = true
				if record.RequestURI != "/parallel?id="+url.QueryEscape(id) {
					t.Errorf("backend request URI = %q for ID %q", record.RequestURI, id)
				}
				if record.Method == http.MethodPost {
					assertProxyBody(t, record, []byte("request-body-"+id))
				} else if record.Method == http.MethodGet {
					assertProxyBody(t, record, nil)
				} else {
					t.Errorf("unexpected backend method %q for ID %q", record.Method, id)
				}
			}
		})
	}
}

func assertProxyBody(t *testing.T, got recordedProxyRequest, want []byte) {
	t.Helper()
	if got.BodyLength != int64(len(want)) {
		t.Errorf("backend body length = %d, want %d", got.BodyLength, len(want))
	}
	if got.BodySHA256 != sha256.Sum256(want) {
		t.Errorf("backend body SHA-256 = %x, want %x", got.BodySHA256, sha256.Sum256(want))
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
