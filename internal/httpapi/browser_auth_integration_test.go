package httpapi_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/auth"
	"magworkspace/internal/httpapi"
)

const browserAuthBase = "/api/v1/auth/browser"
const refreshCookieName = "magworkspace_refresh"

type browserResponse struct {
	integrationResponse
	cookies []*http.Cookie
}

func browserCall(t *testing.T, client *http.Client, baseURL, path, body string, cookie *http.Cookie) browserResponse {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+browserAuthBase+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", baseURL)
	req.Header.Set("X-Magworkspace-Browser", "1")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return browserResponse{integrationResponse{status: resp.StatusCode, body: data, err: err}, resp.Cookies()}
}

func checkBrowserData(t *testing.T, response browserResponse, status int) integrationAuth {
	t.Helper()
	envelope := checkIntegrationResponse(t, response.integrationResponse, status)
	if bytes.Contains(envelope.Data, []byte(`"refresh_token"`)) {
		t.Fatal("browser response exposed the refresh token in JSON")
	}
	return integrationData[integrationAuth](t, envelope)
}

func checkRefreshCookie(t *testing.T, response browserResponse, deleted bool) *http.Cookie {
	t.Helper()
	if len(response.cookies) != 1 {
		t.Fatalf("Set-Cookie count = %d, want 1", len(response.cookies))
	}
	cookie := response.cookies[0]
	if cookie.Name != refreshCookieName || cookie.Path != browserAuthBase || cookie.Domain != "" ||
		!cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode {
		t.Fatal("refresh cookie must be HttpOnly, Strict, host-only and limited to browser auth")
	}
	if deleted {
		if cookie.Value != "" || cookie.MaxAge != -1 || !cookie.Expires.Before(time.Now()) {
			t.Fatal("refresh cookie was not cleared")
		}
	} else if !auth.ValidToken(cookie.Value, "rt") || cookie.MaxAge < 1 || !cookie.Expires.After(time.Now()) {
		t.Fatal("missing valid persistent refresh cookie")
	}
	return cookie
}

func TestHTTPBrowserAuthOriginGuard(t *testing.T) {
	handler := httpapi.New(nil, nil, httpapi.Config{})
	for _, path := range []string{"/register", "/login", "/refresh", "/logout"} {
		for _, test := range []struct {
			name   string
			modify func(*http.Request)
			status int
		}{
			{"missing_origin", func(r *http.Request) { r.Header.Del("Origin") }, 403},
			{"null_origin", func(r *http.Request) { r.Header.Set("Origin", "null") }, 403},
			{"other_origin", func(r *http.Request) { r.Header.Set("Origin", "http://attacker.test") }, 403},
			{"same_site_other_origin", func(r *http.Request) { r.Header.Set("Origin", "http://sub.localhost:8080") }, 403},
			{"origin_path", func(r *http.Request) { r.Header.Set("Origin", "http://localhost:8080/") }, 403},
			{"origin_duplicate", func(r *http.Request) { r.Header.Add("Origin", "http://localhost:8080") }, 403},
			{"wrong_scheme", func(r *http.Request) { r.Header.Set("Origin", "https://localhost:8080") }, 403},
			{"missing_marker", func(r *http.Request) { r.Header.Del("X-Magworkspace-Browser") }, 403},
			{"marker_duplicate", func(r *http.Request) { r.Header.Add("X-Magworkspace-Browser", "1") }, 403},
			{"fetch_same_site", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "same-site") }, 403},
			{"fetch_cross_site", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }, 403},
			{"fetch_duplicate", func(r *http.Request) { r.Header.Add("Sec-Fetch-Site", "same-origin") }, 403},
			{"form", func(r *http.Request) { r.Header.Set("Content-Type", "application/x-www-form-urlencoded") }, 415},
			{"forwarded_cannot_allow_origin", func(r *http.Request) {
				r.Header.Set("Origin", "https://example.test")
				r.Header.Set("Forwarded", "host=example.test;proto=https")
				r.Header.Set("X-Forwarded-Host", "example.test")
				r.Header.Set("X-Forwarded-Proto", "https")
			}, 403},
		} {
			t.Run(path+"/"+test.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodPost, "http://localhost:8080"+browserAuthBase+path, strings.NewReader("{}"))
				req.Header.Set("Origin", "http://localhost:8080")
				req.Header.Set("X-Magworkspace-Browser", "1")
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Sec-Fetch-Site", "same-origin")
				test.modify(req)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, req)
				if response.Code != test.status {
					t.Fatalf("status = %d, want %d", response.Code, test.status)
				}
				if response.Header().Get("Set-Cookie") != "" {
					t.Fatal("rejected origin must not change the session cookie")
				}
			})
		}
	}
}

func TestHTTPBrowserAuthEmptyBodyAndCookieFlags(t *testing.T) {
	handler := httpapi.New(nil, nil, httpapi.Config{})
	for _, test := range []struct {
		name, host string
		tls        bool
		secure     bool
	}{
		{"loopback", "127.0.0.1:8080", false, false},
		{"localhost", "localhost:8080", false, false},
		{"ipv6_loopback", "[::1]:8080", false, false},
		{"remote", "example.test", false, true},
		{"private_network", "192.168.1.2:8080", false, true},
		{"loopback_tls", "127.0.0.1:8080", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := "http"
			if test.tls {
				scheme = "https"
			}
			req := httptest.NewRequest(http.MethodPost, scheme+"://"+test.host+browserAuthBase+"/logout", strings.NewReader("{}"))
			if test.tls {
				req.TLS = &tls.ConnectionState{}
			}
			req.Header.Set("Origin", scheme+"://"+test.host)
			req.Header.Set("X-Magworkspace-Browser", "1")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Forwarded-Proto", "https")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != http.StatusNoContent {
				t.Fatalf("logout without cookie = %d, want 204", response.Code)
			}
			cookie := checkRefreshCookie(t, browserResponse{cookies: response.Result().Cookies()}, true)
			if cookie.Secure != test.secure {
				t.Fatalf("Secure = %v, want %v", cookie.Secure, test.secure)
			}
		})
	}
	for _, path := range []string{"/refresh", "/logout"} {
		for _, body := range []string{"", "null", "[]", `{"refresh_token":"ignored"}`, `{} {}`} {
			req := httptest.NewRequest(http.MethodPost, "http://localhost"+browserAuthBase+path, strings.NewReader(body))
			req.Header.Set("Origin", "http://localhost")
			req.Header.Set("X-Magworkspace-Browser", "1")
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != http.StatusBadRequest || response.Header().Get("Set-Cookie") != "" {
				t.Fatalf("%s body %q must reject with 400 and preserve cookie; status = %d", path, body, response.Code)
			}
		}
	}
}

func TestHTTPBrowserAuthRejectsAmbiguousCookies(t *testing.T) {
	handler := httpapi.New(nil, nil, httpapi.Config{})
	valid := auth.NewRefreshToken()
	for _, headers := range [][]string{
		{refreshCookieName + "=not-a-token"},
		{refreshCookieName + "=" + valid + "; " + refreshCookieName + "=" + valid},
		{refreshCookieName + "=" + valid, refreshCookieName + "=" + valid},
		{refreshCookieName + `="unterminated`},
		{refreshCookieName + "=" + valid + "; invalid"},
	} {
		for _, path := range []string{"/refresh", "/logout"} {
			req := httptest.NewRequest(http.MethodPost, "http://localhost"+browserAuthBase+path, strings.NewReader("{}"))
			req.Header.Set("Origin", "http://localhost")
			req.Header.Set("X-Magworkspace-Browser", "1")
			req.Header.Set("Content-Type", "application/json")
			for _, value := range headers {
				req.Header.Add("Cookie", value)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("ambiguous %s cookie returned %d, want 401", path, response.Code)
			}
			checkRefreshCookie(t, browserResponse{cookies: response.Result().Cookies()}, true)
		}
	}
}

func TestHTTPBrowserAuthLifecycleSurvivesRestartAndLogout(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true, RefreshTTL: 2 * time.Hour})
	client := &http.Client{Timeout: 10 * time.Second}
	registeredResponse := browserCall(t, client, f.server.URL, "/register", `{"name":"browser","email":"browser@example.test","password":"correct-horse-42"}`, nil)
	registered := checkBrowserData(t, registeredResponse, http.StatusCreated)
	cookie := checkRefreshCookie(t, registeredResponse, false)
	if registered.User.ID == "" || len(registered.Workspaces) != 1 || registered.Token == "" || cookie.Secure {
		t.Fatal("browser registration did not return identity/access and local cookie")
	}
	if cookie.MaxAge < 7199 || cookie.MaxAge > 7200 || time.Until(cookie.Expires) < 119*time.Minute || time.Until(cookie.Expires) > 121*time.Minute {
		t.Fatal("refresh cookie expiry does not follow the configured RefreshTTL")
	}
	f.request(t, http.MethodGet, "/api/v1/auth/me", registered.Token, nil, http.StatusOK)
	f.restart(t)
	refreshedResponse := browserCall(t, client, f.server.URL, "/refresh", "{}", cookie)
	refreshed := checkBrowserData(t, refreshedResponse, http.StatusOK)
	rotatedCookie := checkRefreshCookie(t, refreshedResponse, false)
	if refreshed.Token == "" || refreshed.Token == registered.Token || rotatedCookie.Value == cookie.Value {
		t.Fatal("browser refresh did not rotate both credentials")
	}
	f.request(t, http.MethodGet, "/api/v1/auth/me", registered.Token, nil, http.StatusUnauthorized)
	f.request(t, http.MethodGet, "/api/v1/auth/me", refreshed.Token, nil, http.StatusOK)
	reusedResponse := browserCall(t, client, f.server.URL, "/refresh", "{}", cookie)
	checkIntegrationResponse(t, reusedResponse.integrationResponse, http.StatusUnauthorized)
	checkRefreshCookie(t, reusedResponse, true)
	logoutResponse := browserCall(t, client, f.server.URL, "/logout", "{}", rotatedCookie)
	checkIntegrationResponse(t, logoutResponse.integrationResponse, http.StatusNoContent)
	checkRefreshCookie(t, logoutResponse, true)
	f.request(t, http.MethodGet, "/api/v1/auth/me", refreshed.Token, nil, http.StatusUnauthorized)
	revokedResponse := browserCall(t, client, f.server.URL, "/refresh", "{}", rotatedCookie)
	checkIntegrationResponse(t, revokedResponse.integrationResponse, http.StatusUnauthorized)
	checkRefreshCookie(t, revokedResponse, true)
	for _, logoutCookie := range []*http.Cookie{rotatedCookie, nil} {
		response := browserCall(t, client, f.server.URL, "/logout", "{}", logoutCookie)
		checkIntegrationResponse(t, response.integrationResponse, http.StatusNoContent)
		checkRefreshCookie(t, response, true)
	}
	loginResponse := browserCall(t, client, f.server.URL, "/login", `{"email":"browser@example.test","password":"correct-horse-42"}`, nil)
	loggedIn := checkBrowserData(t, loginResponse, http.StatusOK)
	checkRefreshCookie(t, loginResponse, false)
	if loggedIn.User.ID != registered.User.ID || loggedIn.Token == "" || len(loggedIn.Workspaces) != 1 {
		t.Fatal("browser login lost identity or workspaces")
	}
	// Original JSON clients retain their independent token response/rotation.
	legacy := f.register(t, "legacy")
	legacyRefresh := integrationData[integrationAuth](t, f.request(t, http.MethodPost, "/api/v1/auth/refresh", "", map[string]string{"refresh_token": legacy.RefreshToken}, http.StatusOK))
	if legacyRefresh.Token == "" || legacyRefresh.RefreshToken == "" {
		t.Fatal("original JSON token contract changed")
	}
}

func TestHTTPBrowserAuthLogoutWithExpiredAccessAndDatabaseFailure(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	client := &http.Client{Timeout: 10 * time.Second}
	login := browserCall(t, client, f.server.URL, "/register", `{"name":"expired","email":"expired@example.test","password":"correct-horse-42"}`, nil)
	credentials := checkBrowserData(t, login, http.StatusCreated)
	cookie := checkRefreshCookie(t, login, false)
	db, err := sql.Open("mysql", f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `UPDATE auth_sessions SET access_expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE access_hash=?`, auth.HashToken(credentials.Token)); err != nil {
		t.Fatal(err)
	}
	f.request(t, http.MethodGet, "/api/v1/auth/me", credentials.Token, nil, http.StatusUnauthorized)
	logout := browserCall(t, client, f.server.URL, "/logout", "{}", cookie)
	checkIntegrationResponse(t, logout.integrationResponse, http.StatusNoContent)
	checkRefreshCookie(t, logout, true)
	refresh := browserCall(t, client, f.server.URL, "/refresh", "{}", cookie)
	checkIntegrationResponse(t, refresh.integrationResponse, http.StatusUnauthorized)
	checkRefreshCookie(t, refresh, true)
	login = browserCall(t, client, f.server.URL, "/login", `{"email":"expired@example.test","password":"correct-horse-42"}`, nil)
	checkBrowserData(t, login, http.StatusOK)
	cookie = checkRefreshCookie(t, login, false)
	f.store.Close()
	for _, path := range []string{"/refresh", "/logout"} {
		response := browserCall(t, client, f.server.URL, path, "{}", cookie)
		checkIntegrationResponse(t, response.integrationResponse, http.StatusInternalServerError)
		if len(response.cookies) != 0 {
			t.Fatalf("database failure on %s must preserve refresh cookie", path)
		}
	}
	// Reopening persistence proves the failed logout did not silently revoke.
	f.restart(t)
	response := browserCall(t, client, f.server.URL, "/refresh", "{}", cookie)
	checkBrowserData(t, response, http.StatusOK)
	checkRefreshCookie(t, response, false)
}
