package httpapi

import (
	"errors"
	"mime"
	"net"
	"net/http"
	"strings"
	"time"

	"magworkspace/internal/auth"
	"magworkspace/internal/domain"
)

const browserAuthPath = "/api/v1/auth/browser"
const browserRefreshCookie = "magworkspace_refresh"

// Cookie authentication is restricted to the browser's exact origin. The
// explicit header also prevents cross-origin forms from reaching a handler.
// Forwarded headers cannot establish either origin or transport security.
func (s *server) browserAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		origins := r.Header.Values("Origin")
		browser := r.Header.Values("X-Magworkspace-Browser")
		fetchSite := r.Header.Values("Sec-Fetch-Site")
		if r.Host == "" || len(origins) != 1 || origins[0] != scheme+"://"+r.Host ||
			len(browser) != 1 || browser[0] != "1" ||
			len(fetchSite) > 1 || (len(fetchSite) == 1 && fetchSite[0] != "same-origin") {
			problem(w, http.StatusForbidden, "browser_origin_required", "same-origin browser request required")
			return
		}
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			problem(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
			return
		}
		next(w, r)
	}
}

func (s *server) authResponse(w http.ResponseWriter, r *http.Request, status int, u domain.User, spaces []domain.Workspace, a domain.AuthSession, t tokens) {
	data := map[string]any{"user": u, "workspaces": spaces, "token": t.Token, "expires_at": t.ExpiresAt}
	if r.URL.Path == browserAuthPath+"/register" || r.URL.Path == browserAuthPath+"/login" {
		setBrowserRefreshCookie(w, r, t.RefreshToken, a.RefreshExpiresAt)
	} else {
		data["refresh_token"] = t.RefreshToken
	}
	respond(w, status, data)
}

func browserSecureCookie(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	host := r.Host
	if value, _, err := net.SplitHostPort(host); err == nil {
		host = value
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return false
	}
	ip := net.ParseIP(host)
	return ip == nil || !ip.IsLoopback()
}

func setBrowserRefreshCookie(w http.ResponseWriter, r *http.Request, value string, expires time.Time) {
	cookie := &http.Cookie{
		Name: browserRefreshCookie, Value: value, Path: browserAuthPath,
		HttpOnly: true, Secure: browserSecureCookie(r), SameSite: http.SameSiteStrictMode,
		Expires: expires.UTC(),
	}
	if value == "" {
		cookie.MaxAge = -1
		cookie.Expires = time.Unix(1, 0).UTC()
	} else {
		// Round up to avoid a positive subsecond TTL becoming a session cookie.
		cookie.MaxAge = int((time.Until(expires) + time.Second - 1) / time.Second)
		if cookie.MaxAge < 1 {
			cookie.MaxAge = 1
		}
	}
	http.SetCookie(w, cookie)
}

func readBrowserRefreshCookie(r *http.Request) (string, error) {
	var value string
	found := false
	for _, line := range r.Header.Values("Cookie") {
		cookies, err := http.ParseCookie(line)
		if err != nil {
			return "", domain.ErrUnauthorized
		}
		for _, cookie := range cookies {
			if cookie.Name != browserRefreshCookie {
				continue
			}
			if found || !auth.ValidToken(cookie.Value, "rt") {
				return "", domain.ErrUnauthorized
			}
			value, found = cookie.Value, true
		}
	}
	if !found {
		return "", http.ErrNoCookie
	}
	return value, nil
}

func decodeBrowserEmpty(w http.ResponseWriter, r *http.Request) bool {
	// A pointer distinguishes {} from null, which encoding/json otherwise
	// accepts for a struct. decode also rejects fields and trailing values.
	var in *struct{}
	if !decode(w, r, &in) {
		return false
	}
	if in == nil {
		problem(w, http.StatusBadRequest, "invalid_request", "empty JSON object required")
		return false
	}
	return true
}

func (s *server) browserRefresh(w http.ResponseWriter, r *http.Request) {
	if !decodeBrowserEmpty(w, r) {
		return
	}
	refresh, err := readBrowserRefreshCookie(r)
	if err != nil {
		setBrowserRefreshCookie(w, r, "", time.Time{})
		problem(w, http.StatusUnauthorized, "unauthorized", "valid refresh cookie required")
		return
	}
	a, t := s.credentials("")
	if _, err := s.db.RotateAuth(r.Context(), auth.HashToken(refresh), a); err != nil {
		if errors.Is(err, domain.ErrUnauthorized) {
			setBrowserRefreshCookie(w, r, "", time.Time{})
		}
		fail(w, err)
		return
	}
	setBrowserRefreshCookie(w, r, t.RefreshToken, a.RefreshExpiresAt)
	respond(w, http.StatusOK, map[string]any{"token": t.Token, "expires_at": t.ExpiresAt})
}

func (s *server) browserLogout(w http.ResponseWriter, r *http.Request) {
	if !decodeBrowserEmpty(w, r) {
		return
	}
	refresh, err := readBrowserRefreshCookie(r)
	if err != nil && !errors.Is(err, http.ErrNoCookie) {
		setBrowserRefreshCookie(w, r, "", time.Time{})
		problem(w, http.StatusUnauthorized, "unauthorized", "valid refresh cookie required")
		return
	}
	if refresh != "" {
		if err := s.db.DeleteAuthByRefresh(r.Context(), auth.HashToken(refresh)); err != nil {
			fail(w, err)
			return
		}
	}
	setBrowserRefreshCookie(w, r, "", time.Time{})
	w.WriteHeader(http.StatusNoContent)
}
