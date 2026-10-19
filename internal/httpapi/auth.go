package httpapi

import (
	"errors"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"magworkspace/internal/auth"
	"magworkspace/internal/domain"
)

func normalizeEmail(value string) (string, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	a, err := mail.ParseAddress(value)
	return value, err == nil && a.Address == value && len(value) <= 254
}

type tokens struct {
	Token        string    `json:"token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

func (s *server) credentials(user string) (domain.AuthSession, tokens) {
	now := time.Now().UTC()
	t := tokens{auth.NewAccessToken(), auth.NewRefreshToken(), now.Add(s.cfg.AccessTTL)}
	a := domain.AuthSession{ID: auth.RandomID(), UserID: user, AccessHash: auth.HashToken(t.Token), RefreshHash: auth.HashToken(t.RefreshToken), AccessExpiresAt: t.ExpiresAt, RefreshExpiresAt: now.Add(s.cfg.RefreshTTL)}
	return a, t
}
func (s *server) register(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.AllowRegistration {
		problem(w, 403, "registration_disabled", "registration disabled")
		return
	}
	var in struct {
		Name     string `json:"name"`
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if !decode(w, r, &in) {
		return
	}
	email, ok := normalizeEmail(in.Email)
	in.Name = strings.TrimSpace(in.Name)
	if !ok || in.Name == "" || len(in.Name) > 100 || len(in.Password) < 12 || len(in.Password) > 256 {
		problem(w, 400, "invalid_request", "name, email, and password of 12..256 bytes required")
		return
	}
	hash, err := auth.HashPassword(in.Password)
	if err != nil {
		fail(w, err)
		return
	}
	u, ws, err := s.db.CreateUser(r.Context(), in.Name, email, hash)
	if err != nil {
		fail(w, err)
		return
	}
	a, t := s.credentials(u.ID)
	if err = s.db.CreateAuth(r.Context(), a); err != nil {
		fail(w, err)
		return
	}
	s.authResponse(w, r, 201, u, []domain.Workspace{ws}, a, t)
}
func (s *server) login(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if !decode(w, r, &in) {
		return
	}
	email, ok := normalizeEmail(in.Email)
	if !ok || len(in.Password) > 256 {
		problem(w, 401, "unauthorized", "invalid email or password")
		return
	}
	u, hash, err := s.db.UserByEmail(r.Context(), email)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		fail(w, err)
		return
	}
	if errors.Is(err, domain.ErrNotFound) {
		hash = "pbkdf2-sha256$600000$00000000000000000000000000000000$0000000000000000000000000000000000000000000000000000000000000000"
	}
	if !auth.CheckPassword(in.Password, hash) || err != nil {
		problem(w, 401, "unauthorized", "invalid email or password")
		return
	}
	spaces, err := s.db.Workspaces(r.Context(), u.ID)
	if err != nil {
		fail(w, err)
		return
	}
	a, t := s.credentials(u.ID)
	if err = s.db.CreateAuth(r.Context(), a); err != nil {
		fail(w, err)
		return
	}
	s.authResponse(w, r, 200, u, spaces, a, t)
}
func (s *server) refresh(w http.ResponseWriter, r *http.Request) {
	var in struct {
		RefreshToken string `json:"refresh_token"`
	}
	if !decode(w, r, &in) {
		return
	}
	if !auth.ValidToken(in.RefreshToken, "rt") {
		problem(w, 401, "unauthorized", "valid refresh token required")
		return
	}
	a, t := s.credentials("")
	if _, err := s.db.RotateAuth(r.Context(), auth.HashToken(in.RefreshToken), a); err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, t)
}
func (s *server) logout(w http.ResponseWriter, r *http.Request) {
	if err := s.db.DeleteAuth(r.Context(), identity(r).ID); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}
func (s *server) me(w http.ResponseWriter, r *http.Request) {
	u, err := s.db.User(r.Context(), userID(r))
	if err != nil {
		fail(w, err)
		return
	}
	spaces, err := s.db.Workspaces(r.Context(), u.ID)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, map[string]any{"user": u, "workspaces": spaces})
}
