// Package auth handles password hashing and opaque, revocable credentials.
package auth

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
)

const iterations = 600000

func HashPassword(password string) (string, error) {
	if len(password) < 12 || len(password) > 256 {
		return "", errors.New("password must contain 12 to 256 bytes")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key, err := pbkdf2.Key(sha256.New, password, salt, iterations, 32)
	if err != nil {
		return "", err
	}
	return "pbkdf2-sha256$" + strconv.Itoa(iterations) + "$" + hex.EncodeToString(salt) + "$" + hex.EncodeToString(key), nil
}

func CheckPassword(password, encoded string) bool {
	if len(password) > 256 {
		return false
	}
	fields := strings.Split(encoded, "$")
	if len(fields) != 4 || fields[0] != "pbkdf2-sha256" {
		return false
	}
	n, err := strconv.Atoi(fields[1])
	if err != nil || n < iterations || n > 1000000 {
		return false
	}
	salt, err := hex.DecodeString(fields[2])
	if err != nil || len(salt) != 16 {
		return false
	}
	expected, err := hex.DecodeString(fields[3])
	if err != nil || len(expected) != 32 {
		return false
	}
	actual, err := pbkdf2.Key(sha256.New, password, salt, n, 32)
	return err == nil && subtle.ConstantTimeCompare(actual, expected) == 1
}

func RandomID() string        { return hex.EncodeToString(random(16)) }
func NewAccessToken() string  { return "mw_at_" + base64.RawURLEncoding.EncodeToString(random(32)) }
func NewRefreshToken() string { return "mw_rt_" + base64.RawURLEncoding.EncodeToString(random(32)) }
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
func ValidToken(token, kind string) bool {
	prefix := "mw_" + kind + "_"
	if !strings.HasPrefix(token, prefix) {
		return false
	}
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, prefix))
	return err == nil && len(b) == 32
}
func random(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}
