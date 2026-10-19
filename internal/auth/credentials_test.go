package auth

import "testing"

func TestPasswordAndOpaqueTokens(t *testing.T) {
	hash, err := HashPassword("long-correct-password")
	if err != nil {
		t.Fatal(err)
	}
	if !CheckPassword("long-correct-password", hash) || CheckPassword("wrong-password", hash) {
		t.Fatal("password verification")
	}
	for _, invalid := range []string{"", "pbkdf2-sha256$999999999$00$00", "other$600000$00$00"} {
		if CheckPassword("x", invalid) {
			t.Fatal("accepted malformed hash")
		}
	}
	if _, err = HashPassword("short"); err == nil {
		t.Fatal("short password accepted")
	}
	a, b := NewAccessToken(), NewRefreshToken()
	if a == NewAccessToken() || !ValidToken(a, "at") || ValidToken(a, "rt") || !ValidToken(b, "rt") || ValidToken("mw_at_abc", "at") {
		t.Fatal("token separation/randomness")
	}
	if len(HashToken(a)) != 64 || HashToken(a) == HashToken(b) {
		t.Fatal("token hash")
	}
}
