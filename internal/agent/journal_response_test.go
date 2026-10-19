package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJournalTransportPreservesCanonicalRecordStringsAndFloats(t *testing.T) {
	// These valid JSON strings grow up to sixfold through json.Marshal, even for
	// RawMessage. At journal capacity that would make persisted facts unreadable.
	raw := json.RawMessage(`{"version":1,"seq":1,"kind":"created","data":{"n":1.0,"text":"<>&` + "\u2028\u2029" + `"},"prev_hash":"","digest":"` + strings.Repeat("a", 64) + `"}`)
	for _, operation := range []string{"append", "load"} {
		t.Run(operation, func(t *testing.T) {
			handler := journalHTTPHandler(strings.Repeat("b", 64), func(context.Context, string, json.RawMessage) (any, error) {
				if operation == "append" {
					return raw, nil
				}
				return map[string]any{"records": []json.RawMessage{raw}}, nil
			})
			arguments := json.RawMessage(`{}`)
			if operation == "append" {
				arguments = append(append([]byte(`{"record":`), raw...), '}')
			}
			payload := append(append([]byte(`{"operation":"`+operation+`","arguments":`), arguments...), '}')
			request := httptest.NewRequest(http.MethodPost, "/journal", bytes.NewReader(payload))
			request.Header.Set("Authorization", "Bearer "+strings.Repeat("b", 64))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			body, err := io.ReadAll(response.Result().Body)
			if err != nil {
				t.Fatal(err)
			}
			expected := []byte(raw)
			if operation == "load" {
				expected = append(append([]byte(`{"records":[`), raw...), ']', '}')
			}
			if response.Code != http.StatusOK || !bytes.Equal(body, expected) {
				t.Fatalf("journal receipt rewrote canonical bytes: status=%d body=%q expected=%q", response.Code, body, expected)
			}
		})
	}
}

func TestJournalTransportRejectsInvalidRawRecords(t *testing.T) {
	for _, result := range []any{json.RawMessage(`{"invalid":`), map[string]any{"records": []json.RawMessage{json.RawMessage(`not JSON`)}}} {
		if _, err := encodeJournalResult(result); err == nil {
			t.Fatal("invalid raw record accepted")
		}
	}
}
