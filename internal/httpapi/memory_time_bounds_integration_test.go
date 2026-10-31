package httpapi_test

import (
	"reflect"
	"testing"

	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
)

func TestHTTPMemoryTimeBoundsPreserveExistingRecord(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "memory-time-bounds")
	path := memoryPath(owner.Workspaces[0].ID)
	input := func(expiry any) map[string]any {
		return map[string]any{"kind": "fact", "content": "A dated personal reminder", "expires_at": expiry}
	}
	original := integrationData[domain.Memory](t, f.request(t, "POST", path, owner.Token, input(nil), 201))
	for _, expiry := range []float64{1e300, 253402300800, 0, -1} {
		f.request(t, "POST", path, owner.Token, input(expiry), 400)
		f.request(t, "PUT", path+"/"+original.ID, owner.Token, input(expiry), 400)
	}
	unchanged := integrationData[[]domain.Memory](t, f.request(t, "GET", path+"?status=all", owner.Token, nil, 200))
	if len(unchanged) != 1 || !reflect.DeepEqual(unchanged[0], original) {
		t.Fatal("invalid timestamp inserted a row or superseded the existing memory")
	}
	const lastSupportedSecond = float64(253402300799)
	updated := integrationData[domain.Memory](t, f.request(t, "PUT", path+"/"+original.ID, owner.Token, input(lastSupportedSecond), 200))
	if updated.ExpiresAt == nil || *updated.ExpiresAt != lastSupportedSecond {
		t.Fatal("valid UTC boundary did not round trip as Unix seconds")
	}
	f.restart(t)
	loaded := integrationData[[]domain.Memory](t, f.request(t, "GET", path, owner.Token, nil, 200))
	if len(loaded) != 1 || !reflect.DeepEqual(loaded[0], updated) {
		t.Fatal("restart changed memory time or replacement")
	}
}
