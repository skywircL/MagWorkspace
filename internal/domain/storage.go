package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strings"
)

// StorageBindingID identifies a configured namespace independently of credentials
// and indexing/model options. Callers validate endpoints before constructing a
// client. Persist this identity when writing; never infer it at deletion time.
func StorageBindingID(kind, endpoint, namespace string) string {
	if u, err := url.Parse(endpoint); err == nil {
		u.Scheme = strings.ToLower(u.Scheme)
		// DNS names and IPv6 hex are case-insensitive; an IPv6 zone names a
		// case-sensitive interface. Preserve the zone used by the actual dialer.
		if zone := strings.IndexByte(u.Host, '%'); zone >= 0 {
			u.Host = strings.ToLower(u.Host[:zone]) + u.Host[zone:]
		} else {
			u.Host = strings.ToLower(u.Host)
		}
		if (u.Scheme == "http" && u.Port() == "80") || (u.Scheme == "https" && u.Port() == "443") {
			u.Host = u.Hostname()
			if strings.Contains(u.Host, ":") {
				u.Host = "[" + u.Host + "]"
			}
		}
		u.Path = strings.TrimRight(u.Path, "/")
		// Match the provider adapters: HTTP clients rebuild the escaped path
		// from Path, and MinIO uses only the endpoint host (no empty query).
		u.RawPath = ""
		u.ForceQuery = false
		endpoint = u.String()
	}
	data, _ := json.Marshal([]string{kind, endpoint, namespace})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
