package domain

import "testing"

func TestStorageBindingIdentity(t *testing.T) {
	base := StorageBindingID("minio", "http://EXAMPLE.test:80/", "documents")
	if base != StorageBindingID("minio", "http://example.test", "documents") {
		t.Fatal("equivalent endpoints have different identities")
	}
	for _, args := range [][3]string{
		{"minio", "https://example.test", "documents"},
		{"minio", "http://example.test:9000", "documents"},
		{"minio", "http://example.test", "other"},
		{"qdrant", "http://example.test", "documents"},
	} {
		if StorageBindingID(args[0], args[1], args[2]) == base {
			t.Fatal("different storage namespaces collided", args)
		}
	}
	if StorageBindingID("qdrant", "http://[::1]:80/", "mw_") != StorageBindingID("qdrant", "http://[::1]", "mw_") {
		t.Fatal("IPv6 normalization changed target")
	}
	for _, endpoint := range []string{"http://example.test/proxy%2Fapi", "http://example.test/%70roxy/api/"} {
		if StorageBindingID("qdrant", endpoint, "mw_") != StorageBindingID("qdrant", "http://example.test/proxy/api", "mw_") {
			t.Fatal("binding differs from the provider's normalized request path")
		}
	}
	if StorageBindingID("minio", "http://example.test?", "documents") != base {
		t.Fatal("empty query ignored by object client changed storage binding")
	}
}

func TestStorageBindingPreservesIPv6Zone(t *testing.T) {
	for _, tc := range []struct {
		name, scheme, port, canonicalPort string
	}{
		{"http_no_port", "http", "", ""},
		{"http_default_port", "http", ":80", ""},
		{"http_nondefault_port", "http", ":6333", ":6333"},
		{"https_no_port", "https", "", ""},
		{"https_default_port", "https", ":443", ""},
		{"https_nondefault_port", "https", ":8443", ":8443"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			endpoint := tc.scheme + "://[FE80::AB%25StoreA]" + tc.port + "/"
			canonical := tc.scheme + "://[fe80::ab%25StoreA]" + tc.canonicalPort
			binding := StorageBindingID("qdrant", endpoint, "mw_")
			if binding != StorageBindingID("qdrant", canonical, "mw_") {
				t.Fatal("IP spelling or default port changed the same scoped target")
			}
			for _, host := range []string{"[fe80::ab%25storea]", "[fe80::ab]"} {
				other := tc.scheme + "://" + host + tc.canonicalPort
				if binding == StorageBindingID("qdrant", other, "mw_") {
					t.Fatal("distinct IPv6 interfaces share a storage binding")
				}
			}
			if tc.canonicalPort != "" && binding == StorageBindingID("qdrant", tc.scheme+"://[fe80::ab%25StoreA]", "mw_") {
				t.Fatal("nondefault port lost its storage identity")
			}
		})
	}
}
