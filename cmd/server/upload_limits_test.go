package main

import "testing"

func TestConfigureUploadLimit(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{
		{"", 50}, {"1", 1}, {"80", 80}, {"512", 512}, {"0", 0}, {"-1", 0}, {"513", 0}, {"1.5", 0}, {"abc", 0}, {"9999999999999999999999", 0},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("MAX_FILE_SIZE_MB", tc.value)
			got, err := configureUploadLimit()
			if tc.want == 0 {
				if err == nil {
					t.Fatal("invalid policy accepted")
				}
				return
			}
			if err != nil || got != tc.want*(1<<20) {
				t.Fatalf("got %d, %v", got, err)
			}
		})
	}
}
