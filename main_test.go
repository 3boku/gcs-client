package main

import "testing"

func TestParseByteSize(t *testing.T) {
	tests := map[string]int64{
		"1":      1,
		"1b":     1,
		"1KiB":   1024,
		"1kb":    1024,
		"1.5MiB": 1572864,
		"2G":     2 * 1024 * 1024 * 1024,
	}

	for input, want := range tests {
		got, err := parseByteSize(input)
		if err != nil {
			t.Fatalf("parseByteSize(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("parseByteSize(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestParseTransports(t *testing.T) {
	got, err := parseTransports("json,grpc,json")
	if err != nil {
		t.Fatalf("parseTransports: %v", err)
	}
	if len(got) != 2 || got[0] != transportHTTPJSON || got[1] != transportGRPC {
		t.Fatalf("parseTransports returned %#v", got)
	}
}
