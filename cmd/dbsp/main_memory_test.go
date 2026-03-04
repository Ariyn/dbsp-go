package main

import "testing"

func TestParseHumanBytes(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "empty", input: "", want: 0},
		{name: "bytes", input: "1024", want: 1024},
		{name: "kib", input: "1KiB", want: 1 << 10},
		{name: "mib", input: "512MiB", want: 512 << 20},
		{name: "gib", input: "1GiB", want: 1 << 30},
		{name: "fractional", input: "1.5GiB", want: 1610612736},
		{name: "kb", input: "2KB", want: 2048},
		{name: "invalid unit", input: "1ZB", wantErr: true},
		{name: "negative", input: "-1MiB", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseHumanBytes(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseHumanBytes(%q) error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("parseHumanBytes(%q)=%d, want %d", tc.input, got, tc.want)
			}
		})
	}
}
