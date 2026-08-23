package cronx

import (
	"strings"
	"testing"
	"time"
)

func TestLoadZoneResolvesKnownNames(t *testing.T) {
	cases := []struct {
		name string
		want *testing.T // placeholder to keep the table readable
	}{
		{name: "Europe/Oslo"},
		{name: "Asia/Kolkata"},
		{name: "Australia/Lord_Howe"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			z, err := LoadZone(tc.name)
			if err != nil {
				t.Fatalf("LoadZone(%q): %v", tc.name, err)
			}
			if z.String() != tc.name {
				t.Errorf("zone = %q, want %q", z.String(), tc.name)
			}
		})
	}

	t.Run("UTC by name", func(t *testing.T) {
		z, err := LoadZone("UTC")
		if err != nil {
			t.Fatalf("LoadZone(UTC): %v", err)
		}
		if z != time.UTC {
			t.Errorf("LoadZone(UTC) = %v, want the time.UTC singleton", z)
		}
	})

	t.Run("lower case utc is accepted and normalised", func(t *testing.T) {
		z, err := LoadZone("utc")
		if err != nil {
			t.Fatalf("LoadZone(utc): %v", err)
		}
		if z != time.UTC {
			t.Errorf("LoadZone(utc) = %v, want the time.UTC singleton", z)
		}
	})
}

func TestLoadZoneSuggestsCloseNamesForTypos(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		suggests string
	}{
		{"swapped letters", "Europe/Olso", "Europe/Oslo"},
		{"dropped letter", "Europe/Osl", "Europe/Oslo"},
		{"transposed city", "Asia/Kolkatta", "Asia/Kolkata"},
		{"missed capital", "europe/oslo", "Europe/Oslo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadZone(tc.input)
			if err == nil {
				t.Fatalf("LoadZone(%q) succeeded, want an error with a suggestion", tc.input)
			}
			msg := err.Error()
			if !strings.Contains(msg, `did you mean "`+tc.suggests+`"?`) {
				t.Errorf("error = %q, want it to suggest %q", msg, tc.suggests)
			}
		})
	}
}

func TestLoadZoneRejectsWithoutFalseSuggestionsOrHostDependence(t *testing.T) {
	t.Run("garbage gets no suggestion", func(t *testing.T) {
		_, err := LoadZone("ZZZZZZ/NotAZone")
		if err == nil {
			t.Fatal("LoadZone accepted a made up zone")
		}
		if strings.Contains(err.Error(), "did you mean") {
			t.Errorf("error = %q, want no suggestion for garbage input", err.Error())
		}
	})

	t.Run("empty name says what to do instead", func(t *testing.T) {
		_, err := LoadZone("")
		if err == nil {
			t.Fatal("LoadZone(\"\") succeeded")
		}
		if !strings.Contains(err.Error(), "empty time zone name") {
			t.Errorf("error = %q, want it to name the empty input", err.Error())
		}
	})

	t.Run("Local is refused because it depends on the host", func(t *testing.T) {
		for _, name := range []string{"Local", "local"} {
			_, err := LoadZone(name)
			if err == nil {
				t.Fatalf("LoadZone(%q) succeeded: host dependent zones break determinism", name)
			}
			if !strings.Contains(err.Error(), "depends on the host environment") {
				t.Errorf("LoadZone(%q) error = %q, want the host dependence named", name, err.Error())
			}
		}
	})
}

func TestLoadZoneResolvesRealOffsets(t *testing.T) {
	oslo := mustZone(t, "Europe/Oslo")
	if _, off := utc(t, "2026-01-15T12:00:00Z").In(oslo).Zone(); off != 3600 {
		t.Errorf("Oslo winter offset = %d, want 3600", off)
	}
	if _, off := utc(t, "2026-07-15T12:00:00Z").In(oslo).Zone(); off != 7200 {
		t.Errorf("Oslo summer offset = %d, want 7200", off)
	}
	lh := mustZone(t, "Australia/Lord_Howe")
	// Southern hemisphere: DST runs roughly October to April, so January
	// carries the summer offset (+11:00) and July the winter one (+10:30).
	if _, off := utc(t, "2026-07-15T12:00:00Z").In(lh).Zone(); off != (10*3600 + 1800) {
		t.Errorf("Lord Howe winter offset = %d, want 37800", off)
	}
	if _, off := utc(t, "2026-01-15T12:00:00Z").In(lh).Zone(); off != 11*3600 {
		t.Errorf("Lord Howe summer offset = %d, want 39600", off)
	}
}
