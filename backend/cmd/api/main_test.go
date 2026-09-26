package main

import (
	"slices"
	"testing"
	"time"
)

func TestLimitsFromEnv(t *testing.T) {
	t.Setenv("LAMBARI_SIM_MAX_RATE", "1000")
	t.Setenv("LAMBARI_SIM_MAX_DURATION", "10m")
	t.Setenv("LAMBARI_DISABLE_INGEST", "true")
	t.Setenv("LAMBARI_ALLOWED_ORIGINS", "https://a.vercel.app, https://b.example")

	l, err := limitsFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if l.SimMaxRate != 1000 || l.SimMaxDuration != 10*time.Minute || !l.DisableIngest {
		t.Errorf("limits = %+v", l)
	}
	if !slices.Equal(l.AllowedOrigins, []string{"https://a.vercel.app", "https://b.example"}) {
		t.Errorf("origins = %v", l.AllowedOrigins)
	}
}

func TestLimitsFromEnvRejectsBadValues(t *testing.T) {
	t.Setenv("LAMBARI_SIM_MAX_RATE", "-5")
	if _, err := limitsFromEnv(); err == nil {
		t.Error("negative rate accepted")
	}
}
