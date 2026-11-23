package main

import (
	"context"
	"io"
	"os"
	"testing"
	"time"
)

func TestStreamsCachePostGIS(t *testing.T) {
	if os.Getenv("XZDP_INTEGRATION") != "1" {
		t.Skip("set XZDP_INTEGRATION=1 with DATABASE_URL and REDIS_URL")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	if e := run(ctx, io.Discard); e != nil {
		t.Fatal(e)
	}
}
