package main

import (
	"context"
	"io"
	"os"
	"testing"
	"time"
)

func TestKafkaCachePostGIS(t *testing.T) {
	if os.Getenv("XZDP_INTEGRATION") != "1" {
		t.Skip("set XZDP_INTEGRATION=1 with DATABASE_URL , REDIS_URL and KAFKA_BROKERS")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	if e := run(ctx, io.Discard); e != nil {
		t.Fatal(e)
	}
}
