package main

import (
	"log/slog"
	"os"

	"xzdp/internal/httpapi"
)

func main() {
	if e := httpapi.Run(true); e != nil {
		slog.Error("service stopped", "error", e)
		os.Exit(1)
	}
}
