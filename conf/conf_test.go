package conf

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOverrides(t *testing.T) {
	file := filepath.Join(t.TempDir(), "conf.yaml")
	if err := os.WriteFile(file, []byte("redis:\n  address: file:6379\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XZDP_CONFIG", file)
	t.Setenv("XZDP_REDIS_ADDRESS", "env:6379")
	c, err := Load()
	if err != nil || c.Redis.Address != "env:6379" || c.Hertz.EnablePprof {
		t.Fatalf("config override: %v", err)
	}
	t.Setenv("XZDP_CONFIG", file+".missing")
	if _, err := Load(); err == nil {
		t.Fatal("missing explicit config accepted")
	}
}
