package config

import "testing"

func minimalConfig() *Config {
	cfg := &Config{}
	cfg.MySQL.Host = "localhost"
	cfg.BigQuery.Project = "proj"
	cfg.Batching.MaxRows = 10
	cfg.CDC.Tables = []struct {
		DB    string `yaml:"db"`
		Table string `yaml:"table"`
	}{
		{DB: "testdb", Table: "users"},
	}
	return cfg
}

func TestValidate_DefaultsAndModeValidation(t *testing.T) {
	cfg := minimalConfig()
	// default mode should be set to "continuous"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validate error: %v", err)
	}
	if cfg.Mode != "continuous" {
		t.Fatalf("expected default mode 'continuous', got '%s'", cfg.Mode)
	}

	// invalid mode should fail
	cfg = minimalConfig()
	cfg.Mode = "bogus"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected invalid mode to fail validation")
	}

	// explicit 'once' should be accepted
	cfg = minimalConfig()
	cfg.Mode = "once"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected mode 'once' to be valid: %v", err)
	}
}
