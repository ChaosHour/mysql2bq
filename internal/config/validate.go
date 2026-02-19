package config

import "fmt"

func (c *Config) Validate() error {
	if c.MySQL.Host == "" {
		return fmt.Errorf("mysql.host is required")
	}
	if c.BigQuery.Project == "" {
		return fmt.Errorf("bigquery.project is required")
	}
	if len(c.CDC.Tables) == 0 {
		return fmt.Errorf("at least one table must be configured")
	}
	if c.Batching.MaxRows <= 0 {
		return fmt.Errorf("batching.max_rows must be > 0")
	}

	// Normalize/validate run mode
	if c.Mode == "" {
		c.Mode = "continuous"
	} else if c.Mode != "continuous" && c.Mode != "once" {
		return fmt.Errorf("invalid mode: %s (must be 'continuous' or 'once')", c.Mode)
	}

	// Set retry defaults if not configured
	if c.Retry.MaxAttempts <= 0 {
		c.Retry.MaxAttempts = 3
	}
	if c.Retry.InitialDelay == "" {
		c.Retry.InitialDelay = "1s"
	}
	if c.Retry.MaxDelay == "" {
		c.Retry.MaxDelay = "30s"
	}

	// Set HTTP defaults if not configured
	if c.HTTP.Host == "" {
		c.HTTP.Host = "localhost"
	}
	if c.HTTP.Port <= 0 {
		c.HTTP.Port = 8080
	}
	if c.HTTP.Path == "" {
		c.HTTP.Path = "/metrics"
	}

	return nil
}
