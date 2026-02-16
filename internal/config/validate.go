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
	return nil
}
