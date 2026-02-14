package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MySQL struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		ServerID uint32 `yaml:"server_id"`
	} `yaml:"mysql"`

	BigQuery struct {
		Project               string `yaml:"project"`
		Dataset               string `yaml:"dataset"`
		DatasetLocation       string `yaml:"dataset_location,omitempty"`
		ServiceAccountKeyJSON string `yaml:"service_account_key_json,omitempty"`
		Endpoint              string `yaml:"endpoint,omitempty"`
		ClientProjectID       string `yaml:"client_project_id,omitempty"`
	} `yaml:"bigquery"`

	CDC struct {
		Tables []struct {
			DB    string `yaml:"db"`
			Table string `yaml:"table"`
		} `yaml:"tables"`
	} `yaml:"cdc"`

	Batching struct {
		MaxRows  int    `yaml:"max_rows"`
		MaxDelay string `yaml:"max_delay"`
		MaxBytes int    `yaml:"max_bytes,omitempty"`
		Timeout  string `yaml:"timeout,omitempty"`
	} `yaml:"batching"`

	Checkpoint struct {
		Type string `yaml:"type"`
		Path string `yaml:"path"`
	} `yaml:"checkpoint"`

	Logging struct {
		Level string `yaml:"level,omitempty"`
		File  string `yaml:"file,omitempty"`
	} `yaml:"logging,omitempty"`

	TimePartitioning           string `yaml:"time_partitioning,omitempty"`
	TimePartitioningExpiration string `yaml:"time_partitioning_expiration,omitempty"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
