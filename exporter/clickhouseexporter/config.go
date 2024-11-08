package clickhouseexporter

import (
	"net/url"
)

// Config defines configuration for ClickHouse exporter
type Config struct {
	Endpoint         string `mapstructure:"endpoint"`
	Database         string `mapstructure:"database"`
	Username         string `mapstructure:"username"`
	Password         string `mapstructure:"password"`
	MetricsTableName string `mapstructure:"metrics_table_name"`
}

const defaultDatabase = "default"

func (cfg *Config) buildDSN() (string, error) {
	dsnURL, _ := url.Parse(cfg.Endpoint)

	queryParams := dsnURL.Query()

	if dsnURL.Scheme == "https" {
		queryParams.Set("secure", "true")
	}

	if dsnURL.Path == "" || cfg.Database != defaultDatabase {
		dsnURL.Path = cfg.Database
	}

	if cfg.Username != "" {
		dsnURL.User = url.UserPassword(cfg.Username, string(cfg.Password))
	}

	dsnURL.RawQuery = queryParams.Encode()

	return dsnURL.String(), nil
}
