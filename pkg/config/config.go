package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for CacheForge.
// We'll expand this as the project grows.
type Config struct {
	Node    NodeConfig    `yaml:"node"`
	Network NetworkConfig `yaml:"network"`
	Cache   CacheConfig   `yaml:"cache"`
	Logging LoggingConfig `yaml:"logging"`
}

// NodeConfig contains node-specific settings.
type NodeConfig struct {
	ID      string `yaml:"id"`
	DataDir string `yaml:"data_dir"`
}

// NetworkConfig contains network/listen settings.
type NetworkConfig struct {
	BindAddr string `yaml:"bind_addr"`
	Port     int    `yaml:"port"`
}

// CacheConfig contains global cache settings.
type CacheConfig struct {
	MaxMemory  string `yaml:"max_memory"`
	DefaultTTL string `yaml:"default_ttl"`
}

// LoggingConfig contains logging settings.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	LogDir string `yaml:"log_dir"`
}

// Load reads and parses the configuration file, falling back to defaults
// if the file does not exist.
func Load(path string) (*Config, error) {
	config := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("⚠️  Configuration file %s not found, using defaults\n", path)
			return config, nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}

// Default returns a Config populated with sensible defaults.
func Default() *Config {
	return &Config{
		Node: NodeConfig{
			ID:      "cacheforge-node-1",
			DataDir: "/tmp/cacheforge",
		},
		Network: NetworkConfig{
			BindAddr: "0.0.0.0",
			Port:     8080,
		},
		Cache: CacheConfig{
			MaxMemory:  "8GB",
			DefaultTTL: "0",
		},
		Logging: LoggingConfig{
			Level:  "info",
			LogDir: "logs",
		},
	}
}

// Validate checks whether the configuration is valid.
func (c *Config) Validate() error {
	if c.Node.ID == "" {
		return fmt.Errorf("node.id cannot be empty")
	}
	if c.Network.Port <= 0 || c.Network.Port > 65535 {
		return fmt.Errorf("network.port must be between 1 and 65535")
	}
	return nil
}
