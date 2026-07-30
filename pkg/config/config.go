package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for CacheForge.
type Config struct {
	Node        NodeConfig        `yaml:"node"`
	Network     NetworkConfig     `yaml:"network"`
	Cache       CacheConfig       `yaml:"cache"`
	Persistence PersistenceConfig `yaml:"persistence"`
	Logging     LoggingConfig     `yaml:"logging"`
	Stores      []StoreConfig     `yaml:"stores"`
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
	MaxMemory       string  `yaml:"max_memory"`
	DefaultTTL      string  `yaml:"default_ttl"`
	CuckooFilterFPP float64 `yaml:"cuckoo_filter_fpp"`
	MaxStores       int     `yaml:"max_stores"`
}

// PersistenceConfig defines persistence behavior.
type PersistenceConfig struct {
	Enabled          bool          `yaml:"enabled"`
	Strategy         string        `yaml:"strategy"` // "aof", "snapshot", "hybrid"
	EnableAOF        bool          `yaml:"enable_aof"`
	SyncPolicy       string        `yaml:"sync_policy"` // "always", "everysec", "no"
	SyncInterval     time.Duration `yaml:"sync_interval"`
	SnapshotInterval time.Duration `yaml:"snapshot_interval"`
	MaxLogSize       string        `yaml:"max_log_size"`
	CompressionLevel int           `yaml:"compression_level"` // 0-9
	RetainLogs       int           `yaml:"retain_logs"`
}

// LoggingConfig contains logging settings.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	LogDir string `yaml:"log_dir"`
}

// StoreConfig represents configuration for individual stores.
// Store config is immutable after creation — to change, drop and recreate the store.
type StoreConfig struct {
	Name           string `yaml:"name"`
	EvictionPolicy string `yaml:"eviction_policy"`
	MaxMemory      string `yaml:"max_memory"`
	DefaultTTL     string `yaml:"default_ttl"`
	CuckooFilter   *bool  `yaml:"cuckoo_filter,omitempty"` // nil = inherit global (true)
	Persistence    string `yaml:"persistence,omitempty"`   // "hybrid", "aof", "snapshot", "disabled"; empty = inherit global
}

// IsCuckooFilterEnabled returns whether the cuckoo filter is enabled for a store.
// If not explicitly set on the store, returns true (enabled by default).
func (sc *StoreConfig) IsCuckooFilterEnabled() bool {
	if sc.CuckooFilter == nil {
		return true // enabled by default
	}
	return *sc.CuckooFilter
}

// GetPersistence returns the effective persistence strategy for a store.
// If not set on the store, returns the provided global default.
func (sc *StoreConfig) GetPersistence(globalStrategy string) string {
	if sc.Persistence == "" {
		return globalStrategy
	}
	return sc.Persistence
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
			MaxMemory:       "8GB",
			DefaultTTL:      "0",
			CuckooFilterFPP: 0.01, // 1% false positive rate
			MaxStores:       16,
		},
		Persistence: PersistenceConfig{
			Enabled:          true,
			Strategy:         "hybrid",
			EnableAOF:        true,
			SyncPolicy:       "everysec",
			SyncInterval:     1 * time.Second,
			SnapshotInterval: 15 * time.Minute,
			MaxLogSize:       "100MB",
			CompressionLevel: 6,
			RetainLogs:       3,
		},
		Logging: LoggingConfig{
			Level:  "info",
			LogDir: "logs",
		},
		Stores: []StoreConfig{
			{
				Name:           "default",
				EvictionPolicy: "lru",
				MaxMemory:      "8GB",
				DefaultTTL:     "0",
			},
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
