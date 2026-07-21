package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"cacheforge/pkg/config"
)

// LogLevelFromString converts string to LogLevel
func LogLevelFromString(level string) LogLevel {
	switch strings.ToLower(level) {
	case "debug":
		return DEBUG
	case "info":
		return INFO
	case "warn", "warning":
		return WARN
	case "error":
		return ERROR
	case "fatal":
		return FATAL
	default:
		return INFO
	}
}

// InitializeFromConfig initializes the global logger from the cacheforge config
func InitializeFromConfig(cfg *config.Config) (*Logger, error) {
	nodeID := cfg.Node.ID
	logCfg := cfg.Logging

	// Ensure log directory exists
	if logCfg.LogDir != "" {
		if err := os.MkdirAll(logCfg.LogDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %v", err)
		}
	}

	// Default log file in the log directory
	logFile := filepath.Join(logCfg.LogDir, fmt.Sprintf("%s.log", nodeID))

	loggerCfg := Config{
		Level:         LogLevelFromString(logCfg.Level),
		NodeID:        nodeID,
		LogFile:       logFile,
		EnableConsole: true,
		EnableFile:    true,
		BufferSize:    1024,
	}

	logger := NewLogger(loggerCfg)
	SetGlobalLogger(logger)

	return logger, nil
}

// ComponentNames for structured logging
const (
	ComponentRESP        = "resp"
	ComponentHTTP        = "http"
	ComponentCluster     = "cluster"
	ComponentGossip      = "gossip"
	ComponentEventBus    = "event_bus"
	ComponentCoordinator = "coordinator"
	ComponentStorage     = "storage"
	ComponentCache       = "cache"
	ComponentPersistence = "persistence"
	ComponentFilter      = "filter"
	ComponentAuth        = "auth"
	ComponentHealth      = "health"
	ComponentConfig      = "config"
	ComponentMain        = "main"
)

// ActionNames for structured logging
const (
	ActionStart       = "start"
	ActionStop        = "stop"
	ActionRequest     = "request"
	ActionResponse    = "response"
	ActionConnect     = "connect"
	ActionDisconnect  = "disconnect"
	ActionJoin        = "join"
	ActionLeave       = "leave"
	ActionReplication = "replication"
	ActionPersist     = "persist"
	ActionRestore     = "restore"
	ActionSnapshot    = "snapshot"
	ActionCompaction  = "compaction"
	ActionElection    = "election"
	ActionConsensus   = "consensus"
	ActionSync        = "sync"
	ActionValidation  = "validation"
	ActionTimeout     = "timeout"
	ActionRetry       = "retry"
	ActionFailover    = "failover"
	ActionBackup      = "backup"
	ActionCleanup     = "cleanup"
)
