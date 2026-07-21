package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"cacheforge/internal/logging"
	"cacheforge/pkg/config"
)

func main() {
	// Determine config path from flag or env, fall back to default
	configPath := os.Getenv("CACHEFORGE_CONFIG")
	if configPath == "" {
		configPath = "config.yaml"
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Initialize the structured logger from config
	logger, err := logging.InitializeFromConfig(cfg)
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer logger.Close()

	ctx := context.Background()
	logging.Info(ctx, logging.ComponentMain, logging.ActionStart,
		fmt.Sprintf("CacheForge node %s starting", cfg.Node.ID))

	// Basic HTTP handler wrapped with the logging middleware
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "CacheForge node %s\n", cfg.Node.ID)
	})

	addr := fmt.Sprintf("%s:%d", cfg.Network.BindAddr, cfg.Network.Port)
	logging.Info(ctx, logging.ComponentMain, logging.ActionStart,
		fmt.Sprintf("listening on %s", addr))

	if err := http.ListenAndServe(addr, logging.HTTPMiddleware(mux)); err != nil {
		logging.Error(ctx, logging.ComponentMain, logging.ActionStop,
			"server stopped", err)
		os.Exit(1)
	}
}
