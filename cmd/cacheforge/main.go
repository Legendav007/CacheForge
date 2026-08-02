package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cacheforge/internal/logging"
	"cacheforge/internal/metrics"
	"cacheforge/internal/storage"
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

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.Node.DataDir, 0755); err != nil {
		logging.Fatal(ctx, logging.ComponentMain, logging.ActionStart, "Failed to create data directory", err)
		os.Exit(1)
	}

	// Create StoreManager to manage multiple named stores
	storeManager := storage.NewStoreManager(storage.StoreManagerConfig{
		DataDir:           cfg.Node.DataDir,
		MaxStores:         cfg.Cache.MaxStores,
		GlobalPersistence: cfg.Persistence,
		GlobalCacheConfig: cfg.Cache,
	})
	defer storeManager.Close()

	// Create stores from config (YAML-defined)
	for _, storeCfg := range cfg.Stores {
		if err := storeManager.CreateStore(storeCfg, ctx); err != nil {
			logging.Fatal(ctx, logging.ComponentMain, logging.ActionStart, "Failed to create store", err, map[string]interface{}{"store": storeCfg.Name})
			os.Exit(1)
		}
	}

	// Load runtime-created stores from stores.json
	if err := storeManager.LoadRegistry(ctx); err != nil {
		logging.Warn(ctx, logging.ComponentStorage, logging.ActionRestore, "Failed to load store registry", map[string]interface{}{"error": err.Error()})
	}

	// Save registry so any config-defined stores are also tracked
	storeManager.SaveRegistry()

	logging.Info(ctx, logging.ComponentMain, logging.ActionStart, "Stores initialized", map[string]interface{}{
		"total_stores": storeManager.StoreCount(),
		"stores":       storeManager.ListStores(),
	})

	// Get default store for backward-compatible endpoints
	defaultStore := storeManager.GetDefaultStore()

	// Build HTTP mux with cache + metrics endpoints
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "CacheForge node %s\n", cfg.Node.ID)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"healthy": true,
			"node":    cfg.Node.ID,
		})
	})
	mux.Handle("/api/cache/", logging.HTTPMiddleware(http.HandlerFunc(handleCacheRequest(defaultStore, cfg.Node.ID))))
	mux.HandleFunc("/api/stores", func(w http.ResponseWriter, r *http.Request) {
		handleStoreRequest(w, r, storeManager, cfg.Node.ID)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		handleMetrics(w, defaultStore, cfg.Node.ID)
	})

	addr := fmt.Sprintf("%s:%d", cfg.Network.BindAddr, cfg.Network.Port)
	logging.Info(ctx, logging.ComponentMain, logging.ActionStart,
		fmt.Sprintf("listening on %s", addr))

	server := &http.Server{
		Addr:    addr,
		Handler: logging.CorrelationIDMiddleware(mux),
	}

	// Graceful shutdown on interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		logging.Info(ctx, logging.ComponentMain, logging.ActionStop, "Shutting down CacheForge node")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logging.Error(ctx, logging.ComponentMain, logging.ActionStop, "server stopped", err)
		os.Exit(1)
	}
}

// handleCacheRequest handles GET/PUT/DELETE on /api/cache/{key}
func handleCacheRequest(store *storage.BasicStore, nodeID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/api/cache/")
		if key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			value, err := store.Get(key)
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false, "error": "Key not found", "key": key, "node": nodeID,
				})
				return
			}
			if b, ok := value.([]byte); ok {
				value = string(b)
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"data":    map[string]interface{}{"key": key, "value": value},
				"node":    nodeID,
			})

		case http.MethodPut:
			var body struct {
				Value interface{} `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "Invalid JSON body", http.StatusBadRequest)
				return
			}
			if err := store.Set(key, body.Value, "http-api", time.Hour); err != nil {
				http.Error(w, fmt.Sprintf("Failed to set key: %v", err), http.StatusInternalServerError)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"message": "Key set successfully",
				"data":    map[string]interface{}{"key": key, "value": body.Value},
				"node":    nodeID,
			})

		case http.MethodDelete:
			err := store.Delete(key)
			existed := err == nil
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true, "existed": existed, "key": key, "node": nodeID,
			})

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// handleStoreRequest handles store management endpoints
func handleStoreRequest(w http.ResponseWriter, r *http.Request, storeManager *storage.StoreManager, nodeID string) {
	w.Header().Set("Content-Type", "application/json")

	path := strings.TrimPrefix(r.URL.Path, "/api/stores")
	if path == "" || path == "/" {
		switch r.Method {
		case http.MethodGet:
			stores := storeManager.ListStores()
			json.NewEncoder(w).Encode(map[string]interface{}{
				"stores": stores, "total_count": len(stores), "node": nodeID,
			})
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	// Store-scoped: /api/stores/{name}
	name := strings.TrimPrefix(path, "/")
	if name == "" {
		http.Error(w, `{"error":"store name required"}`, http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s := storeManager.GetStore(name)
		if s == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": fmt.Sprintf("store '%s' not found", name)})
			return
		}
		stats := s.Stats()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"store": name,
			"stats": map[string]interface{}{
				"total_items":  stats.TotalItems,
				"total_memory": stats.TotalMemory,
				"hit_count":    stats.HitCount,
				"miss_count":   stats.MissCount,
				"hit_rate":     stats.HitRate(),
			},
			"node": nodeID,
		})
	case http.MethodDelete:
		if err := storeManager.DropStore(name); err != nil {
			status := http.StatusInternalServerError
			if strings.Contains(err.Error(), "not found") {
				status = http.StatusNotFound
			} else if strings.Contains(err.Error(), "cannot drop") {
				status = http.StatusForbidden
			}
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "message": fmt.Sprintf("Store '%s' dropped", name)})
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleMetrics writes Prometheus-compatible metrics
func handleMetrics(w http.ResponseWriter, store *storage.BasicStore, nodeID string) {
	stats := store.Stats()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	var b strings.Builder
	fmt.Fprintf(&b, "# HELP cacheforge_items_total Total number of items in the cache\n")
	fmt.Fprintf(&b, "# TYPE cacheforge_items_total gauge\n")
	fmt.Fprintf(&b, "cacheforge_items_total{node=\"%s\"} %d\n", nodeID, stats.TotalItems)

	fmt.Fprintf(&b, "# HELP cacheforge_memory_bytes Current memory usage in bytes\n")
	fmt.Fprintf(&b, "# TYPE cacheforge_memory_bytes gauge\n")
	fmt.Fprintf(&b, "cacheforge_memory_bytes{node=\"%s\"} %d\n", nodeID, stats.TotalMemory)

	fmt.Fprintf(&b, "# HELP cacheforge_hits_total Total cache hits\n")
	fmt.Fprintf(&b, "# TYPE cacheforge_hits_total counter\n")
	fmt.Fprintf(&b, "cacheforge_hits_total{node=\"%s\"} %d\n", nodeID, stats.HitCount)

	fmt.Fprintf(&b, "# HELP cacheforge_misses_total Total cache misses\n")
	fmt.Fprintf(&b, "# TYPE cacheforge_misses_total counter\n")
	fmt.Fprintf(&b, "cacheforge_misses_total{node=\"%s\"} %d\n", nodeID, stats.MissCount)

	fmt.Fprintf(&b, "# HELP cacheforge_hit_rate Cache hit rate percentage\n")
	fmt.Fprintf(&b, "# TYPE cacheforge_hit_rate gauge\n")
	fmt.Fprintf(&b, "cacheforge_hit_rate{node=\"%s\"} %.2f\n", nodeID, stats.HitRate())

	// Latency histograms and operation counters from metrics collector
	metrics.Global().WritePrometheus(&b, nodeID)

	w.Write([]byte(b.String()))
}
