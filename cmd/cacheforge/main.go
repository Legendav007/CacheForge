package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cacheforge/internal/cluster"
	"cacheforge/internal/logging"
	"cacheforge/internal/metrics"
	"cacheforge/internal/network"
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

	// Create distributed coordinator with configuration-driven clustering
	resolvedSeeds := resolveSeeds(ctx, cfg.Cluster.Seeds, cfg.Cluster.SeedDNS, cfg.Cluster.SeedDNSPort)

	clusterConfig := cluster.ClusterConfig{
		NodeID:                  cfg.Node.ID,
		ClusterName:             "cacheforge",
		BindAddress:             cfg.Network.BindAddr,
		BindPort:                cfg.Network.Port + 100, // gossip port offset from HTTP
		AdvertiseAddress:        cfg.Network.BindAddr,
		HTTPPort:                cfg.Network.Port,
		SeedNodes:               resolvedSeeds,
		HashRing:                cluster.DefaultHashRingConfig(),
		JoinTimeout:             30,
		HeartbeatInterval:       5,
		FailureDetectionTimeout: 15,
	}

	coord, err := cluster.NewDistributedCoordinator(clusterConfig)
	if err != nil {
		logging.Fatal(ctx, logging.ComponentMain, logging.ActionStart, "Failed to create distributed coordinator", err)
		os.Exit(1)
	}

	// Start coordinator (this handles clustering, replication, and gossip)
	if err := coord.Start(ctx); err != nil {
		logging.Fatal(ctx, logging.ComponentMain, logging.ActionStart, "Failed to start coordinator", err)
		os.Exit(1)
	}

	// Subscribe to replication events
	if eventBus := coord.GetEventBus(); eventBus != nil {
		eventsChan := eventBus.Subscribe(cluster.EventDataOperation)
		go func() {
			for {
				select {
				case event := <-eventsChan:
					handleReplicationEvent(ctx, event, storeManager, cfg.Node.ID, coord)
				case <-ctx.Done():
					return
				}
			}
		}()
	} else {
		logging.Warn(ctx, logging.ComponentEventBus, logging.ActionStart, "Event bus not available - replication disabled")
	}

	// Create node communicator for hash-ring routing & replication
	nodeCommunicator := cluster.NewNodeCommunicator(cfg.Node.ID, coord.GetMembership())

	// Create distributed-aware RESP server using configured address
	respBindAddr := fmt.Sprintf("%s:%d", cfg.Network.BindAddr, cfg.Network.Port+1000)
	respServer := network.NewServer(respBindAddr, defaultStore, coord)
	respServer.SetStoreManager(storeManager)
	respServer.SetNodeCommunicator(nodeCommunicator)
	respServer.SetConsistencyLevel(cfg.Cluster.ConsistencyLevel)

	// Start RESP server
	go func() {
		logging.Info(ctx, logging.ComponentRESP, logging.ActionStart, "RESP server listening", map[string]interface{}{"bind_addr": respBindAddr})
		if err := respServer.Start(); err != nil {
			logging.Error(ctx, logging.ComponentRESP, logging.ActionStart, "RESP server error", err, nil)
		}
	}()

	// Build HTTP mux with cache + metrics + cluster endpoints
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "CacheForge node %s\n", cfg.Node.ID)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		correlationID := logging.GetCorrelationID(r.Context())
		if correlationID == "" {
			correlationID = logging.NewCorrelationID()
			r = r.WithContext(logging.WithCorrelationID(r.Context(), correlationID))
		}
		health := coord.GetHealth()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Correlation-ID", correlationID)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"healthy":        health.Healthy,
			"node":           cfg.Node.ID,
			"cluster_size":   health.ClusterSize,
			"correlation_id": correlationID,
		})
	})
	mux.HandleFunc("/api/cluster/members", func(w http.ResponseWriter, r *http.Request) {
		membership := coord.GetMembership()
		if membership == nil {
			http.Error(w, "Cluster membership not available", http.StatusServiceUnavailable)
			return
		}
		members := membership.GetMembers()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"members":     members,
			"total_count": len(members),
			"node":        cfg.Node.ID,
		})
	})

	// Create read-repairer for cross-node GET during gossip propagation window
	readRepairer := cluster.NewReadRepairer(coord)

	// Internal endpoint: peer GET for read-repair (called by other nodes)
	mux.HandleFunc("/internal/get/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/internal/get/")
		if key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}
		value, err := defaultStore.Get(key)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if b, ok := value.([]byte); ok {
			value = string(b)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"value": value})
	})

	// Internal endpoint: receive direct replication from hash-ring owner
	mux.HandleFunc("/internal/replicate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload struct {
			Key       string      `json:"key"`
			Value     interface{} `json:"value"`
			TTL       float64     `json:"ttl"`
			LamportTS uint64      `json:"lamport_ts"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if payload.Key == "" {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}
		if coord.GetClock() != nil && payload.LamportTS > 0 {
			coord.GetClock().Witness(payload.LamportTS)
		}
		if payload.Value == nil {
			_ = defaultStore.DeleteWithTimestamp(payload.Key, payload.LamportTS)
		} else {
			ttl := time.Duration(payload.TTL) * time.Second
			_, _ = defaultStore.SetWithTimestamp(r.Context(), payload.Key, payload.Value, "replication", ttl, payload.LamportTS)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
	})

	// Cache operations with middleware
	mux.Handle("/api/cache/", logging.HTTPMiddleware(http.HandlerFunc(handleCacheRequest(coord, defaultStore, cfg.Node.ID, readRepairer, nodeCommunicator, cfg.Cluster.ConsistencyLevel))))

	mux.HandleFunc("/api/stores", func(w http.ResponseWriter, r *http.Request) {
		handleStoreRequest(w, r, storeManager, coord, cfg.Node.ID)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		handleMetrics(w, defaultStore, coord, cfg.Node.ID)
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
		_ = coord.Stop(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logging.Error(ctx, logging.ComponentMain, logging.ActionStop, "server stopped", err)
		os.Exit(1)
	}
}

// handleCacheRequest handles GET/PUT/DELETE on /api/cache/{key}
func handleCacheRequest(coordinator cluster.CoordinatorService, store *storage.BasicStore, nodeID string, readRepairer *cluster.ReadRepairer, nodeCommunicator *cluster.NodeCommunicator, consistencyLevel string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract key from URL path
		path := strings.TrimPrefix(r.URL.Path, "/api/cache/")
		if path == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}
		key := path

		w.Header().Set("Content-Type", "application/json")

		// Hash-ring routing: check if this node owns the key
		isProxied := r.Header.Get("X-CacheForge-Proxied") == "true"
		if !isProxied && coordinator != nil && coordinator.GetRouting() != nil && nodeCommunicator != nil {
			routing := coordinator.GetRouting()
			if !routing.IsLocal(key) && !routing.IsReplica(key) {
				ownerNode := routing.RouteKey(key)
				if ownerNode != "" {
					switch r.Method {
					case http.MethodGet:
						value, found, err := nodeCommunicator.ProxyGet(r.Context(), ownerNode, key)
						if err != nil || !found {
							w.WriteHeader(http.StatusNotFound)
							json.NewEncoder(w).Encode(map[string]interface{}{
								"success": false, "error": "Key not found", "key": key, "node": nodeID,
								"routed_to": ownerNode, "correlation_id": logging.GetCorrelationID(r.Context()),
							})
							return
						}
						json.NewEncoder(w).Encode(map[string]interface{}{
							"success": true,
							"data":    map[string]interface{}{"key": key, "value": value},
							"node":    nodeID, "routed_to": ownerNode, "local": false,
							"correlation_id": logging.GetCorrelationID(r.Context()),
						})
						return

					case http.MethodPut:
						var requestBody struct {
							Value interface{} `json:"value"`
						}
						if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
							http.Error(w, "Invalid JSON body", http.StatusBadRequest)
							return
						}
						err := nodeCommunicator.ProxySet(r.Context(), ownerNode, key, requestBody.Value, 3600)
						if err != nil {
							http.Error(w, fmt.Sprintf("Failed to route SET: %v", err), http.StatusBadGateway)
							return
						}
						json.NewEncoder(w).Encode(map[string]interface{}{
							"success": true, "message": "Key set successfully",
							"data": map[string]interface{}{"key": key, "value": requestBody.Value},
							"node": nodeID, "routed_to": ownerNode, "replicated": true,
							"correlation_id": logging.GetCorrelationID(r.Context()),
						})
						return

					case http.MethodDelete:
						existed, err := nodeCommunicator.ProxyDelete(r.Context(), ownerNode, key)
						if err != nil {
							http.Error(w, fmt.Sprintf("Failed to route DELETE: %v", err), http.StatusBadGateway)
							return
						}
						json.NewEncoder(w).Encode(map[string]interface{}{
							"success": true, "existed": existed, "key": key,
							"node": nodeID, "routed_to": ownerNode,
							"correlation_id": logging.GetCorrelationID(r.Context()),
						})
						return
					}
				}
			}
		}

		switch r.Method {
		case http.MethodGet:
			value, err := store.Get(key)

			// On local miss, attempt read-repair from a peer node.
			// Skip if the key was recently deleted locally (tombstoned).
			if err != nil && readRepairer != nil && !store.IsTombstoned(key) {
				result := readRepairer.TryPeers(r.Context(), key)
				if result != nil && result.Found {
					value = result.Value
					err = nil
					_ = store.Set(key, value, "read-repair", time.Hour)
				}
			}

			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false, "error": "Key not found", "key": key, "node": nodeID,
					"correlation_id": logging.GetCorrelationID(r.Context()),
				})
				return
			}
			if b, ok := value.([]byte); ok {
				value = string(b)
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":        true,
				"data":           map[string]interface{}{"key": key, "value": value},
				"node":           nodeID,
				"local":          true,
				"correlation_id": logging.GetCorrelationID(r.Context()),
			})

		case http.MethodPut:
			var body struct {
				Value interface{} `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "Invalid JSON body", http.StatusBadRequest)
				return
			}

			// Allocate a Lamport timestamp for causal ordering
			lamportTS := uint64(0)
			if coordinator != nil && coordinator.GetClock() != nil {
				lamportTS = coordinator.GetClock().Tick()
			}

			ttl := time.Hour
			if _, err := store.SetWithTimestamp(r.Context(), key, body.Value, "http-api", ttl, lamportTS); err != nil {
				http.Error(w, fmt.Sprintf("Failed to set key: %v", err), http.StatusInternalServerError)
				return
			}

			// Replicate to hash-ring replicas
			replicated := false
			if coordinator != nil && nodeCommunicator != nil && coordinator.GetRouting() != nil {
				replicas := coordinator.GetRouting().GetReplicas(key, 3)
				if consistencyLevel == "quorum" {
					quorumSize := len(replicas)/2 + 1
					_, _ = nodeCommunicator.ReplicateToReplicasQuorum(r.Context(), replicas, key, body.Value, ttl.Seconds(), lamportTS, quorumSize)
				} else {
					for _, replica := range replicas {
						if replica == nodeID {
							continue
						}
						_ = nodeCommunicator.ReplicateEntry(r.Context(), replica, key, body.Value, ttl.Seconds(), lamportTS)
					}
				}
				replicated = true
			}

			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":        true,
				"message":        "Key set successfully",
				"data":           map[string]interface{}{"key": key, "value": body.Value},
				"node":           nodeID,
				"replicated":     replicated,
				"correlation_id": logging.GetCorrelationID(r.Context()),
			})

		case http.MethodDelete:
			// Allocate a Lamport timestamp up-front for the tombstone
			lamportTS := uint64(0)
			if coordinator != nil && coordinator.GetClock() != nil {
				lamportTS = coordinator.GetClock().Tick()
			}

			err := store.DeleteWithTimestamp(key, lamportTS)
			existed := err == nil

			// Synchronously replicate deletes
			replicated := false
			if coordinator != nil && nodeCommunicator != nil && coordinator.GetRouting() != nil {
				replicas := coordinator.GetRouting().GetReplicas(key, 3)
				for _, replica := range replicas {
					if replica == nodeID {
						continue
					}
					_ = nodeCommunicator.ReplicateEntry(r.Context(), replica, key, nil, 0, lamportTS)
				}
				replicated = true
			}

			response := map[string]interface{}{
				"success": true, "existed": existed, "key": key,
				"node": nodeID, "replicated": replicated,
				"correlation_id": logging.GetCorrelationID(r.Context()),
			}
			if !existed {
				response["message"] = "Key not found"
			}
			json.NewEncoder(w).Encode(response)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// handleStoreRequest handles store management endpoints
func handleStoreRequest(w http.ResponseWriter, r *http.Request, storeManager *storage.StoreManager, coordinator cluster.CoordinatorService, nodeID string) {
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
func handleMetrics(w http.ResponseWriter, store *storage.BasicStore, coordinator cluster.CoordinatorService, nodeID string) {
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

	// Cluster metrics
	if coordinator != nil {
		health := coordinator.GetHealth()
		healthVal := 0
		if health.Healthy {
			healthVal = 1
		}
		fmt.Fprintf(&b, "# HELP cacheforge_cluster_healthy Whether the cluster is healthy (1=yes, 0=no)\n")
		fmt.Fprintf(&b, "# TYPE cacheforge_cluster_healthy gauge\n")
		fmt.Fprintf(&b, "cacheforge_cluster_healthy{node=\"%s\"} %d\n", nodeID, healthVal)

		fmt.Fprintf(&b, "# HELP cacheforge_cluster_size Number of nodes in the cluster\n")
		fmt.Fprintf(&b, "# TYPE cacheforge_cluster_size gauge\n")
		fmt.Fprintf(&b, "cacheforge_cluster_size{node=\"%s\"} %d\n", nodeID, health.ClusterSize)
	}

	// Latency histograms and operation counters from metrics collector
	metrics.Global().WritePrometheus(&b, nodeID)

	w.Write([]byte(b.String()))
}

// resolveSeeds resolves seed node addresses using DNS-based discovery.
func resolveSeeds(ctx context.Context, staticSeeds []string, seedDNS string, seedDNSPort int) []string {
	var resolved []string

	// 1. DNS-based discovery (highest priority)
	if seedDNS != "" {
		port := seedDNSPort
		if port == 0 {
			port = 7946
		}

		ips, err := net.LookupHost(seedDNS)
		if err != nil {
			logging.Warn(ctx, logging.ComponentCluster, logging.ActionStart, "DNS seed discovery failed", map[string]interface{}{
				"dns":   seedDNS,
				"error": err.Error(),
			})
		} else {
			for _, ip := range ips {
				addr := net.JoinHostPort(ip, strconv.Itoa(port))
				resolved = append(resolved, addr)
			}
			logging.Info(ctx, logging.ComponentCluster, logging.ActionStart, "DNS seed discovery resolved", map[string]interface{}{
				"dns":      seedDNS,
				"resolved": resolved,
				"count":    len(resolved),
			})
		}
	}

	// 2. Static seeds — resolve hostnames that don't have ports
	for _, seed := range staticSeeds {
		if seed == "" {
			continue
		}

		// If it already has a port (host:port), pass through
		if strings.Contains(seed, ":") {
			resolved = append(resolved, seed)
			continue
		}

		// Bare hostname — resolve and attach gossip port
		ips, err := net.LookupHost(seed)
		if err != nil {
			resolved = append(resolved, net.JoinHostPort(seed, strconv.Itoa(7946)))
			continue
		}

		for _, ip := range ips {
			resolved = append(resolved, net.JoinHostPort(ip, strconv.Itoa(7946)))
		}
	}

	// Deduplicate
	seen := make(map[string]struct{})
	deduped := make([]string, 0, len(resolved))
	for _, addr := range resolved {
		if _, exists := seen[addr]; !exists {
			seen[addr] = struct{}{}
			deduped = append(deduped, addr)
		}
	}

	return deduped
}

// handleReplicationEvent processes incoming replication events from other nodes
func handleReplicationEvent(ctx context.Context, event cluster.ClusterEvent, storeManager *storage.StoreManager, nodeID string, coordinator cluster.CoordinatorService) {
	// Skip events from ourselves
	if event.NodeID == nodeID {
		return
	}

	correlationCtx := logging.WithCorrelationID(ctx, event.CorrelationID)

	if event.Type != cluster.EventDataOperation {
		return
	}

	eventData, ok := event.Data.(map[string]interface{})
	if !ok {
		return
	}

	operation, _ := eventData["operation"].(string)
	key, _ := eventData["key"].(string)
	if key == "" {
		return
	}

	// Resolve the target store (defaults to "default")
	storeName := "default"
	if sn, ok := eventData["store"].(string); ok && sn != "" {
		storeName = sn
	}
	store := storeManager.GetStore(storeName)
	if store == nil {
		logging.Warn(correlationCtx, logging.ComponentCluster, logging.ActionReplication, "Store not found for replication event", map[string]interface{}{
			"store": storeName,
			"key":   key,
		})
		return
	}

	// Extract Lamport timestamp from the event
	var lamportTS uint64
	if tsInterface, exists := eventData["lamport_ts"]; exists {
		if tsFloat, ok := tsInterface.(float64); ok {
			lamportTS = uint64(tsFloat)
		}
	}

	if coordinator.GetClock() != nil && lamportTS > 0 {
		coordinator.GetClock().Witness(lamportTS)
	}

	switch operation {
	case "SET":
		store.FilterAdd(key)
		value := eventData["value"]

		var ttl time.Duration
		if ttlInterface, exists := eventData["ttl"]; exists {
			if ttlFloat, ok := ttlInterface.(float64); ok {
				ttl = time.Duration(ttlFloat) * time.Second
			}
		}

		_, _ = store.SetWithTimestamp(correlationCtx, key, value, "replication", ttl, lamportTS)

	case "DELETE":
		localTS := store.GetTimestamp(key)
		if lamportTS > 0 && localTS > lamportTS {
			return
		}
		_ = store.Delete(key)

	default:
		logging.Warn(correlationCtx, logging.ComponentCluster, logging.ActionReplication, "Unknown replication operation", map[string]interface{}{
			"operation": operation,
			"key":       key,
		})
	}
}
