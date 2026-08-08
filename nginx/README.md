# Nginx Load Balancer Configuration Guide

This guide covers multiple approaches for configuring nginx as a load balancer for CacheForge, from simple static configs to dynamic service discovery.

## 🎯 Quick Start (Recommended for Most Users)

### Step 1: Start Your CacheForge Cluster
```bash
# Start 3 nodes
./scripts/start-cluster-enhanced.sh --nodes=3

# Or start 5 nodes with custom ports
./scripts/start-cluster-enhanced.sh --nodes=5 --base-port=8000
```

### Step 2: Use the Static Nginx Config
```bash
# Edit the nginx config to match your nodes
vim nginx/cacheforge.conf

# Start nginx
./scripts/start-nginx.sh

# Connect via load balancer
redis-cli -h localhost -p 6379
```

## 📋 Available Approaches

### Approach 1: Static Configuration (Simplest)

**Best for:** Small, stable clusters with known node counts.

**File:** `nginx/cacheforge.conf`

**How it works:**
- Manually edit the config file to add/remove nodes
- Simple, predictable, no external dependencies
- Perfect for development and small production deployments

**Adding a node:**
```bash
# Method A: Edit config manually
vim nginx/cacheforge.conf
# Add: server 127.0.0.1:8083 max_fails=3 fail_timeout=30s;

# Method B: Use management script
./scripts/manage-nginx.sh add-node --port=8083
./scripts/manage-nginx.sh reload-nginx
```