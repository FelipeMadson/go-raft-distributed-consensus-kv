package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FelipeMadson/go-raft-distributed-consensus-kv/pkg/kv"
	"github.com/FelipeMadson/go-raft-distributed-consensus-kv/pkg/raft"
	"github.com/FelipeMadson/go-raft-distributed-consensus-kv/pkg/transport"
)

type App struct {
	net     *transport.Network
	nodes   []*raft.RaftNode
	servers []*kv.KVServer
}

func main() {
	fmt.Println("===============================================================")
	fmt.Println(" Go Raft Distributed Consensus Key-Value Store (v1.0.0)")
	fmt.Println(" Linearizable Replicated State Machine - Felipe Madison")
	fmt.Println("===============================================================")

	net := transport.NewNetwork()
	nodeCount := 3
	app := &App{
		net:     net,
		nodes:   make([]*raft.RaftNode, nodeCount),
		servers: make([]*kv.KVServer, nodeCount),
	}

	for i := 0; i < nodeCount; i++ {
		storage := raft.NewMemoryStorage()
		applyCh := make(chan raft.ApplyMsg, 500)

		var peers []int
		for j := 0; j < nodeCount; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}

		app.nodes[i] = raft.NewRaftNode(i, peers, storage, net, applyCh)
		app.servers[i] = kv.NewKVServer(i, app.nodes[i], applyCh)
		log.Printf("[Init] Raft Node %d initialized (peers: %v)\n", i, peers)
	}

	// Helper to find current leader
	getLeader := func() (*kv.KVServer, int) {
		for i := 0; i < nodeCount; i++ {
			_, isLeader := app.nodes[i].GetState()
			if isLeader {
				return app.servers[i], i
			}
		}
		return nil, -1
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "healthy",
			"timestamp": time.Now().UTC(),
			"nodes":     nodeCount,
		})
	})

	mux.HandleFunc("/cluster", func(w http.ResponseWriter, r *http.Request) {
		clusterInfo := make([]map[string]interface{}, nodeCount)
		for i := 0; i < nodeCount; i++ {
			term, isLeader := app.nodes[i].GetState()
			role := "Follower"
			if isLeader {
				role = "Leader"
			}
			clusterInfo[i] = map[string]interface{}{
				"node_id":   i,
				"term":      term,
				"role":      role,
				"is_leader": isLeader,
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"cluster": clusterInfo,
		})
	})

	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid body", http.StatusBadRequest)
			return
		}

		leaderSrv, leaderID := getLeader()
		if leaderSrv == nil {
			http.Error(w, "No leader currently available in cluster", http.StatusServiceUnavailable)
			return
		}

		clientID := fmt.Sprintf("http-%s", r.RemoteAddr)
		seqNum := time.Now().UnixNano()

		if err := leaderSrv.Put(clientID, seqNum, payload.Key, payload.Value); err != nil {
			http.Error(w, fmt.Sprintf("Raft commit failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"committed": true,
			"leader_id": leaderID,
			"key":       payload.Key,
			"value":     payload.Value,
		})
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Missing 'key' query parameter", http.StatusBadRequest)
			return
		}

		leaderSrv, leaderID := getLeader()
		if leaderSrv == nil {
			http.Error(w, "No leader currently elected", http.StatusServiceUnavailable)
			return
		}

		val, exists, err := leaderSrv.Get(key)
		if err != nil {
			http.Error(w, fmt.Sprintf("Read failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"key":       key,
			"value":     val,
			"exists":    exists,
			"leader_id": leaderID,
		})
	})

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		log.Println("[Server] Raft KV HTTP Gateway listening on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[Server] Listen error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("[Server] Shutting down cluster gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_ = server.Shutdown(ctx)
	for i := 0; i < nodeCount; i++ {
		app.servers[i].Close()
	}
	log.Println("[Server] All Raft nodes terminated cleanly.")
}
