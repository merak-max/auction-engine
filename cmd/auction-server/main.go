package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/merak-max/auction-engine/internal/auction"
	"github.com/merak-max/auction-engine/internal/httpapi"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:8788", "HTTP listen address")
	configPath := flag.String("config", "config/campaigns.json", "campaign budgets and bursts")
	flag.Parse()
	cfg, err := auction.LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	if os.Getenv("AUCTION_TOKEN") == "" {
		host, _, err := net.SplitHostPort(*addr)
		if err != nil || (host != "localhost" && !net.ParseIP(host).IsLoopback()) {
			log.Fatal("AUCTION_TOKEN is required for non-loopback listeners")
		}
	}
	opts := httpapi.Options{Token: os.Getenv("AUCTION_TOKEN"), Bids: make(map[string]int64, len(cfg.Campaigns))}
	for _, c := range cfg.Campaigns {
		opts.Bids[c.ID] = c.BidMicros
	}
	if dsn := os.Getenv("AUCTION_DATABASE_URL"); dsn != "" {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		db.SetMaxOpenConns(16)
		db.SetMaxIdleConns(16)
		store, err := auction.NewPostgresStore(db, cfg)
		if err != nil {
			log.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = store.Init(ctx)
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		opts.Run = store.Run
		opts.Ready = store.Ping
	} else {
		engine, err := auction.New(cfg, nil)
		if err != nil {
			log.Fatal(err)
		}
		opts.Run = func(_ context.Context, r auction.Request) (auction.Result, error) { return engine.Run(r) }
	}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.NewHandler(opts),
		ReadHeaderTimeout: time.Second,
		ReadTimeout:       2 * time.Second,
		WriteTimeout:      10 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}
	log.Printf("auction server listening on %s", *addr)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	finished := make(chan error, 1)
	go func() { finished <- srv.ListenAndServe() }()
	select {
	case err := <-finished:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}
}
