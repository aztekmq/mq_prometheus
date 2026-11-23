package main

/*
  Copyright (c) IBM Corporation 2016, 2026

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific

   Contributors:
     Mark Taylor - Initial Contribution
*/

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"html"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	cf "github.com/ibm-messaging/mq-metric-samples/v5/pkg/config"
	log "github.com/sirupsen/logrus"
)

var BuildStamp, GitCommit, BuildPlatform string

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "--version" || os.Args[1] == "-version") {
		fmt.Printf("mq_prometheus_ent (%s %s/%s)\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
		fmt.Printf("IBM MQ Go dependency: %s\n", cf.MqGolangVersion())
		if GitCommit != "" {
			fmt.Printf("Git commit: %s\n", GitCommit)
		}
		return
	}
	cf.PrintInfo("IBM MQ metrics exporter for Prometheus monitoring", BuildStamp, GitCommit, BuildPlatform)
	if err := run(); err != nil {
		log.Error(err)
		os.Exit(10)
	}
}

func run() error {
	if err := initConfig(); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cache := newSnapshotCache(config.collectionInterval)
	server, err := newHTTPServer(cache)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}

	httpDone := make(chan error, 1)
	go func() {
		log.Infof("Metrics HTTP server listening on %s", listener.Addr())
		if server.TLSConfig != nil {
			httpDone <- server.ServeTLS(listener, "", "")
		} else {
			httpDone <- server.Serve(listener)
		}
	}()

	workerDone := make(chan error, 1)
	go func() { workerDone <- runWorker(ctx, cache) }()

	var result error
	select {
	case result = <-httpDone:
	case result = <-workerDone:
		workerDone = nil
	case <-ctx.Done():
	}
	cancel()

	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		result = errors.Join(result, err)
	}
	if workerDone != nil {
		select {
		case err := <-workerDone:
			result = errors.Join(result, err)
		case <-shutdown.Done():
			result = errors.Join(result, fmt.Errorf("MQ worker did not stop before the shutdown deadline: %w", shutdown.Err()))
		}
	}
	return result
}

func newHTTPServer(cache *snapshotCache) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.Handle(config.httpMetricPath, cache)
	if config.httpMetricPath != "/" {
		path := html.EscapeString(config.httpMetricPath)
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprintf(w, "<!doctype html><title>IBM MQ metrics exporter</title><a href=\"%s\">Metrics</a>", path)
		})
	}
	server := &http.Server{
		Addr: net.JoinHostPort(config.httpListenHost, config.httpListenPort), Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}
	if config.httpsCertFile != "" || config.httpsKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.httpsCertFile, config.httpsKeyFile)
		if err != nil {
			return nil, err
		}
		server.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}
	return server, nil
}
