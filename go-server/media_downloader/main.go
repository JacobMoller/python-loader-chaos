package main

import (
	"container/list"
	"context"
	"flag"
	"fmt"
	"log"
	"io"
	pb "media_downloader/gen/go"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type ressourceState int

const (
	unavailable ressourceState = 0
	downloading ressourceState = 1
	available   ressourceState = 2

	downloadedPath = "/app/downloaded-medias/"
)

var (
	grpcHost      = mustGetEnv("MEDIA_DOWNLOADER_HOST")
	grpcPort      = mustGetEnvInt("MEDIA_DOWNLOADER_PORT")
	ressources    = make(map[string]*ressourceEntry)
	c             = sync.NewCond(&sync.Mutex{})
	cacheSize     int64
	maxCacheSize  = mustGetEnvInt64("MAX_CACHE_SIZE")
	ressourceTTL  = time.Duration(mustGetEnvInt64("RESSOURCE_TTL")) * time.Second
	cacheElements = list.New()

	// Optional URI rewriting: replaces MEDIA_URI_REWRITE_FROM with MEDIA_URI_REWRITE_TO
	// in incoming media URIs before downloading. Useful when URIs stored in DB use
	// "localhost" but the media server is only reachable via another hostname inside Docker.
	uriRewriteFrom = os.Getenv("MEDIA_URI_REWRITE_FROM")
	uriRewriteTo   = os.Getenv("MEDIA_URI_REWRITE_TO")

	// Buffered channel for async file deletion
	deletionQueue = make(chan string, 1000)
)

type ressourceEntry struct {
	requestCounter int
	path           string
	state          ressourceState
	timer          *time.Timer
	size           int64
	elem           *list.Element
}

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	return value
}

func mustGetEnvInt(key string) int {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	v, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("Environment variable %s must be an integer, got: %s", key, value)
	}
	return v
}

func mustGetEnvInt64(key string) int64 {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		log.Fatalf("Environment variable %s must be an integer, got: %s", key, value)
	}
	return v
}

func (res *ressourceEntry) addRequest() {
	res.requestCounter++
}

func (res *ressourceEntry) removeRequest() {
	res.requestCounter--
}

type server struct {
	pb.UnimplementedMediaDownloaderServer
}

func rewriteURI(uri string) string {
	if uriRewriteFrom != "" && uriRewriteTo != "" {
		return strings.Replace(uri, uriRewriteFrom, uriRewriteTo, 1)
	}
	return uri
}

// httpClient is reused across downloads for connection pooling.
var httpClient = &http.Client{
	Timeout: 120 * time.Second,
	Transport: &http.Transport{
		ResponseHeaderTimeout: 10 * time.Second,
		MaxIdleConns:          10,
		IdleConnTimeout:       30 * time.Second,
	},
}

// downloadResult holds the result of a download.
type downloadResult struct {
	filename   string
	size       int64
	statusCode int
}

// downloadFile downloads a URL to destDir. Returns immediately on HTTP errors.
func downloadFile(destDir, url string) (*downloadResult, error) {
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		// Drain body to allow connection reuse
		io.Copy(io.Discard, resp.Body)
		return &downloadResult{statusCode: resp.StatusCode}, nil
	}

	// Extract filename from URL
	parts := strings.Split(url, "/")
	filename := parts[len(parts)-1]
	destPath := filepath.Join(destDir, filename)

	f, err := os.Create(destPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	n, err := io.Copy(f, resp.Body)
	f.Close()
	if err != nil {
		os.Remove(destPath)
		return nil, fmt.Errorf("failed to write file: %w", err)
	}

	return &downloadResult{
		filename:   destPath,
		size:       n,
		statusCode: resp.StatusCode,
	}, nil
}

func (s *server) RequestMedia(ctx context.Context, req *pb.RequestMediaRequest) (*pb.RequestMediaResponse, error) {
	if uri := req.GetMediaUri(); !(len(uri) >= 7 && (uri[:7] == "http://" || (len(uri) >= 8 && uri[:8] == "https://"))) {
		return &pb.RequestMediaResponse{MediaPath: uri}, nil
	}

	// Rewrite URI for internal Docker networking (e.g. localhost -> host.docker.internal)
	downloadURI := rewriteURI(req.GetMediaUri())

	c.L.Lock()
	defer c.L.Unlock()
	log.Printf("Requesting media: %s (download from: %s)", req.GetMediaUri(), downloadURI)
	entry, ok := ressources[req.GetMediaUri()]
	if !ok {
		ressources[req.GetMediaUri()] = &ressourceEntry{
			requestCounter: 0,
			state:          unavailable,
		}
		entry = ressources[req.GetMediaUri()]
	}
	entry.addRequest()
	if entry.timer != nil {
		log.Printf("Stopping timer for %s", req.GetMediaUri())
		entry.timer.Stop()
		entry.timer = nil
	}

	if entry.state == downloading {
		log.Printf("Media %s is currently being downloaded, waiting for completion", req.GetMediaUri())
		for entry.state == downloading {
			c.Wait()
		}
	}
	if entry.state == available {
		log.Printf("Media %s is already available", req.GetMediaUri())
		return &pb.RequestMediaResponse{MediaPath: entry.path}, nil
	} else if entry.state == unavailable {
		log.Printf("Media %s is not available, starting download", req.GetMediaUri())
		entry.state = downloading

		// Release lock during network I/O to avoid blocking all other operations
		uri := req.GetMediaUri()
		c.L.Unlock()

		var result *downloadResult
		var err error
		maxRetries := 5
		for attempt := 1; attempt <= maxRetries; attempt++ {
			result, err = downloadFile(downloadedPath, downloadURI)

			if err == nil {
				// HTTP error (e.g. 404) — no retry
				if result.statusCode >= 400 {
					break
				}
				// Success
				break
			}

			log.Printf("Download attempt %d/%d failed for %s: %v – retrying in %ds...",
				attempt, maxRetries, uri, err, attempt*2)
			time.Sleep(time.Duration(attempt*2) * time.Second)
		}

		c.L.Lock()
		log.Printf("Download complete, mutex re-acquired: %s (err=%v)", uri, err)

		// Re-fetch entry: it may have been evicted while we were downloading
		entry, ok = ressources[uri]
		if !ok {
			if result != nil && result.filename != "" {
				deletionQueue <- result.filename
			}
			return nil, status.Errorf(codes.Aborted, "media was evicted during download")
		}

		if err != nil {
			entry.state = unavailable
			entry.removeRequest()
			if entry.requestCounter <= 0 {
				delete(ressources, uri)
			}
			c.Broadcast()
			return nil, status.Errorf(codes.NotFound, "remote request failed: %v", err)
		}
		if result.statusCode >= 400 {
			log.Printf("Media %s returned HTTP %d, discarding", uri, result.statusCode)
			entry.state = unavailable
			entry.removeRequest()
			if entry.requestCounter <= 0 {
				delete(ressources, uri)
			}
			c.Broadcast()
			return nil, status.Errorf(codes.NotFound, "remote server returned HTTP %d", result.statusCode)
		}
		if result.size > maxCacheSize {
			entry.state = unavailable
			entry.removeRequest()
			if entry.requestCounter <= 0 {
				delete(ressources, uri)
			}
			c.Broadcast()
			deletionQueue <- result.filename
			return nil, status.Errorf(codes.ResourceExhausted, "requested media is too large (%d bytes), maximum allowed size is %d bytes", result.size, maxCacheSize)
		}
		entry.path = result.filename
		entry.size = result.size
		cacheSize += entry.size
		var evictedPaths []string
		for cacheSize > maxCacheSize {
			oldest := cacheElements.Front()
			if oldest == nil {
				return nil, status.Errorf(codes.ResourceExhausted, "cache size exceeded and no elements to remove")
			}
			path := evictRessource(oldest.Value.(string))
			if path != "" {
				evictedPaths = append(evictedPaths, path)
			}
		}
		entry.elem = cacheElements.PushBack(req.GetMediaUri())

		entry.state = available
		c.Broadcast()

		// Send evicted files to background deletion worker (non-blocking)
		removeFiles(evictedPaths)

		return &pb.RequestMediaResponse{MediaPath: entry.path}, nil

	}

	return nil, nil
}

func (s *server) ReleaseMedia(ctx context.Context, req *pb.ReleaseMediaRequest) (*pb.ReleaseMediaResponse, error) {
	if uri := req.GetMediaUri(); !(len(uri) >= 7 && (uri[:7] == "http://" || (len(uri) >= 8 && uri[:8] == "https://"))) {
		return &pb.ReleaseMediaResponse{}, nil
	}
	c.L.Lock()
	defer c.L.Unlock()
	log.Printf("Releasing media: %s", req.GetMediaUri())
	entry, ok := ressources[req.GetMediaUri()]
	if !ok {
		log.Printf("Media %s already evicted, nothing to release", req.GetMediaUri())
		return &pb.ReleaseMediaResponse{}, nil
	}
	entry.removeRequest()
	if entry.requestCounter <= 0 {
		uri := req.GetMediaUri()
		entry.timer = time.AfterFunc(ressourceTTL, func() {
			c.L.Lock()
			defer c.L.Unlock()
			destroyRessource(uri)
		})
	}
	return &pb.ReleaseMediaResponse{}, nil
}

// evictRessource removes the entry from the map and cache list, returns the file path to delete.
// Must be called under c.L lock. Does NOT remove the file from disk.
func evictRessource(URI string) string {
	res, ok := ressources[URI]
	if !ok {
		return ""
	}
	if res.requestCounter > 0 {
		return ""
	}
	if res.elem != nil {
		cacheElements.Remove(res.elem)
	}
	path := res.path
	cacheSize -= res.size
	delete(ressources, URI)
	return path
}

// destroyRessource removes entry from map and sends file to async deletion.
// Must be called under c.L lock.
func destroyRessource(URI string) {
	path := evictRessource(URI)
	if path != "" {
		deletionQueue <- path
	}
}

// removeFiles sends file paths to the deletion goroutine for async removal.
func removeFiles(paths []string) {
	for _, p := range paths {
		if p != "" {
			deletionQueue <- p
		}
	}
}

// deletionWorker runs in a background goroutine and deletes files from disk.
func deletionWorker() {
	for path := range deletionQueue {
		log.Printf("removing ressource %s", path)
		if err := os.Remove(path); err != nil {
			log.Printf("failed to remove file: %v", err)
		}
	}
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", grpcHost, grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// Start background file deletion worker
	go deletionWorker()

	s := grpc.NewServer()
	healthcheck := health.NewServer()
	healthgrpc.RegisterHealthServer(s, healthcheck)
	pb.RegisterMediaDownloaderServer(s, &server{})
	log.Printf("server listening at %v (maxCacheSize=%d bytes, ressourceTTL=%s)", lis.Addr(), maxCacheSize, ressourceTTL)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	healthcheck.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
}
