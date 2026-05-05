package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type byteSize int64

func (b *byteSize) Set(value string) error {
	n, err := parseByteSize(value)
	if err != nil {
		return err
	}
	*b = byteSize(n)
	return nil
}

func (b byteSize) String() string {
	return formatBytes(int64(b))
}

type config struct {
	bucket                  string
	prefix                  string
	transports              string
	size                    byteSize
	warmupSize              byteSize
	chunkSize               byteSize
	iterations              int
	concurrency             int
	timeout                 time.Duration
	credentialsFile         string
	cleanup                 bool
	warmup                  bool
	grpcConnPool            int
	grpcMetrics             bool
	disableGRPCAutoChecksum bool
	contentType             string
}

type transportKind string

const (
	transportHTTPJSON transportKind = "http-json"
	transportGRPC     transportKind = "grpc"
)

type runResult struct {
	Transport transportKind
	Iteration int
	Bytes     int64
	Duration  time.Duration
}

type payloadData struct {
	size    int64
	pattern []byte
}

type benchTarget struct {
	kind   transportKind
	client *storage.Client
}

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(2)
	}

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	cfg := config{
		bucket:                  "bucket-speed-test",
		prefix:                  "gcs-speed-test",
		transports:              "both",
		size:                    byteSize(64 * 1024 * 1024),
		warmupSize:              byteSize(1 * 1024 * 1024),
		chunkSize:               byteSize(16 * 1024 * 1024),
		iterations:              3,
		concurrency:             1,
		timeout:                 10 * time.Minute,
		cleanup:                 true,
		warmup:                  true,
		grpcConnPool:            0,
		grpcMetrics:             false,
		disableGRPCAutoChecksum: false,
		contentType:             "application/octet-stream",
	}

	flag.StringVar(&cfg.bucket, "bucket", cfg.bucket, "GCS bucket name")
	flag.StringVar(&cfg.prefix, "prefix", cfg.prefix, "object name prefix used for benchmark uploads")
	flag.StringVar(&cfg.transports, "transports", cfg.transports, "transports to run: both, http-json, grpc, or comma-separated values")
	flag.Var(&cfg.size, "size", "payload size per object, for example 64MiB or 1GiB")
	flag.Var(&cfg.warmupSize, "warmup-size", "warmup payload size per transport")
	flag.Var(&cfg.chunkSize, "chunk-size", "GCS writer chunk size; 0 disables chunking")
	flag.IntVar(&cfg.iterations, "iterations", cfg.iterations, "measured iterations per transport")
	flag.IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "objects uploaded in parallel per iteration")
	flag.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "overall benchmark timeout")
	flag.StringVar(&cfg.credentialsFile, "credentials", "", "service account JSON file path; defaults to GOOGLE_APPLICATION_CREDENTIALS, GCP_SERVICE_ACCOUNT_JSON, GOOGLE_CREDENTIALS, or ADC")
	flag.BoolVar(&cfg.cleanup, "cleanup", cfg.cleanup, "delete benchmark objects after each run")
	flag.BoolVar(&cfg.warmup, "warmup", cfg.warmup, "run one unmeasured warmup upload per transport")
	flag.IntVar(&cfg.grpcConnPool, "grpc-conn-pool", cfg.grpcConnPool, "override gRPC connection pool size; 0 keeps library default")
	flag.BoolVar(&cfg.grpcMetrics, "grpc-metrics", cfg.grpcMetrics, "enable default gRPC client metrics export")
	flag.BoolVar(&cfg.disableGRPCAutoChecksum, "disable-grpc-autochecksum", cfg.disableGRPCAutoChecksum, "disable gRPC writer automatic CRC32C checksum calculation")
	flag.StringVar(&cfg.contentType, "content-type", cfg.contentType, "object content type")
	flag.Parse()

	return cfg
}

func (c config) validate() error {
	if c.bucket == "" {
		return errors.New("bucket is required")
	}
	if int64(c.size) <= 0 {
		return errors.New("size must be greater than 0")
	}
	if int64(c.warmupSize) <= 0 {
		return errors.New("warmup-size must be greater than 0")
	}
	if int64(c.chunkSize) < 0 {
		return errors.New("chunk-size must be zero or greater")
	}
	if c.iterations <= 0 {
		return errors.New("iterations must be greater than 0")
	}
	if c.concurrency <= 0 {
		return errors.New("concurrency must be greater than 0")
	}
	if c.timeout <= 0 {
		return errors.New("timeout must be greater than 0")
	}
	if c.grpcConnPool < 0 {
		return errors.New("grpc-conn-pool must be zero or greater")
	}
	if _, err := parseTransports(c.transports); err != nil {
		return err
	}
	return nil
}

func run(cfg config) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	transports, err := parseTransports(cfg.transports)
	if err != nil {
		return err
	}

	credOpts, credSource, err := credentialOptions(cfg.credentialsFile)
	if err != nil {
		return err
	}

	fmt.Printf("bucket=%s prefix=%s credentials=%s size=%s warmup_size=%s iterations=%d concurrency=%d chunk_size=%s cleanup=%t\n",
		cfg.bucket, cfg.prefix, credSource, cfg.size, cfg.warmupSize, cfg.iterations, cfg.concurrency, cfg.chunkSize, cfg.cleanup)
	fmt.Printf("grpc_metrics=%t disable_grpc_autochecksum=%t timeout=%s\n\n",
		cfg.grpcMetrics, cfg.disableGRPCAutoChecksum, cfg.timeout)

	targets, err := newTargets(ctx, transports, credOpts, cfg)
	if err != nil {
		return err
	}
	defer closeTargets(targets)

	payload := makePayload(int64(cfg.size))
	warmupPayload := makePayload(int64(cfg.warmupSize))
	runID := time.Now().UTC().Format("20060102T150405Z")

	var all []runResult
	for _, target := range targets {
		if cfg.warmup {
			fmt.Printf("%s warmup...\n", target.kind)
			if _, err := runIteration(ctx, target, cfg, runID, 0, warmupPayload, true); err != nil {
				return fmt.Errorf("%s warmup: %w", target.kind, err)
			}
		}

		for i := 1; i <= cfg.iterations; i++ {
			result, err := runIteration(ctx, target, cfg, runID, i, payload, false)
			if err != nil {
				return fmt.Errorf("%s iteration %d: %w", target.kind, i, err)
			}
			all = append(all, result)
			fmt.Printf("%-9s iter=%d bytes=%s duration=%s throughput=%s\n",
				result.Transport, result.Iteration, formatBytes(result.Bytes), result.Duration.Round(time.Millisecond), formatThroughput(result.Bytes, result.Duration))
		}
		fmt.Println()
	}

	printSummary(all)
	return nil
}

func newTargets(ctx context.Context, transports []transportKind, credOpts []option.ClientOption, cfg config) ([]benchTarget, error) {
	targets := make([]benchTarget, 0, len(transports))
	for _, transport := range transports {
		opts := append([]option.ClientOption(nil), credOpts...)
		switch transport {
		case transportHTTPJSON:
			client, err := storage.NewClient(ctx, opts...)
			if err != nil {
				closeTargets(targets)
				return nil, fmt.Errorf("create http-json client: %w", err)
			}
			targets = append(targets, benchTarget{kind: transportHTTPJSON, client: client})
		case transportGRPC:
			if !cfg.grpcMetrics {
				opts = append(opts, storage.WithDisabledClientMetrics())
			}
			if cfg.grpcConnPool > 0 {
				opts = append(opts, option.WithGRPCConnectionPool(cfg.grpcConnPool))
			}
			client, err := storage.NewGRPCClient(ctx, opts...)
			if err != nil {
				closeTargets(targets)
				return nil, fmt.Errorf("create grpc client: %w", err)
			}
			targets = append(targets, benchTarget{kind: transportGRPC, client: client})
		default:
			closeTargets(targets)
			return nil, fmt.Errorf("unsupported transport %q", transport)
		}
	}
	return targets, nil
}

func closeTargets(targets []benchTarget) {
	for _, target := range targets {
		_ = target.client.Close()
	}
}

func runIteration(ctx context.Context, target benchTarget, cfg config, runID string, iteration int, payload payloadData, warmup bool) (runResult, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	objects := make([]string, cfg.concurrency)
	kind := "measured"
	if warmup {
		kind = "warmup"
	}
	for i := range objects {
		objects[i] = fmt.Sprintf("%s/%s/%s/%s-iter-%03d-part-%03d.bin", strings.Trim(cfg.prefix, "/"), runID, target.kind, kind, iteration, i)
	}
	if cfg.cleanup {
		defer cleanupObjects(context.Background(), target.client, cfg.bucket, objects)
	}

	start := time.Now()
	errCh := make(chan error, cfg.concurrency)
	var wg sync.WaitGroup
	for i := range objects {
		wg.Add(1)
		go func(object string) {
			defer wg.Done()
			if err := uploadObject(runCtx, target, cfg, object, payload); err != nil {
				cancel()
				errCh <- fmt.Errorf("%s: %w", object, err)
			}
		}(objects[i])
	}
	wg.Wait()
	close(errCh)
	duration := time.Since(start)

	if err := firstError(errCh); err != nil {
		return runResult{}, err
	}

	bytesUploaded := payload.size * int64(cfg.concurrency)
	return runResult{
		Transport: target.kind,
		Iteration: iteration,
		Bytes:     bytesUploaded,
		Duration:  duration,
	}, nil
}

func uploadObject(ctx context.Context, target benchTarget, cfg config, object string, payload payloadData) error {
	handle := target.client.Bucket(cfg.bucket).Object(object).If(storage.Conditions{DoesNotExist: true})
	writer := handle.NewWriter(ctx)
	writer.ContentType = cfg.contentType
	writer.ChunkSize = int(cfg.chunkSize)
	if target.kind == transportGRPC && cfg.disableGRPCAutoChecksum {
		writer.DisableAutoChecksum = true
	}

	if _, err := io.Copy(writer, payload.Reader()); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func cleanupObjects(ctx context.Context, client *storage.Client, bucket string, objects []string) {
	for _, object := range objects {
		if err := client.Bucket(bucket).Object(object).Delete(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup warning: gs://%s/%s: %v\n", bucket, object, err)
		}
	}
}

func firstError(errCh <-chan error) error {
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func parseTransports(value string) ([]transportKind, error) {
	if value == "" || strings.EqualFold(value, "both") || strings.EqualFold(value, "all") {
		return []transportKind{transportHTTPJSON, transportGRPC}, nil
	}

	rawParts := strings.Split(value, ",")
	transports := make([]transportKind, 0, len(rawParts))
	seen := map[transportKind]bool{}
	for _, rawPart := range rawParts {
		part := strings.ToLower(strings.TrimSpace(rawPart))
		var transport transportKind
		switch part {
		case "http", "json", "rest", "http-json", "json-rest":
			transport = transportHTTPJSON
		case "grpc":
			transport = transportGRPC
		default:
			return nil, fmt.Errorf("unknown transport %q", rawPart)
		}
		if !seen[transport] {
			transports = append(transports, transport)
			seen[transport] = true
		}
	}
	if len(transports) == 0 {
		return nil, errors.New("at least one transport is required")
	}
	return transports, nil
}

func credentialOptions(credentialsFile string) ([]option.ClientOption, string, error) {
	if credentialsFile != "" {
		return []option.ClientOption{option.WithCredentialsFile(credentialsFile)}, credentialsFile, nil
	}
	if value := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); value != "" {
		return []option.ClientOption{option.WithCredentialsFile(value)}, "GOOGLE_APPLICATION_CREDENTIALS", nil
	}
	if value := os.Getenv("GCP_SERVICE_ACCOUNT_JSON"); value != "" {
		return []option.ClientOption{option.WithCredentialsJSON([]byte(value))}, "GCP_SERVICE_ACCOUNT_JSON", nil
	}
	if value := os.Getenv("GOOGLE_CREDENTIALS"); value != "" {
		return []option.ClientOption{option.WithCredentialsJSON([]byte(value))}, "GOOGLE_CREDENTIALS", nil
	}
	if value := os.Getenv("GCP_SERVICE_ACCOUNT_JSON_B64"); value != "" {
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, "", fmt.Errorf("decode GCP_SERVICE_ACCOUNT_JSON_B64: %w", err)
		}
		return []option.ClientOption{option.WithCredentialsJSON(decoded)}, "GCP_SERVICE_ACCOUNT_JSON_B64", nil
	}
	if _, err := os.Stat("service-account.json"); err == nil {
		return []option.ClientOption{option.WithCredentialsFile("service-account.json")}, "service-account.json", nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, "", fmt.Errorf("stat service-account.json: %w", err)
	}
	return nil, "application default credentials", nil
}

func makePayload(size int64) payloadData {
	patternSize := size
	if patternSize > 1024*1024 {
		patternSize = 1024 * 1024
	}

	pattern := make([]byte, patternSize)
	var x uint64 = 0x9e3779b97f4a7c15
	for i := range pattern {
		x ^= x << 7
		x ^= x >> 9
		x ^= x << 8
		pattern[i] = byte(x)
	}
	return payloadData{size: size, pattern: pattern}
}

func (p payloadData) Reader() io.Reader {
	return &repeatReader{
		pattern:   p.pattern,
		remaining: p.size,
	}
}

type repeatReader struct {
	pattern   []byte
	offset    int
	remaining int64
}

func (r *repeatReader) Read(dst []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(dst)) > r.remaining {
		dst = dst[:int(r.remaining)]
	}

	written := 0
	for written < len(dst) {
		n := copy(dst[written:], r.pattern[r.offset:])
		written += n
		r.offset = (r.offset + n) % len(r.pattern)
	}
	r.remaining -= int64(written)
	return written, nil
}

func printSummary(results []runResult) {
	grouped := map[transportKind][]runResult{}
	for _, result := range results {
		grouped[result.Transport] = append(grouped[result.Transport], result)
	}

	fmt.Println("summary")
	fmt.Printf("%-9s %4s %12s %10s %10s %10s %10s %14s\n",
		"transport", "runs", "bytes/run", "min", "median", "mean", "max", "throughput")

	order := []transportKind{transportHTTPJSON, transportGRPC}
	for _, transport := range order {
		runs := grouped[transport]
		if len(runs) == 0 {
			continue
		}

		durations := make([]time.Duration, len(runs))
		var totalDuration time.Duration
		var totalBytes int64
		for i, run := range runs {
			durations[i] = run.Duration
			totalDuration += run.Duration
			totalBytes += run.Bytes
		}
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		mean := totalDuration / time.Duration(len(runs))
		median := durations[len(durations)/2]
		if len(durations)%2 == 0 {
			median = (durations[len(durations)/2-1] + durations[len(durations)/2]) / 2
		}

		fmt.Printf("%-9s %4d %12s %10s %10s %10s %10s %14s\n",
			transport,
			len(runs),
			formatBytes(runs[0].Bytes),
			durations[0].Round(time.Millisecond),
			median.Round(time.Millisecond),
			mean.Round(time.Millisecond),
			durations[len(durations)-1].Round(time.Millisecond),
			formatThroughput(totalBytes, totalDuration))
	}
}

func parseByteSize(value string) (int64, error) {
	s := strings.TrimSpace(value)
	if s == "" {
		return 0, errors.New("empty byte size")
	}

	lower := strings.ToLower(s)
	multipliers := []struct {
		suffix string
		mult   int64
	}{
		{"gib", 1024 * 1024 * 1024},
		{"gb", 1024 * 1024 * 1024},
		{"g", 1024 * 1024 * 1024},
		{"mib", 1024 * 1024},
		{"mb", 1024 * 1024},
		{"m", 1024 * 1024},
		{"kib", 1024},
		{"kb", 1024},
		{"k", 1024},
		{"b", 1},
	}

	multiplier := int64(1)
	number := lower
	for _, candidate := range multipliers {
		if strings.HasSuffix(lower, candidate.suffix) {
			multiplier = candidate.mult
			number = strings.TrimSpace(strings.TrimSuffix(lower, candidate.suffix))
			break
		}
	}
	if number == "" {
		return 0, fmt.Errorf("missing number in byte size %q", value)
	}

	parsed, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return 0, fmt.Errorf("parse byte size %q: %w", value, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("byte size must be non-negative: %q", value)
	}

	result := parsed * float64(multiplier)
	if result > float64(^uint64(0)>>1) {
		return 0, fmt.Errorf("byte size is too large: %q", value)
	}
	return int64(result), nil
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	value := float64(bytes)
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	for _, suffix := range units {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.2f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.2f PiB", value/unit)
}

func formatThroughput(bytes int64, duration time.Duration) string {
	if duration <= 0 {
		return "n/a"
	}
	mibPerSecond := float64(bytes) / 1024 / 1024 / duration.Seconds()
	return fmt.Sprintf("%.2f MiB/s", mibPerSecond)
}
