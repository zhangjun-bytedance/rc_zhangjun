// Command notifyd 是 API 通知投递服务的主程序。
//
// 进程内包含两个长期运行的部分：HTTP 入口和投递调度器。
// 它们共享同一个持久化队列，但生命周期独立管理——
// 关闭时必须先停 HTTP 入口（不再接新任务），再让调度器把在途投递跑完。
// 顺序反了会出现"已经不收新请求但还在拒绝老任务"的空窗。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"rc_zhangjun/internal/api"
	"rc_zhangjun/internal/config"
	"rc_zhangjun/internal/delivery"
	"rc_zhangjun/internal/dispatcher"
	"rc_zhangjun/internal/metrics"
	"rc_zhangjun/internal/store"
)

func main() {
	if err := run(); err != nil {
		// 启动期错误直接打到 stderr 并非零退出：
		// 让 systemd/K8s 能立刻识别为启动失败，而不是拉起一个半残的进程。
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath = flag.String("config", "config.yaml", "path to the configuration file")
		logLevel   = flag.String("log-level", "info", "log level: debug, info, warn, error")
		logFormat  = flag.String("log-format", "text", "log format: text or json")
		validate   = flag.Bool("validate", false, "validate the configuration and exit")
	)
	flag.Parse()

	logger, err := newLogger(*logLevel, *logFormat)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if *validate {
		// 让配置校验可以在 CI 或部署前单独跑一遍，
		// 而不是靠"启动看它崩不崩"来验证。
		fmt.Printf("configuration %s is valid: %d endpoint(s)\n", *configPath, len(cfg.Endpoints))
		return nil
	}

	endpoints := cfg.EndpointMap()

	st, err := store.Open(store.Options{
		Path:        cfg.Store.Path,
		Synchronous: cfg.Store.Synchronous,
		BusyTimeout: cfg.Store.BusyTimeout,
	})
	if err != nil {
		return err
	}
	defer st.Close()

	exec, err := delivery.NewExecutor(endpoints)
	if err != nil {
		return err
	}

	mx := metrics.New()
	owner := instanceID(cfg.Dispatcher.InstanceID)

	disp := dispatcher.New(dispatcher.Options{
		Config:    cfg.Dispatcher,
		Endpoints: endpoints,
		Store:     st,
		Executor:  exec,
		Metrics:   mx,
		Logger:    logger,
		Owner:     owner,
	})

	srv := &http.Server{
		Addr: cfg.Server.Addr,
		Handler: api.New(api.Options{
			Config:     cfg.Server,
			Endpoints:  endpoints,
			Store:      st,
			Dispatcher: disp,
			Metrics:    mx,
			Logger:     logger,
		}).Handler(),
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  60 * time.Second,
	}

	// SIGINT / SIGTERM 触发优雅关闭。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		disp.Run(ctx, cfg.Server.ShutdownGrace)
	}()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.Server.Addr,
			"instance", owner, "store", cfg.Store.Path)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case err := <-serverErr:
		if err != nil {
			// 监听失败（端口被占用等）时主动触发关闭，
			// 否则调度器会在一个没有入口的进程里继续跑。
			stop()
			wg.Wait()
			return fmt.Errorf("http server: %w", err)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining")
	}

	// 先关 HTTP 入口：停止接收新任务，但给在途请求留出完成时间。
	// 入口的 drain 时间给得比投递 grace 短，因为入口只是"写一条记录"，很快。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http server shutdown was not clean", "error", err)
	}

	// 再等调度器把在途投递跑完（Run 内部自己处理 grace 和硬取消）。
	wg.Wait()
	logger.Info("shutdown complete")
	return nil
}

func newLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level %q", level)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch format {
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stdout, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(os.Stdout, opts)), nil
	default:
		return nil, fmt.Errorf("invalid log format %q", format)
	}
}

// instanceID 生成 lease 持有者标识。
//
// 必须能区分同一台机器上的不同进程（滚动发布期间新旧进程会短暂共存），
// 所以用 hostname + pid 而不是只用 hostname。
func instanceID(configured string) string {
	if configured != "" {
		return configured
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}
