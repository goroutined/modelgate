// modelgate 入口：解析配置、打开存储、启动 HTTP 服务。
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/goroutined/modelgate/internal/api"
	"github.com/goroutined/modelgate/internal/provider"
	"github.com/goroutined/modelgate/internal/config"
	"github.com/goroutined/modelgate/internal/live"
	"github.com/goroutined/modelgate/internal/proxy"
	"github.com/goroutined/modelgate/internal/web"
	"github.com/goroutined/modelgate/internal/store"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("modelgate: 参数错误: %v", err)
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("modelgate: %v", err)
	}
	defer st.Close()
	log.Printf("modelgate: 数据库就绪 %s", cfg.DBPath)

	hub := live.NewHub()
	// modellink 官方服务商数据：异步首次加载，管理页选择服务商用
	cat, err := provider.NewSource()
	if err != nil {
		log.Printf("modelgate: modellink 服务商数据不可用: %v", err)
	} else {
		cat.StartLoad()
		// 服务商数据自动同步：启动后等首载完成同步一次，之后每小时刷新官方配置
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			// 等首次目录加载完成
			if _, err := cat.Providers(ctx); err == nil {
				if res, err := cat.Sync(ctx, st); err != nil {
					log.Printf("modelgate: 服务商数据同步失败: %v", err)
				} else {
					log.Printf("modelgate: 服务商数据同步完成 版本=%s 更新=%d 官方已下线=%d", res.Version, res.Updated, res.Gone)
				}
			}
			for range time.Tick(time.Hour) {
				// 每小时顺带执行日志/统计的保留期与水位线清理
				if res, err := st.Cleanup(); err != nil {
					log.Printf("modelgate: 清理失败: %v", err)
				} else if res.CallsDeleted+res.StatsHourlyDeleted+res.StatsDailyDeleted > 0 {
					log.Printf("modelgate: 清理完成 calls=%d stats_hourly=%d stats_daily=%d", res.CallsDeleted, res.StatsHourlyDeleted, res.StatsDailyDeleted)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				if res, err := cat.Sync(ctx, st); err != nil {
					log.Printf("modelgate: 服务商数据同步失败: %v", err)
				} else {
					log.Printf("modelgate: 服务商数据同步完成 版本=%s 更新=%d 官方已下线=%d", res.Version, res.Updated, res.Gone)
				}
				cancel()
			}
		}()
	}
	apiHandler := api.New(st, cfg.AdminToken, cat)
	webHandler := web.Handler()
	proxyHandler := proxy.New(st, hub, cat)

	// 根分发：/api 管理面，/healthz 探活，其余路径——页面资源或调用转发
	root := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/live":
			hub.ServeHTTP(w, r) // 实时观测 WebSocket
		case strings.HasPrefix(r.URL.Path, "/api/"):
			apiHandler.ServeHTTP(w, r)
		case r.URL.Path == "/healthz":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		case r.URL.Path == "/" || strings.Contains(r.URL.Path, "."):
			webHandler.ServeHTTP(w, r)
		default:
			proxyHandler.ServeHTTP(w, r)
		}
	})
	mux := http.NewServeMux()
	mux.Handle("/", root)
	srv := &http.Server{Addr: cfg.Listen, Handler: mux}
	go func() {
		log.Printf("modelgate: 监听 http://%s", cfg.Listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("modelgate: %v", err)
		}
	}()

	// 优雅退出：等待在途调用（尤其 SSE 流）收尾
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("modelgate: 退出超时: %v", err)
	}
}
