// Package provider 用 modellink-go 加载官方模型服务商数据，供管理页选择服务商/查看模型。
// 首次加载需要网络（npm registry）；之后走本地缓存。
package provider

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	modellink "github.com/goroutined/modellink-go"

	"github.com/goroutined/modelgate/internal/store"
)

// Endpoint 协议端点。
type Endpoint struct {
	Protocol string `json:"protocol"`
	API      string `json:"api"`
	Default  bool   `json:"default"`
}

// Provider 官方模型服务商条目。
type Provider struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	API       string         `json:"api"` // 默认 base url
	Protocol  string         `json:"protocol"`
	Endpoints  []Endpoint `json:"endpoints"`
	Models     []string       `json:"models"`
	PricingURL string         `json:"pricing_url"`
	ConsoleURL string         `json:"console_url"`
	APIKeyURL  string         `json:"api_key_url"`
	Version   string         `json:"modellink_version"`
}

// Source 封装 modellink 客户端；快照加载一次后内存复用。
type Source struct {
	client *modellink.Client

	mu       sync.RWMutex
	loaded   bool
	loading  chan struct{}
	prov     []Provider
	version  string
	loadErr  error
}

// NewSource 构造数据源。
func NewSource() (*Source, error) {
	c, err := modellink.New(modellink.Options{})
	if err != nil {
		return nil, fmt.Errorf("catalog: 初始化 modellink 客户端: %w", err)
	}
	return &Source{client: c, loading: make(chan struct{})}, nil
}

// Providers 返回服务商列表；必要时触发首次加载（可能耗时数秒）。
func (s *Source) Providers(ctx context.Context) ([]Provider, error) {
	s.mu.RLock()
	if s.loaded {
		defer s.mu.RUnlock()
		return s.prov, s.loadErr
	}
	loading := s.loading
	s.mu.RUnlock()

	select {
	case <-loading: // 等首次加载完成
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.prov, s.loadErr
}

// StartLoad 异步触发首次加载，避免管理页首请求卡住。
func (s *Source) StartLoad() {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s.doLoad(ctx)
	}()
}

// Refresh 检查并加载最新数据版本，然后重载内存服务商数据。
func (s *Source) Refresh(ctx context.Context) error {
	if _, err := s.client.CheckLatest(ctx); err != nil {
		// 检查失败不致命，继续用缓存/当前版本
		_ = err
	}
	if _, err := s.client.LoadLatest(ctx); err != nil {
		return err
	}
	// 重置加载状态后重新加载
	s.mu.Lock()
	if s.loaded {
		s.loaded = false
		s.loading = make(chan struct{})
	}
	s.mu.Unlock()
	s.doLoad(ctx)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.loadErr
}

// Result 服务商数据同步结果。
type Result struct {
	Updated   int `json:"updated"`    // 官方配置有实际变化并刷新的
	Unchanged int `json:"unchanged"`  // 已是最新无变化
	Gone      int `json:"gone"`       // 官方数据里已消失（本地保留不动）
	Version   string `json:"version"`
}

// Sync 用最新官方数据刷新本地模型服务商（非 custom-）的官方配置；不动 Key/模型/自定义服务商。
func (s *Source) Sync(ctx context.Context, st *store.Store) (Result, error) {
	var res Result
	if err := s.Refresh(ctx); err != nil {
		return res, fmt.Errorf("刷新服务商数据: %w", err)
	}
	s.mu.RLock()
	provs := s.prov
	res.Version = s.version
	s.mu.RUnlock()
	byID := map[string]Provider{}
	for _, p := range provs {
		byID[p.ID] = p
	}
	ups, err := st.ListUpstreams()
	if err != nil {
		return res, err
	}
	for _, u := range ups {
		if strings.HasPrefix(u.Name, "custom-") {
			continue
		}
		p, ok := byID[u.Name]
		if !ok {
			res.Gone++
			continue
		}
		eps := make([]store.Endpoint, 0, len(p.Endpoints))
		for _, e := range p.Endpoints {
			eps = append(eps, store.Endpoint{Protocol: e.Protocol, API: e.API, Default: e.Default})
		}
		if len(eps) == 0 {
			eps = append(eps, store.Endpoint{Protocol: "openai-compatible", API: p.API, Default: true})
		}
		changed, err := st.SyncUpstream(u.ID, eps, p.PricingURL, p.ConsoleURL)
		if err != nil {
			return res, fmt.Errorf("同步 %s: %w", u.Name, err)
		}
		if changed {
			res.Updated++
		} else {
			res.Unchanged++
		}
	}
	return res, nil
}

func (s *Source) doLoad(ctx context.Context) {
	snap, err := s.client.Load(ctx)
	var provs []Provider
	var version string
	if err == nil {
		version = snap.Manifest.Version
		for _, p := range snap.Catalog.Providers {
			api := ""
			if p.API != nil {
				api = *p.API
			}
			models := make([]string, 0, len(p.Models))
			for _, m := range p.Models {
				models = append(models, m.ID)
			}
			sort.Strings(models)
			endpoints := make([]Endpoint, 0, len(p.Endpoints))
			for _, e := range p.Endpoints {
				endpoints = append(endpoints, Endpoint{
					Protocol: string(e.Protocol), API: e.API,
					Default: e.Default != nil && *e.Default,
				})
			}
			pricingURL, consoleURL, apiKeyURL := "", "", ""
			if p.Links != nil {
				if p.Links.Pricing != nil {
					pricingURL = *p.Links.Pricing
				}
				if p.Links.Console != nil {
					consoleURL = *p.Links.Console
				}
				if p.Links.APIKey != nil {
					apiKeyURL = *p.Links.APIKey
				}
			}
			provs = append(provs, Provider{
				ID: p.ID, Name: p.Name, API: api,
				Protocol: string(p.Protocol), Endpoints: endpoints, Models: models,
				PricingURL: pricingURL, ConsoleURL: consoleURL, APIKeyURL: apiKeyURL, Version: version,
			})
		}
		sort.Slice(provs, func(i, j int) bool { return provs[i].Name < provs[j].Name })
	}
	s.mu.Lock()
	s.prov, s.version, s.loadErr, s.loaded = provs, version, err, true
	close(s.loading)
	s.mu.Unlock()
}


// Usage 一次调用的计费用量（协议无关归一）。
type Usage struct {
	PromptTokens     int64
	CompletionTokens int64
	CachedInputTokens int64 // 命中缓存的输入
	CacheWriteTokens  int64
}

// CostResult 计价结果。
type CostResult struct {
	CNY    float64
	Points float64
	OK     bool
}

// Cost 用 modellink 快照计价；未命中目录或无价格返回 OK=false。
func (s *Source) Cost(providerID, model string, u Usage, at time.Time) CostResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.loaded {
		return CostResult{}
	}
	snap, err := s.client.LoadCached(context.Background())
	if err != nil {
		return CostResult{}
	}
	usage := modellink.OpenAIChatUsage{
		PromptTokens:       u.PromptTokens,
		CompletionTokens:   u.CompletionTokens,
		PromptCachedTokens: u.CachedInputTokens,
	}
	res, ok, err := snap.Cost(modellink.CostQuery{Provider: providerID, Model: model, Usage: usage, At: at})
	if err != nil || !ok {
		return CostResult{}
	}
	out := CostResult{OK: true}
	switch res.Currency {
	case modellink.CostCNY:
		out.CNY = res.Total
	case modellink.CostPoints:
		out.Points = res.Total
	case modellink.CostUSD:
		// 统一按 CNY 口径不换算，视为未计价（美元供应商目录里极少）
		out.OK = false
	}
	return out
}
