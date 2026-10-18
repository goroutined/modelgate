// Package live 是实时观测的内存广播中心：调用事件（含 SSE 分包）推给订阅者。
// 只服务"正在看的人"，归档仍由 store 负责；进程重启后实时态清空。
package live

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// Event 推给订阅者的事件；Data 用 json.RawMessage 保留原始结构。
type Event struct {
	Type string          `json:"type"` // call_start / chunk / call_end
	Body json.RawMessage `json:"body"`
}

// Hub 内存订阅中心。
type Hub struct {
	mu   sync.RWMutex
	subs map[chan Event]struct{}
}

// NewHub 构造 Hub。
func NewHub() *Hub {
	return &Hub{subs: make(map[chan Event]struct{})}
}

// Publish 广播事件；无订阅者时丢弃。
func (h *Hub) Publish(typ string, body any) {
	raw, err := json.Marshal(body)
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.subs) == 0 {
		return
	}
	ev := Event{Type: typ, Body: raw}
	for ch := range h.subs {
		select {
		case ch <- ev:
		default: // 慢消费者丢弃，不阻塞转发
		}
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, // 管理页本机使用，不校验来源
}

// ServeWS 处理 /api/live 的 WebSocket 升级与推送循环。
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	ch := make(chan Event, 256)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.subs, ch)
		h.mu.Unlock()
		conn.Close()
	}()
	// 读循环只为了感知客户端断开；不 close(ch)，避免 Publish 向已关闭 channel 发送
	done := make(chan struct{})
	go func() {
		conn.SetReadLimit(512)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				close(done)
				return
			}
		}
	}()
	for {
		select {
		case <-done:
			return
		case ev := <-ch:
			if err := conn.WriteJSON(ev); err != nil {
				return
			}
		}
	}
}

// ServeHTTP 让 Hub 直接作为 http.Handler 挂载。
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.ServeWS(w, r) }
