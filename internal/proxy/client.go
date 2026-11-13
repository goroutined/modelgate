package proxy

import (
	"encoding/json"
	"net/http"
)

// 客户端会话识别：从请求头提取「哪个客户端、哪个会话」，供日志按会话分组。
// 字段是通用概念，不绑定具体客户端；新客户端兼容时在 clientExtractors 追加规则即可，
// 存储与展示层只感知 session_id / client 两个值。

// clientSession 识别结果；SessionID 为空表示未识别（归入「未标记」组）。
type clientSession struct {
	SessionID string // 会话键：客户端自带的会话标识头
	Client    string // 客户端标识：展示用，如 Codex Desktop
	TurnID    string // 轮次键：一次任务/一轮对话内多次调用的归属，空=客户端未提供
}

// clientExtractors 按顺序执行，首个命中即返回。
var clientExtractors = []func(h http.Header) clientSession{
	// Codex Desktop / CLI：Session-Id 头唯一标识一次会话，Originator 作客户端名
	func(h http.Header) clientSession {
		sid := h.Get("Session-Id")
		if sid == "" {
			return clientSession{}
		}
		client := h.Get("Originator")
		if client == "" {
			client = "codex"
		}
		return clientSession{SessionID: sid, Client: client, TurnID: codexTurnID(h)}
	},
	// DeepSeek Harness（dsh）：X-Deepseek-Harness-Session-Id 标识会话；无轮次头，turn 留空
	func(h http.Header) clientSession {
		sid := h.Get("X-Deepseek-Harness-Session-Id")
		if sid == "" {
			return clientSession{}
		}
		return clientSession{SessionID: sid, Client: "DeepSeek Harness"}
	},
}

// codexTurnID 从 X-Codex-Turn-Metadata（JSON 头）提取 turn_id；解析失败留空不阻塞记录。
func codexTurnID(h http.Header) string {
	raw := h.Get("X-Codex-Turn-Metadata")
	if raw == "" {
		return ""
	}
	var meta struct {
		TurnID string `json:"turn_id"`
	}
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return ""
	}
	return meta.TurnID
}

func identifyClient(h http.Header) clientSession {
	for _, extract := range clientExtractors {
		if cs := extract(h); cs.SessionID != "" {
			return cs
		}
	}
	return clientSession{}
}
