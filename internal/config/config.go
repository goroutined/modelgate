// Package config 定义启动配置；业务配置（key 等）存 SQLite，这里只有 flag/env。
package config

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

// Config 是 modelgate 的全部启动参数。
type Config struct {
	Listen string // 监听地址，如 127.0.0.1:8080
	DBPath string // SQLite 文件路径
	// AdminToken 为空时管理页不做鉴权（仅本机使用场景）；对外暴露前必须设置。
	AdminToken string
}

// Parse 解析 flag；同名环境变量 MODELGATE_* 作为默认值。
func Parse(args []string) (*Config, error) {
	home, _ := os.UserHomeDir()
	defDB := filepath.Join(home, ".modelgate", "modelgate.db")

	fs := flag.NewFlagSet("modelgate", flag.ContinueOnError)
	c := &Config{}
	fs.StringVar(&c.Listen, "listen", envOr("MODELGATE_LISTEN", "127.0.0.1:8080"), "监听地址")
	fs.StringVar(&c.DBPath, "db", envOr("MODELGATE_DB", defDB), "SQLite 数据库路径")
	fs.StringVar(&c.AdminToken, "admin-token", envOr("MODELGATE_ADMIN_TOKEN", ""), "管理页访问令牌；为空则不鉴权（仅限本机）")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if c.Listen == "" {
		return nil, fmt.Errorf("config: listen 不能为空")
	}
	return c, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
