// Package store 负责 SQLite 的打开、migrations 与保留策略清理。
// 使用纯 Go 驱动 modernc.org/sqlite，保证 go install 交叉编译无需 CGO。
package store

import (
	"database/sql"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Store 持有数据库句柄；所有表访问通过它进行。
type Store struct {
	db   *sql.DB
	path string
	raw  *sql.DB // 结构删除专用连接：关闭外键检查，见 openRaw
}

// Open 打开（必要时创建）数据库并执行 migrations。
// 文件所在目录会自动创建；启用 WAL 与外键约束。
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("store: db path 为空")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("store: 创建数据库目录 %s: %w", dir, err)
		}
	}
	// _pragma 通过 DSN 生效：WAL 提升并发写读，foreign_keys 保证级联删除
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: 打开 %s: %w", path, err)
	}
	db.SetMaxOpenConns(1) // SQLite 单写者，串行化避免 SQLITE_BUSY
	raw, err := openRaw(path)
	if err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db, path: path, raw: raw}
	if err := s.migrate(); err != nil {
		db.Close()
		raw.Close()
		return nil, err
	}
	// 启动兜底：上次后台清理没跑完（进程中途重启）留下的孤儿调用，继续在后台清
	go s.purgeOrphanCalls()
	return s, nil
}

func (s *Store) Close() error {
	if s.raw != nil {
		s.raw.Close()
	}
	return s.db.Close()
}

// openRaw 打开一条关闭外键检查的连接，只用于「结构删除」（删 key / 服务商 / vKey 自身那一行）。
// 原因：唯一的普通连接下，开着外键删一行就得把引用它的历史调用逐行改写——每条调用带 MB 级 body，
// 上万行会把连接占住几分钟，期间所有 API 排队（表现为删除后页面不更新、刷新白屏）。
// 代价是 calls 里留下悬空的历史 id；所有关联查询都是 LEFT JOIN，展示为未知即可。
func openRaw(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(0)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: 打开结构删除连接 %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// migrate 按文件名顺序执行 migrations/*.sql；已执行的记录在 schema_migrations。
func (s *Store) migrate() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		name TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("store: 初始化 schema_migrations: %w", err)
	}
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		var count int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE name = ?`, name).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			continue
		}
		body, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(string(body)); err != nil {
			tx.Rollback()
			return fmt.Errorf("store: 执行 migration %s: %w", name, err)
		}
		if _, err := tx.Exec(`INSERT INTO schema_migrations(name, applied_at) VALUES(?, ?)`, name, time.Now().Unix()); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// DB 暴露底层句柄，供上层构建查询；日常应优先走 Store 方法。
func (s *Store) DB() *sql.DB { return s.db }
