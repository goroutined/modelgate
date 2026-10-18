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
	db *sql.DB
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
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

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
