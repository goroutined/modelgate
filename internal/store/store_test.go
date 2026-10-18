package store

import (
	"path/filepath"
	"testing"
)

// 验证打开数据库即完成初始 migration，且重复打开幂等。
func TestOpenMigrates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var n int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if n == 0 {
		t.Fatal("初始 migration 未执行")
	}
	// 业务表存在
	for _, table := range []string{"upstreams", "upstream_keys", "virtual_keys", "policies", "calls", "chunks"} {
		var c int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&c); err != nil {
			t.Fatalf("表 %s 不可用: %v", table, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 二次打开不重复执行 migration
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	var n2 int
	s2.DB().QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&n2)
	if n2 != n {
		t.Fatalf("migration 被重复执行: %d -> %d", n, n2)
	}
}
