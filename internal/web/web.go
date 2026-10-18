// Package web 内嵌管理页静态资源，零构建步骤。
package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var staticFS embed.FS

// Handler 返回管理页静态文件服务（/ 映射到 index.html）。
func Handler() http.Handler {
	sub, _ := fs.Sub(staticFS, "static")
	return http.FileServerFS(sub)
}
