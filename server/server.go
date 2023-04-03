package server

import (
	over "github.com/Trendyol/overlog"
	"github.com/gin-gonic/gin"
)

func Init() {
	gin.SetMode(gin.ReleaseMode)

	r := NewRouter()
	go func() {
		if err := r.Run(":8080"); err != nil {
			over.Log().Error(err)
			return
		}
	}()
}
