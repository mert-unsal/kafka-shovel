package handler

import "github.com/gin-gonic/gin"

type HealthHandler interface {
	HealthCheck(c *gin.Context)
}

type healthHandler struct{}

func (h healthHandler) HealthCheck(c *gin.Context) {
	c.JSON(200, "health")
}

func NewHealthCheckHandler() HealthHandler {
	return &healthHandler{}
}
