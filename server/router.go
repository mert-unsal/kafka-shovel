package server

import (
	"github.com/gin-gonic/gin"
	"kafka-shovel/handler"
)

func NewRouter() *gin.Engine {
	router := gin.New()

	healthCheckHandler := handler.NewHealthCheckHandler()

	router.GET("/_monitoring/ready", healthCheckHandler.HealthCheck)
	router.GET("/_monitoring/live", healthCheckHandler.HealthCheck)

	return router

}
