package main

import (
	over "github.com/Trendyol/overlog"
	"github.com/gin-gonic/gin"
	"kafka-shovel/configs"
	"kafka-shovel/kafka/client"
)

func main() {

	over.NewDefault()
	manager := configs.NewConfigurationManager()

	kafkaConfig := manager.GetConfig()

	kafka := client.NewConfluentKafka(kafkaConfig)
	kafka.Run()

	over.Log().Info("Confluent Kafka broker %s ", kafkaConfig.KafkaConfiguration.Brokers)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.GET("/_monitoring/ready", healthCheck)
	r.GET("/_monitoring/live", healthCheck)

	appPort := "8085"

	if err := r.Run(":" + appPort); err != nil {
		over.Log().Error(err)
		return
	}

	over.Log().Infof("Kafka Shovel is running in %s", appPort)

}

func healthCheck(c *gin.Context) {
	c.JSON(200, "Healthy")
}
