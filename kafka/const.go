package kafka

import (
	"github.com/Shopify/sarama"
	"time"
)

const (
	Stop = "Stop"
)

type ConnectionParameters struct {
	Conf            *sarama.Config
	Brokers         []string
	Topics          []string
	ConsumerGroupID string
	CurrentTime     time.Time
}
