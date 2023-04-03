package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	over "github.com/Trendyol/overlog"
)

type Consumer interface {
	Subscribe(handler EventHandler) chan error
	Unsubscribe()
}

type EventHandler interface {
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
	ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error
}

type kafkaConsumer struct {
	topic         []string
	consumerGroup sarama.ConsumerGroup
}

func NewConsumer(connectionParams ConnectionParameters) (Consumer, error) {
	cg, err := sarama.NewConsumerGroup(connectionParams.Brokers, connectionParams.ConsumerGroupID, connectionParams.Conf)
	if err != nil {
		return nil, err
	}

	return &kafkaConsumer{
		topic:         connectionParams.Topics,
		consumerGroup: cg,
	}, nil
}

func (c *kafkaConsumer) Subscribe(handler EventHandler) chan error {
	consumerError := make(chan error)
	ctx := context.Background()

	go func() {
		for {
			if err := c.consumerGroup.Consume(ctx, c.topic, handler); err != nil {
				consumerError <- err
				return
			}

			if ctx.Err() != nil {
				consumerError <- ctx.Err()
				return
			}
		}
	}()
	go func() {
		for err := range c.consumerGroup.Errors() {
			over.Log().Error("Error from consumer group : ", err.Error())
		}
	}()
	return consumerError
}

func (c *kafkaConsumer) Unsubscribe() {
	if err := c.consumerGroup.Close(); err != nil {
		over.Log().Warnf("Client wasn't closed ", err)
	}

	over.Log().Infof("Kafka consumer closed")
}
