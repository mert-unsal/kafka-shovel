package services

import (
	over "github.com/Trendyol/overlog"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"kafka-shovel/kafka"
)

const runningKeyHeader = "shovel_running_key"

type eventHandler struct {
	runningKey      string
	service         Service
	currentTime     time.Time
	consumerChannel chan bool
}

func NewEventHandler(service Service, time time.Time, channel chan bool) kafka.EventHandler {
	return &eventHandler{
		service:         service,
		runningKey:      uuid.New().String(),
		currentTime:     time,
		consumerChannel: channel,
	}
}

func Close(ch chan bool) {
	defer func() { recover() }()
	close(ch)
}

func (e *eventHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (e *eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (e *eventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	limitTime := e.currentTime.Add(-time.Minute * 1)
	over.Log().Infof("Go Routine starting time : %s", limitTime)
	over.Log().Infof("Received messages after time : %s will not be processed.", limitTime)

	for message := range claim.Messages() {
		if limitTime.Before(message.Timestamp) {
			over.Log().Infof("CONSUMER WILL BE STOPPED IMMEDIATELY")
			Close(e.consumerChannel)
			return nil
		}
		over.Log().Infof("Received message time : %s, Received key: %s, message: %s, topic: %s \n", message.Timestamp.String(), string(message.Key), message.Value, message.Topic)
		if e.doesMessageProcessed(message) {
			over.Log().Warnf("Message already is processed. Shovel execution halted. key %s, message: %s, topic: %s \n", string(message.Key), message.Value, message.Topic)
			return nil
		}
		e.setMessageProcessed(message)
		err := e.service.OperateEvent(message)
		if err != nil {
			over.Log().Error("Error executing err: ", err)
		}

		session.MarkMessage(message, "")
	}
	return nil
}

func (e *eventHandler) setMessageProcessed(message *sarama.ConsumerMessage) {
	message.Headers = append(message.Headers, &sarama.RecordHeader{
		Key:   []byte(runningKeyHeader),
		Value: []byte(e.runningKey),
	})
}

func (e *eventHandler) doesMessageProcessed(message *sarama.ConsumerMessage) bool {
	for i := 0; i < len(message.Headers); i++ {
		if string(message.Headers[i].Key) == runningKeyHeader {
			if string(message.Headers[i].Value) == e.runningKey {
				return true
			} else {
				message.Headers[i].Value = []byte(e.runningKey)
			}
		}
	}
	return false
}
