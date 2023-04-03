package client

import (
	"kafka-shovel/configs"
	"kafka-shovel/kafka"
	"kafka-shovel/services"
	"kafka-shovel/util"
	"time"

	"github.com/Shopify/sarama"
	over "github.com/Trendyol/overlog"
	cronJob "github.com/robfig/cron"
)

type ConfluentKafka interface {
	Run()
}

type confluentKafka struct {
	config *configs.ApplicationConfig
}

type ShovelWithChannel struct {
	shovel  services.Shovel
	channel chan bool
}

func NewConfluentKafka(config *configs.ApplicationConfig) *confluentKafka {
	return &confluentKafka{
		config: config,
	}
}

func (c confluentKafka) Run() {
	// Run once at start
	shovels := getShovels(c.config.KafkaConfiguration)

	for _, shovel := range shovels {
		localShovel := shovel
		go c.runShovel(localShovel, time.Now())
		cron := cronJob.New()
		err := cron.AddFunc("@every "+localShovel.Period, func() {
			go c.runShovel(localShovel, time.Now())
		})
		if err != nil {
			return
		}
	}
}

func (c confluentKafka) runShovel(shovel services.Shovel, currentTime time.Time) {
	shovelWithChannel := ShovelWithChannel{
		channel: runKafkaShovelListener(c.config.KafkaConfiguration, shovel, currentTime),
		shovel:  shovel,
	}

	go func() {
		ticker := time.NewTicker(time.Duration(shovel.TaskRunDuration) * time.Second)
		<-ticker.C
		Close(shovelWithChannel.channel)
	}()
}

func Close(ch chan bool) {
	defer func() { recover() }()
	close(ch)
}

func runKafkaShovelListener(conf configs.KafkaConfig, shovel services.Shovel, currentTime time.Time) chan bool {
	notificationChannel := make(chan bool)
	config := kafka.ConnectionParameters{
		ConsumerGroupID: conf.GroupName,
		Conf:            KafkaConfig(conf.KafkaVersion, "clientId"),
		Brokers:         conf.Brokers,
		Topics:          []string{shovel.From},
		CurrentTime:     currentTime,
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		over.Log().Error(err)
		panic(err)
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		over.Log().Error(err)
		panic(err)
	}

	go func() {
		<-notificationChannel
		consumer.Unsubscribe()
	}()

	service := services.NewService(producer, shovel)
	handler := services.NewEventHandler(service, currentTime, notificationChannel)
	errChannel := consumer.Subscribe(handler)
	go func() {
		for e := range errChannel {
			over.Log().Info(e)
			_ = producer.Close()
		}
	}()
	over.Log().Infof("%s listener is starting ", shovel.From)
	return notificationChannel
}

func getShovels(conf configs.KafkaConfig) (result []services.Shovel) {
	config := sarama.NewConfig()
	v, err := sarama.ParseKafkaVersion(conf.KafkaVersion)
	if err != nil {
		panic(err)
	}
	config.Version = v
	client, err := sarama.NewClient(conf.Brokers, config)
	if err != nil {
		over.Log().Error(err)
		panic(err)
	}
	defer client.Close()

	topics, err := client.Topics()
	if err != nil {
		over.Log().Error(err)
		panic(err)
	}

	set := util.NewSet()
	set.AddCollection(topics)

	for _, topicConfig := range conf.Topics {
		var retryTopic = topicConfig.ToTopic
		var errorTopic = topicConfig.FromTopic
		if !set.Contains(retryTopic) || !set.Contains(errorTopic) {
			over.Log().Infof("Error on retry topic does not exists : %s", topicConfig.FromTopic)
			continue
		}

		result = append(result, services.Shovel{
			From:            errorTopic,
			To:              retryTopic,
			IsInfiniteRetry: topicConfig.IsInfinite,
			RetryCount:      topicConfig.RetryCount,
			TaskRunDuration: topicConfig.TaskRunDuration,
			Period:          topicConfig.Period,
		})

	}
	return
}

func KafkaConfig(version, clientId string) *sarama.Config {
	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		panic(err)
	}
	//consumer
	config := sarama.NewConfig()
	config.Version = v
	config.Consumer.Return.Errors = true
	config.ClientID = clientId

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 5 * time.Second

	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
	config.Consumer.Fetch.Default = 2048 * 1024

	//producer
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 5 * time.Second
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Timeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.DialTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second
	config.Producer.MaxMessageBytes = 2000000

	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 5 * time.Second

	return config
}
