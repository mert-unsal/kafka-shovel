package configs

import (
	over "github.com/Trendyol/overlog"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

type KafkaCredentials struct {
	StretchKafkaUsername string
	StretchKafkaPassword string
}

type KafkaConfig struct {
	Topics       []TopicConfig
	Brokers      []string
	KafkaVersion string
	GroupName    string
}

type TopicConfig struct {
	ToTopic         string
	FromTopic       string
	RetryCount      int
	IsInfinite      bool
	TaskRunDuration int    `validate:"required"`
	Period          string `validate:"required"`
}

type ApplicationConfig struct {
	KafkaConfiguration KafkaConfig
	Credentials        KafkaCredentials
}

type configurationManager struct {
	config ApplicationConfig
}

func NewConfigurationManager() *configurationManager {
	return &configurationManager{
		config: loadConfig(),
	}
}

func (r configurationManager) GetConfig() *ApplicationConfig {
	return &r.config
}

func loadConfig() ApplicationConfig {
	applicationConfigReader := viper.New()
	applicationConfigReader.SetConfigName("config")
	applicationConfigReader.SetConfigType("json")
	applicationConfigReader.AddConfigPath("./configs")
	err := applicationConfigReader.ReadInConfig()
	if err != nil {
		over.Log().Error(err)
		panic(err)
	}

	config := ApplicationConfig{}
	err = applicationConfigReader.Unmarshal(&config)
	if err != nil {
		return ApplicationConfig{}
	}
	validate := validator.New()
	err = validate.Struct(config)
	if err != nil {
		over.Log().Error(err)
		panic(err)
	}

	kafkaSecretReader := viper.New()
	kafkaSecretReader.SetConfigName("secret")
	kafkaSecretReader.SetConfigType("json")
	kafkaSecretReader.AddConfigPath("./configs")
	err = kafkaSecretReader.ReadInConfig()
	if err != nil {
		over.Log().Error(err)
		panic(err)
	}

	config.Credentials.StretchKafkaUsername = kafkaSecretReader.GetString("stretchKafkaUsername")
	config.Credentials.StretchKafkaPassword = kafkaSecretReader.GetString("stretchKafkaPassword")
	return config

}
