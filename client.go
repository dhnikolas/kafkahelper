package kafkahelper

import (
	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	"sync"
)

type Client struct {
	BrokerList     []string
	SdkClient      sarama.Client
	producerConfig *sarama.Config
	consumerConfig *sarama.Config
	syncProducer    sarama.SyncProducer
	tracer          opentracing.Tracer
	m               *sync.Mutex
}

func NewClient(brokerList []string, kafkaConfig *sarama.Config) (*Client, error) {
	if kafkaConfig == nil {
		kafkaConfig = sarama.NewConfig()
		version, err := sarama.ParseKafkaVersion("2.8.0")
		if err != nil {
			return nil, err
		}
		kafkaConfig.Version = version
		kafkaConfig.Consumer.Return.Errors = true
		kafkaConfig.Producer.Return.Successes = true
	}
	c, err := sarama.NewClient(brokerList, kafkaConfig)
	if err != nil {
		return nil, err
	}
	client := &Client{BrokerList: brokerList, SdkClient: c}

	return client, nil
}

func (c *Client) SetTracer (tracer opentracing.Tracer) {
	c.tracer = tracer
}

func (c *Client) SetConsumerConfig(config *sarama.Config) {
	c.consumerConfig = config
}

func (c *Client) SetProducerConfig(config *sarama.Config) {
	c.producerConfig = config
}

func NewMsg(topic string, message []byte) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	return msg
}
