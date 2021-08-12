package kafkahelper

import (
	"errors"
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

func NewClient(brokerList []string, config *sarama.Config) (*Client, error) {
	if config == nil {
		return nil, errors.New("Kafka config is not set ")
	}
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true
	c, err := sarama.NewClient(brokerList, config)
	if err != nil {
		return nil, err
	}
	m := &sync.Mutex{}
	client := &Client{BrokerList: brokerList, SdkClient: c, m: m}

	return client, nil
}

func DefaultConfig() *sarama.Config {
	config := sarama.NewConfig()
	version, _ := sarama.ParseKafkaVersion("2.8.0")
	config.Version = version

	return config
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
