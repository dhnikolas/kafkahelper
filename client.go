package kafkahelper

import (
	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
)

type Client struct {
	BrokerList     []string
	kafkaVersion   string
	producerConfig *sarama.Config
	consumerConfig *sarama.Config
	syncProducer    sarama.SyncProducer
	asyncProducer   sarama.AsyncProducer
	tracer          opentracing.Tracer
}

func NewClient(brokerList []string) *Client {
	client := &Client{BrokerList: brokerList}
	client.kafkaVersion = "2.8.0"
	return client
}

func (c *Client) SetTracer (tracer opentracing.Tracer) {
	c.tracer = tracer
}

func (c *Client) SetVersion(version string) {
	c.kafkaVersion = version
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
