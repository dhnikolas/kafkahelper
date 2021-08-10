package kafkahelper

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
)

func (c *Client) GetAsyncProducer() (sarama.AsyncProducer, error){
	if c.producerConfig == nil {
		c.producerConfig = sarama.NewConfig()
		c.producerConfig.Producer.Return.Successes = true
	}

	producer, err := sarama.NewAsyncProducer(c.BrokerList, c.producerConfig)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func (c *Client) GetSyncProducer() (sarama.SyncProducer, error){
	if c.producerConfig == nil {
		c.producerConfig = sarama.NewConfig()
		c.producerConfig.Producer.Return.Successes = true
	}

	producer, err := sarama.NewSyncProducer(c.BrokerList, c.producerConfig)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func (c *Client) SendSyncMsg(ctx context.Context, topic string, msg []byte) error {
	if c.syncProducer == nil {
		sp, err := c.GetSyncProducer()
		if err != nil {
			return err
		}
		c.syncProducer = sp
	}
	newMsg := NewMsg(topic, msg)
	span, ctx := opentracing.StartSpanFromContext(ctx, "Produce message to topic " + topic)
	defer span.Finish()
	if c.tracer != nil {
		c.Inject(newMsg, span)
	}
	_, _, err := c.syncProducer.SendMessage(newMsg)
	if err != nil {
		return err
	}

	return nil
}
