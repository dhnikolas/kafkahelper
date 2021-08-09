package kafkahelper

import "github.com/Shopify/sarama"

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

func (c *Client) SendSyncMsg(topic string, msg []byte) error {
	if c.syncProducer == nil {
		sp, err := c.GetSyncProducer()
		if err != nil {
			return err
		}
		c.syncProducer = sp
	}
	newMsg := NewMsg(topic, msg)
	_, _, err := c.syncProducer.SendMessage(newMsg)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) SendAsyncMsg(topic string, msg []byte) error {
	if c.asyncProducer == nil {
		ap, err := c.GetAsyncProducer()
		if err != nil {
			return err
		}
		c.asyncProducer = ap
	}
	newMsg := NewMsg(topic, msg)
	c.asyncProducer.Input() <- newMsg
	return nil
}