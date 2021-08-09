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