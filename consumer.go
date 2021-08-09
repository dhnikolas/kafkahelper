package kafkahelper

import (
	"context"
	"github.com/Shopify/sarama"
)

type consumer struct {
	ready         chan bool
	exit          chan bool
	consumerGroup sarama.ConsumerGroup
	handler       ConsumeHandler
}

type ConsumeHandler interface {
	Receive(message *sarama.ConsumerMessage) error
}

func (c *Client) Consume(ctx context.Context, topic string, group string, handler ConsumeHandler) (*consumer, error) {
	if c.consumerConfig == nil {
		c.consumerConfig = sarama.NewConfig()
		version, err := sarama.ParseKafkaVersion(c.kafkaVersion)
		if err != nil {
			return nil, err
		}
		c.consumerConfig.Version = version
	}

	cg, err := sarama.NewConsumerGroup(c.BrokerList, group, c.consumerConfig)
	if err != nil {
		return nil, err
	}
	cons := &consumer{
		ready:         make(chan bool),
		exit:          make(chan bool),
		consumerGroup: cg,
		handler:       handler,
	}

	exitChan := make(chan bool)
	go func(ch chan bool) {
		defer func() {
			<-cons.exit
			exitChan <- true
		}()
		for {
			err := cons.consumerGroup.Consume(ctx, []string{topic}, cons)
			if err != nil {
				panic(err)
			}
			if ctx.Err() != nil {
				return
			}
			cons.ready = make(chan bool)
		}

	}(exitChan)
	return cons, nil
}

func (cons *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		err := cons.handler.Receive(message)
		if err == nil {
			session.MarkMessage(message, "")
		}
	}
	return nil
}

func (cons *consumer) Setup(sarama.ConsumerGroupSession) error {
	close(cons.ready)
	return nil
}

func (cons *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	cons.exit <- true
	return nil
}

func (cons *consumer) Cg() sarama.ConsumerGroup {
	return cons.consumerGroup
}

func (cons *consumer) Ready() chan bool {
	return cons.ready
}

func (cons *consumer) Exit() chan bool {
	return cons.exit
}

func (cons *consumer) Close() error {
	return cons.consumerGroup.Close()
}
