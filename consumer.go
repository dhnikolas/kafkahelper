package kafkahelper

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
)

type consumer struct {
	ready         chan bool
	exit          chan bool
	consumerGroup sarama.ConsumerGroup
	handler       ConsumeHandler
	c             *Client
}

type ConsumeHandler interface {
	Receive(ctx context.Context, message *sarama.ConsumerMessage) error
	StopReceive (sarama.ConsumerGroupSession) error
	Errors(err error)
}

func (c *Client) Consume(ctx context.Context, topic string, group string, handler ConsumeHandler) (*consumer, error) {
	cg, err := sarama.NewConsumerGroupFromClient(group, c.SdkClient)
	if err != nil {
		return nil, err
	}
	cons := &consumer{
		ready:         make(chan bool),
		exit:          make(chan bool),
		consumerGroup: cg,
		handler:       handler,
		c:             c,
	}

	go func(ch <-chan error, consumeHandler ConsumeHandler) {
		for e := range ch {
			consumeHandler.Errors(e)
		}
	}(cg.Errors(), handler)

	go func() {
		defer func() {
			cons.exit <- true
			fmt.Println("Exiting from listening topic " + topic)
		}()
		for {
			err := cons.consumerGroup.Consume(ctx, []string{topic}, cons)
			if err != nil {
				panic(err)
			}
			if ctx.Err() != nil {
				fmt.Println(ctx.Err())
				return
			}
			cons.ready = make(chan bool)
		}
	}()
	return cons, nil
}

func (cons *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		span, ctx, _ := cons.c.Extract(context.Background(), message)
		err := cons.handler.Receive(ctx, message)
		if err == nil {
			session.MarkMessage(message, "")
		}
		if span != nil {
			span.Finish()
		}
	}
	return nil
}

func (cons *consumer) Setup(sarama.ConsumerGroupSession) error {
	close(cons.ready)
	return nil
}

func (cons *consumer) Cleanup(s sarama.ConsumerGroupSession) error {
	return cons.handler.StopReceive(s)
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
