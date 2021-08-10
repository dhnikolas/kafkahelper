package kafkahelper

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

type produceTrace struct {
	rh []sarama.RecordHeader
}

func (c *Client) Inject (msg *sarama.ProducerMessage, span opentracing.Span)  {
	if c.tracer == nil {
		return
	}
	pt := &produceTrace{rh: msg.Headers}
	c.tracer.Inject(span.Context(), opentracing.TextMap, pt)
	msg.Headers = pt.GetHeaders()

	return
}

func (pt *produceTrace) SetHeaders(headers []sarama.RecordHeader) {
	pt.rh = headers
}

func (pt *produceTrace) GetHeaders() []sarama.RecordHeader {
	return pt.rh
}

func (pt *produceTrace) Set (key, value string) {
	pt.rh = append(pt.rh, sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(value),
	})
}

func (pt *produceTrace) ForeachKey (handler func(key, val string) error) error {
	for _, v := range pt.rh {
		err := handler(string(v.Key), string(v.Value))
		if err != nil {
			return err
		}
	}

	return nil
}

type consumeTrace struct {
	rh []*sarama.RecordHeader
}

func (c *Client) Extract (ctx context.Context, msg *sarama.ConsumerMessage) (opentracing.Span, context.Context, error){
	if c.tracer == nil {
		return nil, ctx, errors.New("Tracing not initialized ")
	}
	ct := &consumeTrace{rh: msg.Headers}
	spanCtx, _ := c.tracer.Extract(opentracing.TextMap, ct)
	span := c.tracer.StartSpan("Kafka consume " + msg.Topic, ext.RPCServerOption(spanCtx))
	ctx = opentracing.ContextWithSpan(ctx, span)

	return span, ctx, nil
}

func (pt *consumeTrace) SetHeaders(headers []*sarama.RecordHeader) {
	pt.rh = headers
}

func (pt *consumeTrace) GetHeaders() []*sarama.RecordHeader {
	return pt.rh
}

func (pt *consumeTrace) Set (key, value string) {
	pt.rh = append(pt.rh, &sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(value),
	})
}

func (pt *consumeTrace) ForeachKey (handler func(key, val string) error) error {
	for _, v := range pt.rh {
		err := handler(string(v.Key), string(v.Value))
		if err != nil {
			return err
		}
	}

	return nil
}