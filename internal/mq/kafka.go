package mq

import (
	"context"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"xzdp/internal/store"
)

type Queue struct {
	*kgo.Client
	Topic string
}

func Open(topic string, consumer bool) (*Queue, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(store.Env("KAFKA_BROKERS", "127.0.0.1:9092"), ",")...),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
		kgo.RecordDeliveryTimeout(5 * time.Second),
	}
	if consumer {
		opts = append(opts, kgo.ConsumeTopics(topic), kgo.ConsumerGroup(topic+".workers"), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	}
	c, e := kgo.NewClient(opts...)
	if e != nil {
		return nil, e
	}
	return &Queue{c, topic}, nil
}

func (q *Queue) Publish(ctx context.Context, key string, value []byte) error {
	return q.ProduceSync(ctx, &kgo.Record{Topic: q.Topic, Key: []byte(key), Value: value}).FirstErr()
}
