package trade

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"xzdp/internal/observe"
)

func sampleLag(ctx context.Context, client *kafka.Client, topic string) (int64, error) {
	metadata, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return 0, err
	}
	if len(metadata.Topics) != 1 || len(metadata.Topics[0].Partitions) == 0 {
		return 0, fmt.Errorf("no partitions for %s", topic)
	}
	var partitions []int
	var offsets []kafka.OffsetRequest
	for _, p := range metadata.Topics[0].Partitions {
		partitions = append(partitions, p.ID)
		offsets = append(offsets, kafka.LastOffsetOf(p.ID), kafka.FirstOffsetOf(p.ID))
	}
	group, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: topic + "-db", Topics: map[string][]int{topic: partitions}})
	if err != nil {
		return 0, err
	}
	if group.Error != nil {
		return 0, group.Error
	}
	ends, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: offsets}})
	if err != nil {
		return 0, err
	}
	committed := make(map[int]int64)
	for _, p := range group.Topics[topic] {
		if p.Error != nil {
			return 0, p.Error
		}
		committed[p.Partition] = p.CommittedOffset
	}
	var lag int64
	for _, p := range ends.Topics[topic] {
		if p.Error != nil {
			return 0, p.Error
		}
		lag += max(0, p.LastOffset-max(committed[p.Partition], p.FirstOffset))
	}
	return lag, nil
}

func monitorLag(ctx context.Context, client *kafka.Client, topic string) error {
	for ctx.Err() == nil {
		if lag, err := sampleLag(ctx, client, topic); err == nil {
			observe.KafkaLag.Set(float64(lag))
			observe.LagSampleTime.SetToCurrentTime()
		}
		pause(ctx, 5*time.Second)
	}
	return ctx.Err()
}
