package trade

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

func (s Sale) persist(ctx context.Context, o Order) error {
	_, err := s.DB.ExecContext(ctx, "SELECT persist_voucher_order($1,$2,$3)", o.ID, o.User, o.Voucher)
	return err
}

func (s Sale) ack(ctx context.Context, id string) error {
	_, err := s.Redis.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HDel(ctx, s.Prefix+"pending", id)
		p.ZRem(ctx, s.Prefix+"relay", id)
		return nil
	})
	return err
}

func (s Sale) relay(ctx context.Context, w *kafka.Writer, topic string) error {
	ids, err := s.Redis.ZRangeByScore(ctx, s.Prefix+"relay", &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(time.Now().Unix(), 10), Count: 100}).Result()
	if err != nil {
		return err
	}
	msgs := make([]kafka.Message, 0, len(ids))
	for _, id := range ids {
		o, err := s.pending(ctx, id)
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		body, _ := json.Marshal(o)
		msgs = append(msgs, kafka.Message{Topic: topic, Key: []byte(id), Value: body})
	}
	if len(msgs) == 0 {
		return nil
	}
	if err = w.WriteMessages(ctx, msgs...); err != nil {
		return err
	}
	// Pending stays durable until PostgreSQL commits. Delayed redelivery also recovers a lost Kafka record.
	p := s.Redis.Pipeline()
	for _, m := range msgs {
		p.ZAddXX(ctx, s.Prefix+"relay", &redis.Z{Score: float64(time.Now().Add(time.Minute).Unix()), Member: string(m.Key)})
	}
	_, err = p.Exec(ctx)
	return err
}

func (s Sale) consume(ctx context.Context, r *kafka.Reader, w *kafka.Writer, topic string) error {
	for ctx.Err() == nil {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if !pause(ctx, time.Second) {
				break
			}
			continue
		}
		var o Order
		decode := json.Unmarshal(msg.Value, &o)
		if decode == nil && (o.ID == "" || o.User <= 0 || o.Voucher <= 0) {
			decode = errors.New("invalid order event")
		}
		applied := false
		for attempt := 0; decode == nil && attempt < 3; attempt++ {
			err = s.persist(ctx, o)
			if err == nil {
				applied = true
				break
			}
			if !pause(ctx, time.Duration(attempt+1)*100*time.Millisecond) {
				return ctx.Err()
			}
		}
		if !applied {
			if decode != nil {
				err = decode
			}
			reason := err.Error()
			for ctx.Err() == nil {
				err = w.WriteMessages(ctx, kafka.Message{Topic: topic + ".dlq", Key: msg.Key, Value: msg.Value, Headers: []kafka.Header{{Key: "reason", Value: []byte(reason)}}})
				if err == nil && decode == nil {
					_, err = s.DB.ExecContext(ctx, "INSERT INTO trade_dead_letter(order_id,voucher_id,user_id,reason) VALUES($1,$2,$3,LEFT($4,255)) ON CONFLICT(order_id) DO UPDATE SET reason=EXCLUDED.reason", o.ID, o.Voucher, o.User, reason)
				}
				if err == nil {
					break
				}
				pause(ctx, time.Second)
			}
		}
		if decode == nil {
			for ctx.Err() == nil {
				if applied {
					err = s.ack(ctx, o.ID)
				} else {
					err = s.Redis.ZRem(ctx, s.Prefix+"relay", o.ID).Err()
				}
				if err == nil {
					break
				}
				pause(ctx, time.Second)
			}
		}
		for ctx.Err() == nil {
			if err = r.CommitMessages(ctx, msg); err == nil {
				break
			}
			pause(ctx, time.Second)
		}
	}
	return ctx.Err()
}
func pause(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func (s Sale) Run(ctx context.Context, broker, env string) error {
	topic := "xzdp-" + env + "-orders"
	admin := kafka.Client{Addr: kafka.TCP(broker), Timeout: 3 * time.Second}
	for {
		reply, err := admin.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}, {Topic: topic + ".dlq", NumPartitions: 1, ReplicationFactor: 1}}})
		if err == nil {
			for _, e := range reply.Errors {
				if e != nil && !errors.Is(e, kafka.TopicAlreadyExists) {
					err = e
					break
				}
			}
		}
		if err == nil {
			break
		}
		slog.Warn("prepare trade topics", "error", err)
		if !pause(ctx, time.Second) {
			return ctx.Err()
		}
	}
	w := &kafka.Writer{Addr: kafka.TCP(broker), RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true, BatchTimeout: 5 * time.Millisecond, WriteTimeout: 3 * time.Second}
	defer w.Close()
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: topic, GroupID: topic + "-db", MaxWait: 100 * time.Millisecond, WatchPartitionChanges: true, PartitionWatchInterval: time.Second, MinBytes: 1, MaxBytes: 1048576})
	defer r.Close()
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return monitorLag(ctx, &admin, topic) })
	g.Go(func() error { return s.consume(ctx, r, w, topic) })
	g.Go(func() error {
		for ctx.Err() == nil {
			if err := s.relay(ctx, w, topic); err != nil && ctx.Err() == nil {
				slog.Warn("order relay", "error", err)
			}
			pause(ctx, 50*time.Millisecond)
		}
		return ctx.Err()
	})
	return g.Wait()
}
