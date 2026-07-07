//go:build integration

package integration

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
)

func pingMySQL(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

func pingKafka(brokers []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], "kafka-ping", 0)
	if err != nil {
		// fall back to a plain dial
		c, e := kafka.DialContext(ctx, "tcp", brokers[0])
		if e != nil {
			return err
		}
		_ = c.Close()
		return nil
	}
	_ = conn.Close()
	return nil
}
