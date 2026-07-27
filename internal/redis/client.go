package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/Logiqode/ThemeParkNFT/internal/config"
)

type Client struct {
	rdb *redis.Client
	cfg config.RedisConfig
}

func NewClient(cfg config.RedisConfig) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr(),
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	return &Client{rdb: rdb, cfg: cfg}
}

// IsDuplicate checks if a trace_id has already been processed (idempotency shield).
// Returns true if the key already exists (duplicate).
func (c *Client) IsDuplicate(ctx context.Context, traceID string) (bool, error) {
	key := fmt.Sprintf("dedup:%s", traceID)
	ok, err := c.rdb.SetNX(ctx, key, "1", time.Duration(c.cfg.DedupTTLSec)*time.Second).Result()
	if err != nil {
		return false, fmt.Errorf("redis setnx dedup: %w", err)
	}
	return !ok, nil // SetNX returns true if key was SET; false = already exists = duplicate
}

// SetGraceWindow caches a gate verify result for the retry grace window (5s).
func (c *Client) SetGraceWindow(ctx context.Context, ticketID string, result string) error {
	key := fmt.Sprintf("grace:%s", ticketID)
	return c.rdb.Set(ctx, key, result, time.Duration(c.cfg.GraceWindowSec)*time.Second).Err()
}

// GetGraceWindow retrieves the cached result for a ticket within the grace window.
func (c *Client) GetGraceWindow(ctx context.Context, ticketID string) (string, error) {
	key := fmt.Sprintf("grace:%s", ticketID)
	val, err := c.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

// AddRideToUserSet adds a ride_id to a user's daily aggregated set.
func (c *Client) AddRideToUserSet(ctx context.Context, userID, rideID, date string) error {
	key := fmt.Sprintf("user:%s:rides:%s", userID, date)
	if err := c.rdb.SAdd(ctx, key, rideID).Err(); err != nil {
		return fmt.Errorf("sadd ride set: %w", err)
	}
	return c.rdb.Expire(ctx, key, time.Duration(c.cfg.AggTTLSec)*time.Second).Err()
}

// GetUserRides returns all ride_ids for a user on a given date.
func (c *Client) GetUserRides(ctx context.Context, userID, date string) ([]string, error) {
	key := fmt.Sprintf("user:%s:rides:%s", userID, date)
	return c.rdb.SMembers(ctx, key).Result()
}

// Ping checks connectivity.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Close shuts down the Redis client.
func (c *Client) Close() error {
	log.Info().Msg("redis client closing")
	return c.rdb.Close()
}
