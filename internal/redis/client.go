package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Sentinel errors for the wristband binding state machine (R19). The atomic
// Lua bind script translates Redis error replies into these typed errors so the
// gate service can distinguish a replayed/conflicting bind from infra failure.
var (
	ErrWristbandAlreadyBound = errors.New("wristband already bound")
	ErrTicketAlreadyBound    = errors.New("ticket already bound")
	ErrBindingNotFound       = errors.New("binding not found")
)

// bindScript atomically double-SETNXs the forward wristband→binding key and the
// reverse ticket→uid key, both with the same EXPIREAT (end of Day+1, R19). Either
// key already existing (wristband in use OR ticket in use) aborts the whole bind.
//
// KEYS[1] = bind:wristband:{uid}
// KEYS[2] = bind:ticket:{ticket_id}
// ARGV[1] = JSON binding value
// ARGV[2] = expireat (unix seconds, end of Day+1)
// ARGV[3] = wristband uid (value stored in the reverse index)
var bindScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then
  return redis.error_reply('ticket already bound')
end
if not redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EXAT', ARGV[2]) then
  return redis.error_reply('wristband already bound')
end
redis.call('SET', KEYS[2], ARGV[3], 'EXAT', ARGV[2])
return redis.status_reply('OK')
`)

// deleteBindingScript atomically reads the binding's ticket_id then DELs both
// the forward and reverse keys, so unbind never leaves a dangling index.
//
// KEYS[1] = bind:wristband:{uid}
// KEYS[2] = bind:ticket:{ticket_id}
var deleteBindingScript = redis.NewScript(`
local tid = redis.call('GET', KEYS[1])
redis.call('DEL', KEYS[1])
if tid then
  local decoded = cjson.decode(tid)
  if decoded and decoded['ticket_id'] then
    redis.call('DEL', 'bind:ticket:' .. decoded['ticket_id'])
  end
end
return redis.status_reply('OK')
`)

// bindingExpiry returns the end of Day+1 (visitor's visit day is "today"; the
// disposable wristband link must expire at the close of the following day).
func bindingExpiry() time.Time {
	now := time.Now()
	// start of day+2 minus 1ns == end of day+1.
	end := time.Date(now.Year(), now.Month(), now.Day()+2, 0, 0, 0, 0, now.Location())
	return end.Add(-time.Nanosecond)
}

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

// bindingKey builds the forward wristband→binding Redis key.
func bindingKey(uid string) string { return "bind:wristband:" + uid }

// ticketKey builds the reverse ticket→uid Redis key.
func ticketKey(ticketID string) string { return "bind:ticket:" + ticketID }

// BindWristband atomically links a wristband NFC uid to a ticket + user email
// (R19). Fails with ErrWristbandAlreadyBound or ErrTicketAlreadyBound if either
// side is already bound. The link is disposable: it expires at end of Day+1.
func (c *Client) BindWristband(ctx context.Context, uid, ticketID, userEmail string) error {
	b := models.WristbandBinding{
		WristbandUID: uid,
		TicketID:     ticketID,
		UserEmail:    userEmail,
		Status:       models.BindingStatusBinding,
		BoundAt:      time.Now().Unix(),
	}
	val, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("marshal binding: %w", err)
	}
	exp := bindingExpiry().Unix()
	err = bindScript.Run(ctx, c.rdb,
		[]string{bindingKey(uid), ticketKey(ticketID)},
		val, exp, uid).Err()
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "ticket already bound"):
			return ErrTicketAlreadyBound
		case strings.Contains(err.Error(), "wristband already bound"):
			return ErrWristbandAlreadyBound
		}
		return fmt.Errorf("bind wristband: %w", err)
	}
	return nil
}

// GetBinding returns the binding for a wristband uid, or nil if not bound.
func (c *Client) GetBinding(ctx context.Context, uid string) (*models.WristbandBinding, error) {
	val, err := c.rdb.Get(ctx, bindingKey(uid)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get binding: %w", err)
	}
	var b models.WristbandBinding
	if err := json.Unmarshal([]byte(val), &b); err != nil {
		return nil, fmt.Errorf("unmarshal binding: %w", err)
	}
	return &b, nil
}

// GetBindingByTicket resolves a binding by ticket_id via the reverse index.
func (c *Client) GetBindingByTicket(ctx context.Context, ticketID string) (*models.WristbandBinding, error) {
	uid, err := c.rdb.Get(ctx, ticketKey(ticketID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get binding by ticket: %w", err)
	}
	return c.GetBinding(ctx, uid)
}

// SetBindingStatus advances a binding's status (BINDING → BOUND) preserving its
// original TTL (KEEPTTL), so promotion never extends the disposable link's life.
func (c *Client) SetBindingStatus(ctx context.Context, uid string, status models.BindingStatus) error {
	b, err := c.GetBinding(ctx, uid)
	if err != nil {
		return err
	}
	if b == nil {
		return ErrBindingNotFound
	}
	b.Status = status
	val, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("marshal binding: %w", err)
	}
	// SET ... KEEPTTL requires Redis 6.0+ (redis:7-alpine satisfies this).
	if err := c.rdb.SetArgs(ctx, bindingKey(uid), val, redis.SetArgs{KeepTTL: true}).Err(); err != nil {
		return fmt.Errorf("set binding status: %w", err)
	}
	return nil
}

// DeleteBinding removes a wristband binding and its reverse index atomically.
func (c *Client) DeleteBinding(ctx context.Context, uid string) error {
	if err := deleteBindingScript.Run(ctx, c.rdb, []string{bindingKey(uid)}).Err(); err != nil {
		return fmt.Errorf("delete binding: %w", err)
	}
	return nil
}

// MarkQRUsed consumes a one-time QR token (R21). Returns true if the token was
// unused (i.e. now consumed), false if it was already used (replay).
func (c *Client) MarkQRUsed(ctx context.Context, uuid string) (bool, error) {
	// TTL matches QR rotation (30s) plus a small skew allowance; a replayed QR
	// inside the rotation window is rejected because SETNX already set the key.
	exp := time.Duration(c.cfg.GraceWindowSec+30) * time.Second
	ok, err := c.rdb.SetNX(ctx, "qr:"+uuid, "1", exp).Result()
	if err != nil {
		return false, fmt.Errorf("mark qr used: %w", err)
	}
	return ok, nil
}

// ClearDedup removes a dedup marker (R25 compensation). Called on transient
// downstream failure so the retry reprocesses the trace_id instead of dropping it.
func (c *Client) ClearDedup(ctx context.Context, traceID string) error {
	if err := c.rdb.Del(ctx, "dedup:"+traceID).Err(); err != nil {
		return fmt.Errorf("clear dedup: %w", err)
	}
	return nil
}

// SetWristbandGrace caches an NFC-check decision keyed by wristband (R24). The
// value is opaque JSON; the gate service re-forwards it verbatim on a cache hit
// so a cached "denied" is never replayed as "allowed" (D1 fix).
func (c *Client) SetWristbandGrace(ctx context.Context, uid, decision string) error {
	key := "gracewb:" + uid
	return c.rdb.Set(ctx, key, decision, time.Duration(c.cfg.GraceWindowSec)*time.Second).Err()
}

// GetWristbandGrace returns the cached NFC-check decision for a wristband, or ""
// if none is cached (or it expired).
func (c *Client) GetWristbandGrace(ctx context.Context, uid string) (string, error) {
	key := "gracewb:" + uid
	val, err := c.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

// ClearWristbandGrace removes a cached NFC-check decision (used on reset so a
// stale decision for a re-bound wristband is never replayed).
func (c *Client) ClearWristbandGrace(ctx context.Context, uid string) error {
	key := "gracewb:" + uid
	return c.rdb.Del(ctx, key).Err()
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
