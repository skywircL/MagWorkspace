// Package redisstate holds reconstructible dispatch and short-lived UI state.
// MySQL remains the authority for ownership, execution leases and results.
package redisstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"magworkspace/internal/domain"
)

var ErrUnavailable = errors.New("redis unavailable")
var ErrHistory = errors.New("event history unavailable")

const EventLimit = 4096
const stateTTL = 30 * time.Minute

type Client struct {
	r      *redis.Client
	prefix string
}

func New(addr, password, prefix string) *Client {
	return &Client{r: redis.NewClient(&redis.Options{Addr: addr, Password: password, MaxRetries: -1, DialTimeout: 500 * time.Millisecond, ReadTimeout: 500 * time.Millisecond, WriteTimeout: 500 * time.Millisecond, ContextTimeoutEnabled: true, PoolSize: 16}), prefix: prefix}
}
func (c *Client) Close() error { return c.r.Close() }
func (c *Client) Ping(ctx context.Context) error {
	if c.r.Ping(ctx).Err() != nil {
		return ErrUnavailable
	}
	return nil
}

// Bounded deduplication is atomic with dispatch. A popped notification is not
// an acknowledgement of work: unclaimed/expired MySQL intents are reconciled.
var enqueue = redis.NewScript(`
for i=1,#ARGV,2 do
 if redis.call('ZCARD',KEYS[1]) >= 1024 then break end
 redis.call('ZADD',KEYS[1],'NX',ARGV[i],ARGV[i+1])
end
redis.call('EXPIRE',KEYS[1],3600)
return 1`)

func (c *Client) Dispatch(ctx context.Context, profile string, jobs []domain.IndexJob) error {
	if len(jobs) == 0 {
		return nil
	}
	args := make([]any, 0, len(jobs)*2)
	for _, job := range jobs {
		args = append(args, job.CreatedAt.UnixMilli(), job.ID)
	}
	if enqueue.Run(ctx, c.r, []string{c.prefix + "queue:" + profile}, args...).Err() != nil {
		return ErrUnavailable
	}
	return nil
}
func (c *Client) Pop(ctx context.Context, profile string) (string, error) {
	rows, err := c.r.ZPopMin(ctx, c.prefix+"queue:"+profile, 1).Result()
	if err != nil {
		return "", ErrUnavailable
	}
	if len(rows) == 0 {
		return "", nil
	}
	return rows[0].Member.(string), nil
}
func (c *Client) progressKey(job domain.IndexJob) string {
	return c.prefix + "progress:" + job.ID + ":" + job.Generation
}
func (c *Client) PutProgress(ctx context.Context, job domain.IndexJob, stage string, chunks int) {
	p := domain.IndexProgress{Stage: stage, Chunks: chunks, UpdatedAt: time.Now().UTC()}
	raw, _ := json.Marshal(p)
	// Progress is advisory; failure never changes the durable job outcome.
	_ = c.r.Set(ctx, c.progressKey(job), raw, stateTTL).Err()
}
func (c *Client) Progress(ctx context.Context, job domain.IndexJob) *domain.IndexProgress {
	if job.Status != "running" || job.Generation == "" {
		return nil
	}
	raw, err := c.r.Get(ctx, c.progressKey(job)).Bytes()
	if err != nil {
		return nil
	}
	var p domain.IndexProgress
	if json.Unmarshal(raw, &p) != nil {
		return nil
	}
	return &p
}

type Event struct {
	Sequence uint64
	Name     string
	Data     json.RawMessage
}

func (c *Client) eventKey(run string, generation uint64) string {
	return fmt.Sprintf("%sevents:%s:%d", c.prefix, run, generation)
}

var appendEvent = redis.NewScript(`
local id=ARGV[1]..'-0'
local old=redis.call('XRANGE',KEYS[1],id,id)
if #old>0 then
 if old[1][2][2]~=ARGV[2] or old[1][2][4]~=ARGV[3] then return redis.error_reply('event conflict') end
 return id
end
redis.call('XADD',KEYS[1],'MAXLEN','=',4096,id,'name',ARGV[2],'data',ARGV[3])
redis.call('EXPIRE',KEYS[1],1800)
return id`)

func (c *Client) AppendEvent(ctx context.Context, run string, generation, seq uint64, name string, data []byte) error {
	if seq == 0 || len(data) > 256*1024 {
		return ErrHistory
	}
	switch name {
	case "run.started", "message.start", "message.delta", "message.discard", "run.completed", "run.failed", "run.suspended":
	default:
		return ErrHistory
	}
	if appendEvent.Run(ctx, c.r, []string{c.eventKey(run, generation)}, seq, name, string(data)).Err() != nil {
		return ErrUnavailable
	}
	return nil
}
func (c *Client) Events(ctx context.Context, run string, generation, after uint64) ([]Event, error) {
	key := c.eventKey(run, generation)
	// Read tail first, so missing/expired data is distinguishable from an idle run.
	tail, err := c.r.XRevRangeN(ctx, key, "+", "-", 1).Result()
	if err != nil {
		return nil, ErrUnavailable
	}
	if len(tail) == 0 {
		return nil, ErrHistory
	}
	tailSeq, _ := strconv.ParseUint(strings.TrimSuffix(tail[0].ID, "-0"), 10, 64)
	if after > tailSeq {
		return nil, ErrHistory
	}
	rows, err := c.r.XRangeN(ctx, key, fmt.Sprintf("(%d-0", after), "+", 128).Result()
	if err != nil {
		return nil, ErrUnavailable
	}
	out := make([]Event, 0, len(rows))
	for _, row := range rows {
		seq, e := strconv.ParseUint(strings.TrimSuffix(row.ID, "-0"), 10, 64)
		name, nok := row.Values["name"].(string)
		data, dok := row.Values["data"].(string)
		if e != nil || seq != after+1 || !nok || !dok {
			return nil, ErrHistory
		}
		out = append(out, Event{seq, name, json.RawMessage(data)})
		after = seq
	}
	return out, nil
}
