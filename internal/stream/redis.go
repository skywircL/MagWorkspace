package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client redis.UniversalClient
	prefix string
	ttl    time.Duration
}

// The caller owns the Redis client and closes it after background executions stop.
func NewRedis(client redis.UniversalClient, prefix string, ttl time.Duration) *Redis {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Redis{client, prefix, ttl}
}
func (r *Redis) keys(k Key) []string {
	base := fmt.Sprintf("%sstream:{%s}:%s:%d", r.prefix, k.SessionID, k.RunID, k.Generation)
	return []string{base + ":events", base + ":meta", base + ":stop", r.liveKey(k.SessionID)}
}
func (r *Redis) liveKey(sid string) string { return r.prefix + "stream:{" + sid + "}:live" }

const touchLive = `
local live=redis.call('GET',KEYS[4])
if live then
 local l=cjson.decode(live)
 if l.run_id==ARGV[2] and tostring(l.generation)==ARGV[3] then redis.call('PEXPIRE',KEYS[4],ARGV[1]) end
end
`

var appendOutput = redis.NewScript(`
local total=tonumber(redis.call('HGET',KEYS[2],'total') or '0')
if tonumber(ARGV[4])~=total+1 then return -1 end
redis.call('RPUSH',KEYS[1],ARGV[5])
local size=tonumber(redis.call('HGET',KEYS[2],'bytes') or '0')+string.len(ARGV[5])
local base=tonumber(redis.call('HGET',KEYS[2],'base') or '0')
while redis.call('LLEN',KEYS[1])>4096 or size>8388608 do
 local raw=redis.call('LPOP',KEYS[1]);size=size-string.len(raw);base=base+1
end
redis.call('HSET',KEYS[2],'total',total+1,'bytes',size,'base',base)
redis.call('PEXPIRE',KEYS[1],ARGV[1]);redis.call('PEXPIRE',KEYS[2],ARGV[1])
` + touchLive + `return 1`)

func (r *Redis) args(k Key) []any {
	return []any{max(r.ttl.Milliseconds(), 1), k.RunID, strconv.FormatUint(k.Generation, 10)}
}
func (r *Redis) AppendEvent(ctx context.Context, k Key, e Event) error {
	if !k.valid() {
		return ErrInvalid
	}
	raw, err := encodeEvent(e)
	if err != nil {
		return err
	}
	n, err := appendOutput.Run(ctx, r.client, r.keys(k), append(r.args(k), e.Sequence, string(raw))...).Int()
	if err != nil {
		return ErrUnavailable
	}
	if n < 0 {
		return ErrHistory
	}
	return nil
}

var readOutput = redis.NewScript(`
if redis.call('EXISTS',KEYS[1])==0 or redis.call('EXISTS',KEYS[2])==0 then return {-1} end
local base=tonumber(redis.call('HGET',KEYS[2],'base'))
local total=tonumber(redis.call('HGET',KEYS[2],'total'))
local offset=tonumber(ARGV[4])
if offset<base or offset>total then return {-1} end
local rows=redis.call('LRANGE',KEYS[1],offset-base,offset-base+127)
` + touchLive + `return rows`)

func (r *Redis) GetEvents(ctx context.Context, k Key, offset uint64) ([]Event, uint64, error) {
	if !k.valid() {
		return nil, offset, ErrInvalid
	}
	rows, err := readOutput.Run(ctx, r.client, r.keys(k), append(r.args(k), offset)...).Slice()
	if err != nil {
		return nil, offset, ErrUnavailable
	}
	out := make([]Event, 0, len(rows))
	for _, row := range rows {
		raw, ok := row.(string)
		if !ok {
			return nil, offset, ErrHistory
		}
		var e Event
		if json.Unmarshal([]byte(raw), &e) != nil || e.Sequence != offset+uint64(len(out))+1 {
			return nil, offset, ErrHistory
		}
		out = append(out, e)
	}
	return out, offset + uint64(len(out)), nil
}
func (r *Redis) ResetEvents(ctx context.Context, k Key) error {
	if !k.valid() {
		return ErrInvalid
	}
	keys := r.keys(k)
	if r.client.Del(ctx, keys[0], keys[1]).Err() != nil {
		return ErrUnavailable
	}
	return nil
}

var requestStop = redis.NewScript(`
redis.call('SET',KEYS[3],'1','PX',ARGV[1])
` + touchLive + `return 1`)

func (r *Redis) RequestStop(ctx context.Context, k Key) error {
	if !k.valid() {
		return ErrInvalid
	}
	if err := requestStop.Run(ctx, r.client, r.keys(k), r.args(k)...).Err(); err != nil {
		return ErrUnavailable
	}
	return nil
}

var readStop = redis.NewScript(touchLive + `return redis.call('EXISTS',KEYS[3])`)

func (r *Redis) StopRequested(ctx context.Context, k Key) (bool, error) {
	if !k.valid() {
		return false, ErrInvalid
	}
	n, err := readStop.Run(ctx, r.client, r.keys(k), r.args(k)...).Int()
	if err != nil {
		return false, ErrUnavailable
	}
	return n == 1, nil
}

var setLive = redis.NewScript(`
local old=redis.call('GET',KEYS[1])
if ARGV[3]=='claim' then
 if (old or '')~=ARGV[4] then return 0 end
elseif old and old~=ARGV[2] then return 0 end
redis.call('SET',KEYS[1],ARGV[2],'PX',ARGV[1]);return 1`)

func (r *Redis) setLive(ctx context.Context, sid string, old, next LiveRun, claim bool) error {
	if !validID(sid) || !next.valid() {
		return ErrInvalid
	}
	b, _ := json.Marshal(next)
	expected := ""
	if old != (LiveRun{}) {
		a, _ := json.Marshal(old)
		expected = string(a)
	}
	mode := "set"
	if claim {
		mode = "claim"
	}
	n, err := setLive.Run(ctx, r.client, []string{r.liveKey(sid)}, max(r.ttl.Milliseconds(), 1), string(b), mode, expected).Int()
	if err != nil {
		return ErrUnavailable
	}
	if n == 0 {
		return ErrConflict
	}
	return nil
}
func (r *Redis) SetLiveRun(ctx context.Context, sid string, l LiveRun) error {
	return r.setLive(ctx, sid, LiveRun{}, l, false)
}
func (r *Redis) ClaimLiveRun(ctx context.Context, sid string, old, next LiveRun) error {
	return r.setLive(ctx, sid, old, next, true)
}
func (r *Redis) GetLiveRun(ctx context.Context, sid string) (LiveRun, error) {
	if !validID(sid) {
		return LiveRun{}, ErrInvalid
	}
	raw, err := r.client.Get(ctx, r.liveKey(sid)).Bytes()
	if errors.Is(err, redis.Nil) {
		return LiveRun{}, nil
	}
	if err != nil {
		return LiveRun{}, ErrUnavailable
	}
	var l LiveRun
	if json.Unmarshal(raw, &l) != nil || !l.valid() {
		return LiveRun{}, ErrHistory
	}
	return l, nil
}

var clearLive = redis.NewScript(`if redis.call('GET',KEYS[1])==ARGV[1] then redis.call('DEL',KEYS[1]) end return 1`)

func (r *Redis) ClearLiveRun(ctx context.Context, sid string, l LiveRun) error {
	if !validID(sid) || !l.valid() {
		return ErrInvalid
	}
	b, _ := json.Marshal(l)
	if clearLive.Run(ctx, r.client, []string{r.liveKey(sid)}, string(b)).Err() != nil {
		return ErrUnavailable
	}
	return nil
}

var _ Manager = (*Redis)(nil)
