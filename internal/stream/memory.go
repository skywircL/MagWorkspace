package stream

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

type memoryData struct {
	events  [][]byte
	base    uint64
	bytes   int
	expires time.Time
}
type memoryLive struct {
	run     LiveRun
	expires time.Time
}
type Memory struct {
	mu    sync.Mutex
	data  map[Key]*memoryData
	live  map[string]memoryLive
	stops map[Key]time.Time
	ttl   time.Duration
	swept time.Time
}

func NewMemory(ttl time.Duration) *Memory {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Memory{data: map[Key]*memoryData{}, live: map[string]memoryLive{}, stops: map[Key]time.Time{}, ttl: ttl}
}
func (m *Memory) sweep() {
	now := time.Now()
	if now.Sub(m.swept) < time.Second {
		return
	}
	m.swept = now
	for k, v := range m.data {
		if !now.Before(v.expires) {
			delete(m.data, k)
		}
	}
	for k, expires := range m.stops {
		if !now.Before(expires) {
			delete(m.stops, k)
		}
	}
	for k, v := range m.live {
		if !now.Before(v.expires) {
			delete(m.live, k)
		}
	}
}
func (m *Memory) get(k Key, create bool) *memoryData {
	m.sweep()
	d := m.data[k]
	if d != nil && !time.Now().Before(d.expires) {
		delete(m.data, k)
		d = nil
	}
	if d == nil && create {
		d = &memoryData{expires: time.Now().Add(m.ttl)}
		m.data[k] = d
	}
	return d
}
func (m *Memory) touch(k Key) {
	l, ok := m.live[k.SessionID]
	if ok && l.run.RunID == k.RunID && l.run.Generation == k.Generation && time.Now().Before(l.expires) {
		l.expires = time.Now().Add(m.ttl)
		m.live[k.SessionID] = l
	}
}
func (m *Memory) AppendEvent(ctx context.Context, k Key, e Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !k.valid() {
		return ErrInvalid
	}
	raw, err := encodeEvent(e)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.get(k, true)
	// Sequential append detects lost history and ambiguous retries rather than hiding a gap.
	if e.Sequence != d.base+uint64(len(d.events))+1 {
		return ErrHistory
	}
	d.events = append(d.events, raw)
	d.bytes += len(raw)
	for len(d.events) > EventLimit || d.bytes > maxStreamBytes {
		d.bytes -= len(d.events[0])
		d.events[0] = nil
		d.events = d.events[1:]
		d.base++
	}
	d.expires = time.Now().Add(m.ttl)
	m.touch(k)
	return nil
}
func (m *Memory) GetEvents(ctx context.Context, k Key, offset uint64) ([]Event, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, offset, err
	}
	if !k.valid() {
		return nil, offset, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.get(k, false)
	if d == nil || len(d.events) == 0 || offset < d.base || offset > d.base+uint64(len(d.events)) {
		return nil, offset, ErrHistory
	}
	end := min(uint64(len(d.events)), offset-d.base+BatchSize)
	out := make([]Event, 0, end-(offset-d.base))
	for _, raw := range d.events[offset-d.base : end] {
		var e Event
		if json.Unmarshal(raw, &e) != nil {
			return nil, offset, ErrHistory
		}
		out = append(out, e)
	}
	m.touch(k)
	return out, d.base + end, nil
}
func (m *Memory) ResetEvents(ctx context.Context, k Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !k.valid() {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweep()
	delete(m.data, k)
	return nil
}
func (m *Memory) RequestStop(ctx context.Context, k Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !k.valid() {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweep()
	m.stops[k] = time.Now().Add(m.ttl)
	m.touch(k)
	return nil
}
func (m *Memory) StopRequested(ctx context.Context, k Key) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !k.valid() {
		return false, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweep()
	m.touch(k)
	if expires, ok := m.stops[k]; ok && time.Now().Before(expires) {
		return true, nil
	}
	delete(m.stops, k)
	return false, nil
}
func (m *Memory) GetLiveRun(ctx context.Context, sid string) (LiveRun, error) {
	if err := ctx.Err(); err != nil {
		return LiveRun{}, err
	}
	if !validID(sid) {
		return LiveRun{}, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweep()
	l := m.live[sid]
	if !time.Now().Before(l.expires) {
		delete(m.live, sid)
		return LiveRun{}, nil
	}
	return l.run, nil
}
func (m *Memory) setLive(ctx context.Context, sid string, old, next LiveRun, claim bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validID(sid) || !next.valid() {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweep()
	l := m.live[sid]
	if !time.Now().Before(l.expires) {
		l.run = LiveRun{}
	}
	if claim {
		if l.run != old {
			return ErrConflict
		}
	} else if l.run != (LiveRun{}) && l.run != next {
		return ErrConflict
	}
	m.live[sid] = memoryLive{next, time.Now().Add(m.ttl)}
	return nil
}
func (m *Memory) SetLiveRun(ctx context.Context, sid string, l LiveRun) error {
	return m.setLive(ctx, sid, LiveRun{}, l, false)
}
func (m *Memory) ClaimLiveRun(ctx context.Context, sid string, old, next LiveRun) error {
	return m.setLive(ctx, sid, old, next, true)
}
func (m *Memory) ClearLiveRun(ctx context.Context, sid string, l LiveRun) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validID(sid) || !l.valid() {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.live[sid].run == l {
		delete(m.live, sid)
	}
	return nil
}

var _ Manager = (*Memory)(nil)
