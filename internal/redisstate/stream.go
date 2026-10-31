package redisstate

import "magworkspace/internal/stream"

func (c *Client) StreamManager() stream.Manager {
	return stream.NewRedis(c.r, c.prefix, stream.DefaultTTL)
}
