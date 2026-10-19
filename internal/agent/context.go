package agent

import (
	"fmt"
	"time"
)

// Context budgets are deployment configuration, never model or HTTP input.
func normalizeContextConfig(config Config) (Config, error) {
	defaults := []struct {
		target *int
		value  int
	}{
		{&config.MaxContextTokens, 128000}, {&config.ReserveTokens, 16384},
		{&config.KeepRecentTokens, 20000}, {&config.MaxSummaryTokens, 2048},
		{&config.MaxCompletionTokens, 4096},
	}
	for _, item := range defaults {
		if *item.target == 0 {
			*item.target = item.value
		}
		if *item.target < 1 || *item.target > 2000000 {
			return Config{}, fmt.Errorf("%w: context budgets must be between 1 and 2000000 tokens", ErrUnavailable)
		}
	}
	// The same output allowance must be accounted for before every request.
	config.ReserveTokens = max(config.ReserveTokens, config.MaxCompletionTokens)
	if config.ReserveTokens >= config.MaxContextTokens || config.MaxSummaryTokens >= config.MaxContextTokens-config.ReserveTokens {
		return Config{}, fmt.Errorf("%w: response and summary allowances must fit the context window", ErrUnavailable)
	}
	if config.SummaryTimeout == 0 {
		config.SummaryTimeout = time.Minute
	}
	if config.SummaryTimeout < time.Millisecond || config.SummaryTimeout > 10*time.Minute {
		return Config{}, fmt.Errorf("%w: summary timeout must be between 1ms and 10m", ErrUnavailable)
	}
	return config, nil
}
