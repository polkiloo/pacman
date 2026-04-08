package dcstest

import (
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
	"github.com/polkiloo/pacman/internal/dcs/memory"
)

func TestRunWithMemoryBackend(t *testing.T) {
	t.Parallel()

	const ttl = 60 * time.Millisecond

	Run(t, Config{
		New: func(*testing.T) dcs.DCS {
			return memory.New(memory.Config{
				TTL:           ttl,
				SweepInterval: 5 * time.Millisecond,
			})
		},
		TTL: ttl,
	})
}
