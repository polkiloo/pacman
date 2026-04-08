package memory_test

import (
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
	"github.com/polkiloo/pacman/internal/dcs/dcstest"
	"github.com/polkiloo/pacman/internal/dcs/memory"
)

func TestMemoryConformance(t *testing.T) {
	t.Parallel()

	const ttl = 80 * time.Millisecond

	dcstest.Run(t, dcstest.Config{
		New: func(*testing.T) dcs.DCS {
			return memory.New(memory.Config{
				TTL:           ttl,
				SweepInterval: 5 * time.Millisecond,
			})
		},
		TTL: ttl,
	})
}
