package controlplane

import (
	"strconv"
	"strings"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
)

func preferredWALLSN(wal agentmodel.WALProgress) string {
	switch {
	case wal.FlushLSN != "":
		return wal.FlushLSN
	case wal.ReplayLSN != "":
		return wal.ReplayLSN
	case wal.WriteLSN != "":
		return wal.WriteLSN
	case wal.ReceiveLSN != "":
		return wal.ReceiveLSN
	default:
		return ""
	}
}

func walLSNAhead(left, right string) bool {
	leftValue, ok := parseWALLSN(left)
	if !ok {
		return false
	}

	rightValue, ok := parseWALLSN(right)
	if !ok {
		return false
	}

	return leftValue > rightValue
}

func parseWALLSN(s string) (uint64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}

	hi, lo, found := strings.Cut(s, "/")
	if !found {
		return 0, false
	}

	high, err := strconv.ParseUint(hi, 16, 64)
	if err != nil {
		return 0, false
	}

	low, err := strconv.ParseUint(lo, 16, 64)
	if err != nil {
		return 0, false
	}

	return (high << 32) | low, true
}
