package controlplane

import (
	"sort"

	"github.com/polkiloo/pacman/internal/cluster"
)

type operationTraceKey struct {
	kind  cluster.OperationKind
	state cluster.OperationState
}

func (store *MemoryStateStore) recordOperationTraceLocked(operation cluster.Operation) {
	if !observedOperationKind(operation.Kind) {
		return
	}

	if store.operationTraceCounts == nil {
		store.operationTraceCounts = make(map[operationTraceKey]uint64)
	}

	key := operationTraceKey{
		kind:  operation.Kind,
		state: operation.State,
	}
	store.operationTraceCounts[key]++
}

// OperationTraceCounts returns the locally recorded control-plane operation
// transition counters used by the Prometheus metrics surface.
// These are process-local, in-memory counters and do not depend on DCS
// availability.
func (store *MemoryStateStore) OperationTraceCounts() []cluster.OperationTraceCount {
	store.mu.RLock()
	defer store.mu.RUnlock()

	counts := make([]cluster.OperationTraceCount, 0, len(store.operationTraceCounts))
	for key, count := range store.operationTraceCounts {
		counts = append(counts, cluster.OperationTraceCount{
			Kind:  key.kind,
			State: key.state,
			Count: count,
		})
	}

	sort.Slice(counts, func(left, right int) bool {
		if counts[left].Kind != counts[right].Kind {
			return counts[left].Kind < counts[right].Kind
		}

		return counts[left].State < counts[right].State
	})

	return counts
}

func observedOperationKind(kind cluster.OperationKind) bool {
	switch kind {
	case cluster.OperationKindSwitchover, cluster.OperationKindFailover, cluster.OperationKindRejoin:
		return true
	default:
		return false
	}
}
