package cluster

// OperationTraceCount records how many times an operation kind reached a given
// lifecycle state in the current PACMAN process.
type OperationTraceCount struct {
	Kind  OperationKind
	State OperationState
	Count uint64
}
