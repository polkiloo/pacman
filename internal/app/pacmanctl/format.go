package pacmanctl

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

func defaultCLIAPIURL() string {
	for _, key := range []string{"PACMANCTL_API_URL", "PACMAN_API_URL"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}

	return defaultAPIURL
}

func defaultCLIAPIToken() string {
	for _, key := range []string{"PACMANCTL_API_TOKEN", "PACMAN_API_TOKEN"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}

	return ""
}

func formatMaintenance(status maintenanceModeStatusJSON) string {
	if !status.Enabled {
		return "disabled"
	}

	if status.Reason != "" {
		return "enabled (" + status.Reason + ")"
	}

	return "enabled"
}

func formatOperation(operation *operationJSON) string {
	if operation == nil {
		return "-"
	}

	parts := []string{operation.Kind, operation.State}
	if operation.ToMember != "" {
		parts = append(parts, "to="+operation.ToMember)
	}
	if operation.FromMember != "" {
		parts = append(parts, "from="+operation.FromMember)
	}
	return strings.Join(parts, " ")
}

func formatScheduledSwitchover(sw *scheduledSwitchoverJSON) string {
	if sw == nil {
		return "-"
	}

	parts := []string{sw.At.UTC().Format(time.RFC3339)}
	if sw.From != "" {
		parts = append(parts, "from="+sw.From)
	}
	if sw.To != "" {
		parts = append(parts, "to="+sw.To)
	}
	return strings.Join(parts, " ")
}

func formatReinitStatus(status *reinitStatusJSON) string {
	if status == nil {
		return "-"
	}

	parts := []string{orDash(status.State), "result=" + orDash(status.LastResult)}
	if status.ToMember != "" {
		parts = append(parts, "to="+status.ToMember)
	}
	if status.FromMember != "" {
		parts = append(parts, "from="+status.FromMember)
	}
	if status.OperationID != "" {
		parts = append(parts, "operation="+status.OperationID)
	}
	return strings.Join(parts, " ")
}

func reinitState(status *reinitStatusJSON) string {
	if status == nil {
		return "-"
	}

	return orDash(status.State)
}

func reinitResult(status *reinitStatusJSON) string {
	if status == nil {
		return "-"
	}

	return orDash(status.LastResult)
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}

	return value.UTC().Format(time.RFC3339)
}

func formatOptionalTime(value *time.Time) string {
	if value == nil {
		return "-"
	}

	return formatTime(*value)
}

func formatOptionalInt64(value int64) string {
	if value == 0 {
		return "-"
	}

	return fmt.Sprintf("%d", value)
}

func formatOptionalInt(value int) string {
	if value == 0 {
		return "-"
	}

	return fmt.Sprintf("%d", value)
}

func formatOptionalBool(value *bool) string {
	if value == nil {
		return "-"
	}

	return fmt.Sprintf("%t", *value)
}

func formatMap(values map[string]any) string {
	if len(values) == 0 {
		return "-"
	}

	return formatAny(values)
}

func formatAny(value any) string {
	if value == nil {
		return "-"
	}

	switch typed := value.(type) {
	case string:
		return orDash(typed)
	case bool:
		return fmt.Sprintf("%t", typed)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", typed)
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}

	return string(encoded)
}

func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func orDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}

	return value
}
