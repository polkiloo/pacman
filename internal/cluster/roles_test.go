package cluster

import (
	"reflect"
	"testing"
)

func TestMemberRoles(t *testing.T) {
	t.Parallel()

	want := []MemberRole{
		MemberRolePrimary,
		MemberRoleReplica,
		MemberRoleStandbyLeader,
		MemberRoleWitness,
		MemberRoleUnknown,
	}

	got := MemberRoles()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected member roles: got %v, want %v", got, want)
	}

	got[0] = MemberRoleUnknown

	if second := MemberRoles(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected member roles copy, got %v, want %v", second, want)
	}
}

func TestMemberRoleCapabilities(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		role        MemberRole
		valid       bool
		dataBearing bool
		writable    bool
		stringValue string
	}{
		{
			name:        "primary",
			role:        MemberRolePrimary,
			valid:       true,
			dataBearing: true,
			writable:    true,
			stringValue: "primary",
		},
		{
			name:        "replica",
			role:        MemberRoleReplica,
			valid:       true,
			dataBearing: true,
			writable:    false,
			stringValue: "replica",
		},
		{
			name:        "standby leader",
			role:        MemberRoleStandbyLeader,
			valid:       true,
			dataBearing: true,
			writable:    false,
			stringValue: "standby_leader",
		},
		{
			name:        "witness",
			role:        MemberRoleWitness,
			valid:       true,
			dataBearing: false,
			writable:    false,
			stringValue: "witness",
		},
		{
			name:        "unknown",
			role:        MemberRoleUnknown,
			valid:       true,
			dataBearing: false,
			writable:    false,
			stringValue: "unknown",
		},
		{
			name:        "invalid",
			role:        MemberRole("archive"),
			valid:       false,
			dataBearing: false,
			writable:    false,
			stringValue: "archive",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.role.IsValid(); got != testCase.valid {
				t.Fatalf("unexpected validity: got %v, want %v", got, testCase.valid)
			}

			if got := testCase.role.IsDataBearing(); got != testCase.dataBearing {
				t.Fatalf("unexpected data-bearing flag: got %v, want %v", got, testCase.dataBearing)
			}

			if got := testCase.role.IsWritable(); got != testCase.writable {
				t.Fatalf("unexpected writable flag: got %v, want %v", got, testCase.writable)
			}

			if got := testCase.role.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}

func TestNodeRoles(t *testing.T) {
	t.Parallel()

	want := []NodeRole{
		NodeRoleData,
		NodeRoleWitness,
		NodeRoleUnknown,
	}

	got := NodeRoles()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected node roles: got %v, want %v", got, want)
	}

	got[0] = NodeRoleUnknown

	if second := NodeRoles(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected node roles copy, got %v, want %v", second, want)
	}
}

func TestNodeRoleCapabilities(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		role             NodeRole
		valid            bool
		hasLocalPostgres bool
		stringValue      string
	}{
		{
			name:             "data",
			role:             NodeRoleData,
			valid:            true,
			hasLocalPostgres: true,
			stringValue:      "data",
		},
		{
			name:             "witness",
			role:             NodeRoleWitness,
			valid:            true,
			hasLocalPostgres: false,
			stringValue:      "witness",
		},
		{
			name:             "unknown",
			role:             NodeRoleUnknown,
			valid:            true,
			hasLocalPostgres: false,
			stringValue:      "unknown",
		},
		{
			name:             "invalid",
			role:             NodeRole("observer"),
			valid:            false,
			hasLocalPostgres: false,
			stringValue:      "observer",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.role.IsValid(); got != testCase.valid {
				t.Fatalf("unexpected validity: got %v, want %v", got, testCase.valid)
			}

			if got := testCase.role.HasLocalPostgres(); got != testCase.hasLocalPostgres {
				t.Fatalf("unexpected postgres capability: got %v, want %v", got, testCase.hasLocalPostgres)
			}

			if got := testCase.role.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}

func TestNodeRoleSupportsMemberRole(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		nodeRole   NodeRole
		memberRole MemberRole
		supported  bool
	}{
		{name: "data node supports primary", nodeRole: NodeRoleData, memberRole: MemberRolePrimary, supported: true},
		{name: "data node supports replica", nodeRole: NodeRoleData, memberRole: MemberRoleReplica, supported: true},
		{name: "data node supports standby leader", nodeRole: NodeRoleData, memberRole: MemberRoleStandbyLeader, supported: true},
		{name: "data node supports unknown", nodeRole: NodeRoleData, memberRole: MemberRoleUnknown, supported: true},
		{name: "data node rejects witness", nodeRole: NodeRoleData, memberRole: MemberRoleWitness, supported: false},
		{name: "witness node supports witness", nodeRole: NodeRoleWitness, memberRole: MemberRoleWitness, supported: true},
		{name: "witness node supports unknown", nodeRole: NodeRoleWitness, memberRole: MemberRoleUnknown, supported: true},
		{name: "witness node rejects primary", nodeRole: NodeRoleWitness, memberRole: MemberRolePrimary, supported: false},
		{name: "unknown node only supports unknown member role", nodeRole: NodeRoleUnknown, memberRole: MemberRoleUnknown, supported: true},
		{name: "unknown node rejects replica", nodeRole: NodeRoleUnknown, memberRole: MemberRoleReplica, supported: false},
		{name: "invalid node role rejects valid member role", nodeRole: NodeRole("observer"), memberRole: MemberRoleReplica, supported: false},
		{name: "valid node role rejects invalid member role", nodeRole: NodeRoleData, memberRole: MemberRole("archive"), supported: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.nodeRole.SupportsMemberRole(testCase.memberRole); got != testCase.supported {
				t.Fatalf("unexpected support result: got %v, want %v", got, testCase.supported)
			}
		})
	}
}
