package controlplane

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
)

// MemberRegistrar accepts control-plane member registrations published by
// pacmand during startup.
type MemberRegistrar interface {
	RegisterMember(context.Context, MemberRegistration) error
}

// MemberDiscovery exposes the control-plane member list built from registered
// members and their latest observed node state.
type MemberDiscovery interface {
	RegisteredMember(nodeName string) (MemberRegistration, bool)
	RegisteredMembers() []MemberRegistration
	Member(nodeName string) (cluster.MemberStatus, bool)
	Members() []cluster.MemberStatus
}

// MemberRegistration describes the static identity a node advertises to the
// control plane before cluster-wide desired state exists.
type MemberRegistration struct {
	NodeName       string
	NodeRole       cluster.NodeRole
	APIAddress     string
	ControlAddress string
	RegisteredAt   time.Time
}

// Validate reports whether the registration is coherent enough to be stored in
// the control plane.
func (registration MemberRegistration) Validate() error {
	node := config.NodeConfig{
		Name:           registration.NodeName,
		Role:           registration.NodeRole,
		APIAddress:     registration.APIAddress,
		ControlAddress: registration.ControlAddress,
	}

	if err := node.Validate(); err != nil {
		return fmt.Errorf("validate member registration: %w", err)
	}

	if registration.RegisteredAt.IsZero() {
		return ErrMemberRegistrationTimeRequired
	}

	return nil
}

// Clone returns a detached copy of the registration.
func (registration MemberRegistration) Clone() MemberRegistration {
	return registration
}

func discoveredMemberFromRegistration(registration MemberRegistration) cluster.MemberStatus {
	host, port := memberEndpoint(registration.APIAddress)

	return cluster.MemberStatus{
		Name:       registration.NodeName,
		APIURL:     memberAPIURL(registration.APIAddress),
		Host:       host,
		Port:       port,
		Role:       memberRoleForNodeRole(registration.NodeRole),
		State:      cluster.MemberStateUnknown,
		Healthy:    false,
		LastSeenAt: registration.RegisteredAt,
	}
}

func memberRoleForNodeRole(nodeRole cluster.NodeRole) cluster.MemberRole {
	switch nodeRole {
	case cluster.NodeRoleWitness:
		return cluster.MemberRoleWitness
	default:
		return cluster.MemberRoleUnknown
	}
}

func memberAPIURL(address string) string {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return ""
	}

	return (&url.URL{
		Scheme: "http",
		Host:   trimmed,
	}).String()
}

func memberEndpoint(address string) (string, int) {
	host, rawPort, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return "", 0
	}

	port, err := strconv.Atoi(rawPort)
	if err != nil {
		return "", 0
	}

	return host, port
}
