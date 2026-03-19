package config

import (
	"net"
	"strconv"
	"strings"
)

// Validate reports whether the node configuration document is coherent enough
// to start pacmand with.
func (config Config) Validate() error {
	if strings.TrimSpace(config.APIVersion) != APIVersionV1Alpha1 {
		return ErrUnsupportedAPIVersion
	}

	if strings.TrimSpace(config.Kind) != KindNodeConfig {
		return ErrUnexpectedKind
	}

	if err := config.Node.Validate(); err != nil {
		return err
	}

	return nil
}

// Validate reports whether the local node configuration is coherent enough to
// bootstrap pacmand.
func (node NodeConfig) Validate() error {
	if strings.TrimSpace(node.Name) == "" {
		return ErrNodeNameRequired
	}

	if node.Role == "" {
		return ErrNodeRoleRequired
	}

	if !node.Role.IsValid() {
		return ErrNodeRoleInvalid
	}

	if strings.TrimSpace(node.APIAddress) == "" {
		return ErrNodeAPIAddressRequired
	}

	if !isValidListenAddress(node.APIAddress) {
		return ErrNodeAPIAddressInvalid
	}

	if strings.TrimSpace(node.ControlAddress) == "" {
		return ErrNodeControlAddressRequired
	}

	if !isValidListenAddress(node.ControlAddress) {
		return ErrNodeControlAddressInvalid
	}

	return nil
}

func isValidListenAddress(address string) bool {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return false
	}

	if strings.TrimSpace(host) == "" {
		return false
	}

	value, err := strconv.Atoi(port)
	if err != nil {
		return false
	}

	return value >= 1 && value <= 65535
}
