package config

import "errors"

var (
	ErrUnsupportedAPIVersion      = errors.New("config apiVersion is unsupported")
	ErrUnexpectedKind             = errors.New("config kind is invalid")
	ErrNodeNameRequired           = errors.New("config node name is required")
	ErrNodeRoleRequired           = errors.New("config node role is required")
	ErrNodeRoleInvalid            = errors.New("config node role is invalid")
	ErrNodeAPIAddressRequired     = errors.New("config node apiAddress is required")
	ErrNodeAPIAddressInvalid      = errors.New("config node apiAddress is invalid")
	ErrNodeControlAddressRequired = errors.New("config node controlAddress is required")
	ErrNodeControlAddressInvalid  = errors.New("config node controlAddress is invalid")
)
