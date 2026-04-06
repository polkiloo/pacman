package peerapi

import "errors"

var (
	errPeerTLSRequired         = errors.New("peer mTLS is required")
	errPeerCertificateRequired = errors.New("verified peer certificate is required")
	errPeerSubjectRequired     = errors.New("peer certificate subject common name is required")
	errPeerSubjectUnauthorized = errors.New("peer certificate subject is not allowed")
)
