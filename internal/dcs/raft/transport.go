package raft

import (
	"crypto/tls"
	"net"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
)

type tcpStreamLayer struct {
	listener  net.Listener
	clientTLS *tls.Config
}

func newTransport(config Config) (*hraft.NetworkTransport, error) {
	stream, err := newTCPStreamLayer(config.BindAddress, config.ServerTLSConfig, config.ClientTLSConfig)
	if err != nil {
		return nil, err
	}

	return hraft.NewNetworkTransportWithConfig(&hraft.NetworkTransportConfig{
		Stream:  stream,
		MaxPool: defaultTransportMaxPool,
		Timeout: config.TransportTimeout,
		Logger:  hclog.NewNullLogger(),
	}), nil
}

func newTCPStreamLayer(bindAddress string, serverTLSConfig, clientTLSConfig *tls.Config) (*tcpStreamLayer, error) {
	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, err
	}

	if serverTLSConfig != nil {
		listener = tls.NewListener(listener, serverTLSConfig.Clone())
	}

	return &tcpStreamLayer{
		listener:  listener,
		clientTLS: cloneTLSConfig(clientTLSConfig),
	}, nil
}

func (layer *tcpStreamLayer) Accept() (net.Conn, error) {
	return layer.listener.Accept()
}

func (layer *tcpStreamLayer) Close() error {
	return layer.listener.Close()
}

func (layer *tcpStreamLayer) Addr() net.Addr {
	return layer.listener.Addr()
}

func (layer *tcpStreamLayer) Dial(address hraft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	if layer.clientTLS != nil {
		tlsConfig := layer.clientTLS.Clone()
		if tlsConfig.ServerName == "" {
			host, _, err := net.SplitHostPort(string(address))
			if err == nil && net.ParseIP(host) == nil {
				tlsConfig.ServerName = host
			}
		}

		dialer := &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: timeout},
			Config:    tlsConfig,
		}
		return dialer.Dial("tcp", string(address))
	}

	return (&net.Dialer{Timeout: timeout}).Dial("tcp", string(address))
}

func cloneTLSConfig(config *tls.Config) *tls.Config {
	if config == nil {
		return nil
	}

	return config.Clone()
}
