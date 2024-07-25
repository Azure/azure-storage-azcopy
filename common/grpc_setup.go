//go:build grpc
// +build grpc

package common

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/grpcctl"
	"net"
)

func (grpcCtlImpl) SetupGrpc(addr string) error {
	if addr != "" {
		// Initialize the grpc server
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("grpcfailed: initialize server: %w", err)
		}

		go func() {
			err = grpcctl.GlobalGRPCServer.Serve(l)
			if err != nil {
				panic("grpcfailed: " + err.Error())
			}
		}()
	}

	return nil
}

func (grpcCtlImpl) SetupOAuthSubscription(updateFunc func(token *OAuthTokenUpdate)) {
	grpcctl.Subscribe(grpcctl.GlobalServer, updateFunc)
}
