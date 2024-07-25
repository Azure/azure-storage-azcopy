package common

import "time"

/*
grpc_shim.go implements a shim that allows for GRPC functionality to reasonably disappear
*/

type GrpcCtl interface {
	SetupGrpc(string) error
	SetupOAuthSubscription(update func(*OAuthTokenUpdate))
}

type grpcCtlImpl struct{}

var GrpcShim grpcCtlImpl

func (g grpcCtlImpl) Available() bool {
	_, ok := (any(g)).(GrpcCtl)
	return ok
}

type OAuthTokenUpdate struct {
	Token  string
	Live   time.Time
	Expiry time.Time
	Wiggle time.Duration
}
