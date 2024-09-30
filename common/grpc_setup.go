//go:build grpc
// +build grpc

package common

import (
	"github.com/Azure/azure-storage-azcopy/v10/grpcctl"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.stackrox.io/grpc-http1/server"
	"net/http"
)

func (grpcCtlImpl) SetupGrpc(addr string, logger ILoggerResetable) error {
	if addr != "" {
		// JobLog is a function, rather than just a reference, to avoid a dependency loop. It's gross, I know.
		grpcctl.JobLog = func(s string) {
			logger.Log(LogInfo, s)
		}

		// Spin off the HTTP server
		go func() {
			// HTTP/1 needs support
			srv := &http.Server{
				Addr: addr,
			}

			// But we must also support HTTP/2 for "modern" clients.
			var h2srv http2.Server

			// The downgrade handler will allow clients to request grpc-web support, removing trailers, etc. for platforms like .NET Framework 4.7.2.
			srv.Handler = h2c.NewHandler(
				server.CreateDowngradingHandler(
					grpcctl.GlobalGRPCServer,
					http.NotFoundHandler(),      // No fallback handler is needed.
					server.PreferGRPCWeb(true)), // If grpc-web is requested, grpc-web we'll give.
				&h2srv)

			// Start listening.
			err := srv.ListenAndServe()
			if err != nil {
				panic("grpcfailed: " + err.Error())
			}
		}()
	}

	// Historically, this could return an error. it does not anymore.
	return nil
}

func (grpcCtlImpl) SetupOAuthSubscription(updateFunc func(token *OAuthTokenUpdate)) {
	grpcctl.Subscribe(grpcctl.GlobalServer, func(i *grpcctl.OAuthTokenUpdate) {
		updateFunc((*OAuthTokenUpdate)(i))
	})
}
