package ws

import (
	"errors"

	"github.com/infrago/infra"
)

func init() {
	infra.RegisterUpgradeAcceptor("", func(opts infra.UpgradeAcceptOptions) error {
		conn, ok := opts.Socket.(Conn)
		if !ok || conn == nil {
			return errors.New("invalid websocket connection")
		}

		return Accept(AcceptOptions{
			Conn:       conn,
			Meta:       opts.Meta,
			Name:       opts.Name,
			Site:       opts.Site,
			Host:       opts.Host,
			Domain:     opts.Domain,
			RootDomain: opts.RootDomain,
			Path:       opts.Path,
			Uri:        opts.Uri,
			Setting:    cloneMap(opts.Setting),
			Params:     cloneMap(opts.Params),
			Query:      cloneMap(opts.Query),
			Form:       cloneMap(opts.Form),
			Value:      cloneMap(opts.Value),
			Args:       cloneMap(opts.Args),
			Locals:     cloneMap(opts.Locals),
		})
	})
}
