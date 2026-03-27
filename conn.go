package ws

import "time"

const (
	TextMessage   = 1
	BinaryMessage = 2
	CloseMessage  = 8
	PingMessage   = 9
	PongMessage   = 10
)

type (
	readDeadlineConn interface {
		SetReadDeadline(time.Time) error
	}

	writeDeadlineConn interface {
		SetWriteDeadline(time.Time) error
	}

	readLimitConn interface {
		SetReadLimit(int64)
	}

	pongHandlerConn interface {
		SetPongHandler(func(string) error)
	}

	compressionConn interface {
		EnableWriteCompression(bool)
		SetCompressionLevel(int) error
	}
)
