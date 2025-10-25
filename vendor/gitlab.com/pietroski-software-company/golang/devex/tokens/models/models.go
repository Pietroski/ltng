package models

import (
	"time"

	"github.com/google/uuid"
)

// Different types of error returned by the VerifyToken function
const (
	ErrCreatingChaChaPolyCypher   = "failed to create new chacha20poly1305 cipher"
	ErrReadingNonceMsg            = "failed to read nonce"
	ErrSerializingTokenMsg        = "error serializing token"
	ErrDeserializingTokenMsg      = "error deserializing token"
	ErrDecodingIntoByteSliceMsg   = "failed to decode token into byte slice"
	ErrDecryptingTokenMsg         = "failed to decrypt token"
	ErrInvalidTokenPayloadSizeMsg = "invalid token payload size"
	ErrCreatingTokenPayloadMsg    = "error creating token payload"
	ErrInvalidTokenMsg            = "token is invalid"
	ErrExpiredTokenMsg            = "token has expired"
	ErrSigningTokenMsg            = "error signing token"
)

// Maker is an interface for managing tokens
type Maker interface {
	// CreateToken creates a new token for a specific username and duration
	CreateToken(
		entityID uuid.UUID,
		duration time.Duration,
		message Message,
	) (string, *Payload, error)

	// VerifyToken checks if the token is valid or not
	VerifyToken(token string) (*Payload, error)
}
