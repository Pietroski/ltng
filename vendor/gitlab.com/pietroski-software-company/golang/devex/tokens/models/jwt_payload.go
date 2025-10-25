package models

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

type (
	JWTPayload struct {
		jwt.RegisteredClaims

		Payload *Payload
	}
)

// NewJWTPayload creates a new token payload with a specific username and duration
// NewJWTPayload returns a *JWTPayload struct which implements
// the jwt.RegisteredClaims struct which holds the following interface:
// GetExpirationTime() (*NumericDate, error)
// GetIssuedAt() (*NumericDate, error)
// GetNotBefore() (*NumericDate, error)
// GetIssuer() (string, error)
// GetSubject() (string, error)
// GetAudience() (ClaimStrings, error)
func NewJWTPayload(
	entityID uuid.UUID,
	duration time.Duration,
	Message Message,
) (*JWTPayload, error) {
	tokenID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}

	jwtPayload := &JWTPayload{
		Payload: &Payload{
			EntityID: entityID,
			Message:  Message,

			Metadata: Metadata{
				TokenID:   tokenID,
				IssuedAt:  time.Now(),
				ExpiresAt: time.Now().Add(duration),
			},
		},
	}

	return jwtPayload, nil
}

// Valid checks if the token payload is valid or not
func (payload *JWTPayload) Valid() error {
	if time.Now().After(payload.Payload.Metadata.ExpiresAt) {
		return errorsx.New(ErrExpiredTokenMsg)
	}

	return nil
}
