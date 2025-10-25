package models

import (
	"time"

	"github.com/google/uuid"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

type (
	// Payload contains the payload data of the token
	Payload struct {
		EntityID uuid.UUID `json:"entity_id"`
		Message  Message   `json:"message"`
		Metadata Metadata  `json:"metadata"`
	}

	Message  []byte
	Metadata struct {
		TokenID   uuid.UUID `json:"token_id"`
		IssuedAt  time.Time `json:"issued_at"`
		ExpiresAt time.Time `json:"expired_at"`
	}
)

// NewPayload creates a new token payload with a specific username and duration
func NewPayload(
	entityID uuid.UUID,
	duration time.Duration,
	message Message,
) (*Payload, error) {
	tokenID, err := uuid.NewV7()
	if err != nil {
		return nil, errorsx.Wrap(err, "failed to create token ID")
	}

	payload := &Payload{
		EntityID: entityID,
		Message:  message,

		Metadata: Metadata{
			TokenID:   tokenID,
			IssuedAt:  time.Now(),
			ExpiresAt: time.Now().Add(duration),
		},
	}

	return payload, nil
}

// Valid checks if the token payload is valid or not
func (payload *Payload) Valid() error {
	if time.Now().After(payload.Metadata.ExpiresAt) {
		return errorsx.New(ErrExpiredTokenMsg)
	}

	return nil
}
