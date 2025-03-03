package oncache

import "errors"

var (
	// Returned when the key is not the expected format.
	ErrInvalidKey = errors.New("invalid key; must be 16, 24, or 32 bytes")

	// If you call Init twice.
	ErrAlreadyInitialized = errors.New("already initialized")
)
