//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

// oncrypt provides encryption and decryption via a known shared key. See
// encryption.spec.txt for information about the protocol.
package oncrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"slices"
)

var ErrWriteFailed = errors.New("write failed")

type encrypter struct {
	mode cipher.BlockMode
	out  io.Writer
}

// Generates a random 16-byte IV for CBC encryption.
func generateIV() []byte {
	iv := make([]byte, aes.BlockSize) // AES block size is 16 bytes
	_, err := rand.Read(iv)
	if err != nil {
		// Panic if crypto/rand is unavailable from the system.
		panic("crypto/rand failed")
	}
	return iv
}

// Copy the data and pad it to a multiple of 16 using zero bytes.
func padData(data []byte) []byte {
	padding := (16 - (len(data) & 15)) & 15
	data = append(slices.Clone(data), bytes.Repeat([]byte{byte(0)}, padding)...)
	return data
}

// Write encrypted data to the stream. `n` is the length of encrypted data, which may not
// match the length of the input. `data` is modified in place with encryption, and should
// be copied beforehand if you want to preserve the data.
func (e *encrypter) Write(data []byte) (n int, err error) {
	padded := padData(data)
	e.mode.CryptBlocks(padded, padded)

	written := 0
	for written < len(padded) {
		n, err = e.out.Write(padded[written:])
		written += n
		if err != nil {
			return written, err
		}
	}

	return written, nil
}

// Wrap the given io.Writer with encryption. This must be done before any data is sent, as
// the encryption header must come first.
//
// The key must be 16, 24, or 32 bytes, resulting in AES-128, AES-192, or AES-256
// encryption respectively.
//
// Errors:
//   - ErrInitFailed: invalid args (key size), failed to write initial data to stream (may
//     contain wrapped error.
func EncryptStream(key []byte, stream io.Writer) (io.Writer, error) {
	blockCipher, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create cipher: %w", ErrInitFailed, err)
	}

	iv := generateIV()
	mode := cipher.NewCBCEncrypter(blockCipher, iv)

	// Version
	if _, err := stream.Write([]byte("ON_1")); err != nil {
		return nil, fmt.Errorf("%w: failed to write header: %w", ErrInitFailed, err)
	}

	// Nonce
	if _, err := stream.Write(iv); err != nil {
		return nil, fmt.Errorf("%w: failed to write nonce: %w", ErrInitFailed, err)
	}

	encrypter := &encrypter{
		mode: mode,
		out:  stream,
	}

	hash := sha256.Sum256(key)
	if _, err := encrypter.Write(hash[:]); err != nil {
		return nil, fmt.Errorf("%w: failed to write key hash: %w", ErrInitFailed, err)
	}

	return encrypter, nil
}
