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

// Generates a random 16-byte salt for encryption.
func generateSalt(size int) []byte {
	salt := make([]byte, size)
	_, err := rand.Read(salt)
	if err != nil {
		// Panic if crypto/rand is unavailable from the system.
		panic("crypto/rand failed")
	}
	return salt
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

type combinedStream struct {
	out *encrypter
	in  *decrypter
}

func (e *combinedStream) Write(data []byte) (n int, err error) {
	return e.out.Write(data)
}

func (e *combinedStream) Read(data []byte) (n int, err error) {
	return e.in.Read(data)
}

// Wrap the given stream with encryption using the oncrypt protocol. This executes the
// oncrypt handshake and returns an encrypted writer. Stream read is required to complete
// the handshake, but not for writing data.
//
// The key must be 16, 24, or 32 bytes, resulting in AES-128, AES-192, or AES-256
// encryption respectively.
//
// Errors:
//
//   - ErrInitFailed: invalid args (key size) or failed handshake. May contain wrapped
//     error.
func EncryptStream(key []byte, stream io.ReadWriter) (io.ReadWriter, error) {
	blockCipher, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create cipher: %w", ErrInitFailed, err)
	}

	remoteSalt := generateSalt(aes.BlockSize)

	// Version
	if _, err := stream.Write([]byte("ON_1")); err != nil {
		return nil, fmt.Errorf("%w: failed to write protocol id: %w", ErrInitFailed, err)
	}

	// Remote Salt
	if _, err := stream.Write(remoteSalt); err != nil {
		return nil, fmt.Errorf("%w: failed to write nonce: %w", ErrInitFailed, err)
	}

	// Read local salt
	var clientSalt [16]byte
	if _, err := io.ReadFull(stream, clientSalt[:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("%w: failed to read client salt: %w", ErrInitFailed, err)
	}

	encrypter := &encrypter{
		mode: cipher.NewCBCEncrypter(blockCipher, clientSalt[:]),
		out:  stream,
	}

	decrypter := &decrypter{
		mode: cipher.NewCBCDecrypter(blockCipher, remoteSalt),
		in:   stream,

		// The buffer is a working memory space for reading decrypted data.
		buffer: make([]byte, 2048),
	}

	hash := sha256.Sum256(append(clientSalt[:], key...))
	if _, err := encrypter.Write(hash[:]); err != nil {
		return nil, fmt.Errorf("%w: failed to write key hash: %w", ErrInitFailed, err)
	}

	return &combinedStream{encrypter, decrypter}, nil
}
