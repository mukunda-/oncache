//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncrypt_test

import (
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"testing"

	"go.mukunda.com/oncache/oncrypt"
)

func TestFailedCryptoRand(t *testing.T) {

	readerHook := rand.Reader
	defer func() {
		rand.Reader = readerHook
		r := recover()
		if r == nil {
			t.Error("expected panic")
		}
		if r != "crypto/rand failed" {
			t.Error("unexpected panic message")
		}
	}()

	rand.Reader = &io.LimitedReader{R: rand.Reader, N: 0}
	oncrypt.EncryptStream(make([]byte, 32), io.Discard)

}

// Most encryption behavior is covered by decrypt tests.

func TestBadEncryptionKey(t *testing.T) {
	// [SPEC] A key must be 16, 24, or 32 bytes.

	for i := 0; i < 33; i++ {
		key := make([]byte, i)
		_, err := oncrypt.EncryptStream(key, io.Discard)
		if i == 16 || i == 24 || i == 32 {
			if err != nil {
				t.Error("Unexpected error:", err)
			}
		} else {
			if !strings.Contains(err.Error(), "invalid key size") {
				t.Error("Expected key error. Got", err)
			}
		}
	}
}

func assertContains(t *testing.T, text string, contains string) {
	t.Helper()
	if !strings.Contains(text, contains) {
		t.Error("Expected \""+text+"\" to contain \"", contains, "\"")
	}
}

func TestFailWritingHeaders(t *testing.T) {
	// [SPEC] Creating an encryption stream should gracefully fail if the stream cannot
	//        accept the required headers.

	// Failed header
	{
		reader, writer := io.Pipe()
		go func() {
			io.ReadFull(reader, make([]byte, 2))
			reader.Close()
		}()

		_, err := oncrypt.EncryptStream(make([]byte, 32), writer)
		if !errors.Is(err, oncrypt.ErrInitFailed) || !errors.Is(err, io.ErrClosedPipe) {
			// should be init failed + closed pipe
			t.Error("Unexpected error format:", err)
		}
		assertContains(t, err.Error(), "header")
	}

	// Failed nonce
	{
		reader, writer := io.Pipe()
		go func() {
			io.ReadFull(reader, make([]byte, 6))
			reader.Close()
		}()

		_, err := oncrypt.EncryptStream(make([]byte, 32), writer)
		if !errors.Is(err, oncrypt.ErrInitFailed) || !errors.Is(err, io.ErrClosedPipe) {
			// should be init failed + closed pipe
			t.Error("Unexpected error format:", err)
		}
		assertContains(t, err.Error(), "nonce")
	}

	// Failed key hash
	{
		reader, writer := io.Pipe()
		go func() {
			io.ReadFull(reader, make([]byte, 4+16+31))
			reader.Close()
		}()

		_, err := oncrypt.EncryptStream(make([]byte, 32), writer)
		if !errors.Is(err, oncrypt.ErrInitFailed) || !errors.Is(err, io.ErrClosedPipe) {
			// should be init failed + closed pipe
			t.Error("Unexpected error format:", err)
		}
		assertContains(t, err.Error(), "key hash")
	}

	// No error with 4+16+32 bytes read.
	{
		reader, writer := io.Pipe()
		go func() {
			io.ReadFull(reader, make([]byte, 4+16+32))
			reader.Close()
		}()

		_, err := oncrypt.EncryptStream(make([]byte, 32), writer)
		if err != nil {
			t.Error("Unexpected error:", err)
		}
	}
}
