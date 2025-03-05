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
	oncrypt.EncryptStream(make([]byte, 32), nil)
}

func mockRemote(key []byte) io.ReadWriter {
	readerIn, writerIn := io.Pipe()
	readerOut, writerOut := io.Pipe()

	go func() {
		decrypter, err := oncrypt.DecryptStream(key, struct {
			io.Reader
			io.Writer
		}{readerIn, writerOut})
		if err != nil {
			panic(err)
		}

		_, _ = io.Copy(decrypter, readerIn)
	}()

	return struct {
		io.Reader
		io.Writer
	}{readerOut, writerIn}
}

// Limit the amount of received bytes to limit before unexpectedly closing the stream.
func mockRemoteLimitedReaders(key []byte, inLimit int64, outLimit int64) io.ReadWriter {

	readerIn, writerIn := io.Pipe()
	readerOut, writerOut := io.Pipe()

	go func() {
		var in io.Reader = readerIn
		if inLimit > 0 {
			in = io.LimitReader(readerIn, inLimit)
		}
		decrypter, err := oncrypt.DecryptStream(key, struct {
			io.Reader
			io.Writer
		}{in, writerOut})
		if err != nil {
			readerIn.Close()
			return
		}

		_, _ = io.Copy(decrypter, readerIn)
	}()

	var out io.Reader = readerOut
	if outLimit > 0 {
		out = io.LimitReader(readerOut, outLimit)
	}

	return struct {
		io.Reader
		io.Writer
	}{out, writerIn}
}

// Most encryption behavior is covered by decrypt tests.

func TestBadEncryptionKey(t *testing.T) {
	// [SPEC] A key must be 16, 24, or 32 bytes.

	for i := 0; i < 33; i++ {
		key := make([]byte, i)
		_, err := oncrypt.EncryptStream(key, mockRemote(make([]byte, i)))
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

func TestFailWritingHandshake(t *testing.T) {
	// [SPEC] Creating an encryption stream should gracefully fail if the stream cannot
	//        accept the required headers.

	testForError := func(t *testing.T, writeLimit int64, readLimit int64, contains string, errclass error) {
		t.Helper()

		_, err := oncrypt.EncryptStream(make([]byte, 32),
			mockRemoteLimitedReaders(make([]byte, 32), writeLimit, readLimit))

		if errclass != nil {
			if !errors.Is(err, oncrypt.ErrInitFailed) || !errors.Is(err, errclass) {
				t.Error("Unexpected error format:", err)
			}

			assertContains(t, err.Error(), contains)
		} else {
			if err != nil {
				t.Error("Unexpected error:", err)
			}
		}
	}

	// protocol id (4 bytes)
	testForError(t, 0+3, 0, "protocol id", io.ErrClosedPipe)
	// remote salt (16 bytes)
	testForError(t, 4+15, 0, "remote salt", io.ErrClosedPipe)
	// Read local salt (16 bytes)
	testForError(t, 20, 0+15, "client salt", io.ErrUnexpectedEOF)
	// key name (8 bytes)
	testForError(t, 20+7, 16, "key name", io.ErrClosedPipe)
	// read status (4 bytes)
	testForError(t, 28, 16+3, "status", io.ErrUnexpectedEOF)
	// read cipher test (16 bytes)
	testForError(t, 28, 20+15, "cipher test", io.ErrUnexpectedEOF)
	// write cipher test (16 bytes)
	testForError(t, 28+15, 36, "cipher test", io.ErrClosedPipe)
	// full handshake
	testForError(t, 44, 36, "", nil)
}
