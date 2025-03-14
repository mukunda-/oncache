//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

var ErrInitFailed = errors.New("initialization failed")
var ErrKeyMismatch = errors.New("key mismatch")

type KeyRing map[string][]byte

type decrypter struct {
	mode       cipher.BlockMode
	in         io.Reader // Underlying stream
	buffer     []byte    // Constant-sized buffer for reading data
	bufferSize int       // Size of data in the buffer currently.
	decrypted  []byte    // Overflow of decrypted data, to be returned to the next Read.

	// EOF has been met. We'll return EOF as soon as we empty our
	// buffers.
	eof bool

	// True to strip zero bytes from the output. Zero bytes are used for encryption
	// padding.
	StripNul bool
}

func strippedCopy(dst []byte, src []byte, stripNul bool) (written int, consumed int) {
	if !stripNul {
		n := copy(dst, src)
		return n, n
	} else {
		reader, writer := 0, 0
		for reader < len(src) && writer < len(dst) {
			if src[reader] != 0 {
				dst[writer] = src[reader]
				writer++
			}
			reader++
		}
		return writer, reader
	}
}

// Read implementation. This will read data from the underlying stream, decrypt it, and
// return the data and how many bytes were decrypted. Padding is stripped according to the
// StripNul flag, which removes zero bytes and readlen count from the output.
//
// Note that due to the null stripping behavior, this function can potentially return (0,
// nil) in some cases, which is not an error.
func (d *decrypter) Read(p []byte) (readlen int, err error) {
	if len(p) == 0 {
		// Corner case. User gave an empty buffer.
		return 0, nil
	}

retry:
	// If we have decrypted data waiting in the buffer, return that first. If `p` is super
	// small, this might happen multiple times before we read the next chunk.
	if len(d.decrypted) > 0 {
		written, consumed := strippedCopy(p, d.decrypted, d.StripNul)
		d.decrypted = d.decrypted[consumed:]
		if len(d.decrypted) == 0 {
			d.decrypted = nil
		}

		if written == 0 {
			// Don't return if nothing was read. Redo and wait for more data.
			goto retry
		}

		return written, nil
	}

	// When not EOF and we have no data to work with, read another chunk of data.
	for !d.eof && d.bufferSize < aes.BlockSize {
		n, err := io.ReadAtLeast(d.in, d.buffer[d.bufferSize:], aes.BlockSize-d.bufferSize)
		d.bufferSize += n
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			d.eof = true
		} else if err != nil {
			return 0, err
		}
	}

	if d.eof && d.bufferSize == 0 {
		// Clean exit, EOF and we have no data waiting.
		return 0, io.EOF
	} else if d.bufferSize < aes.BlockSize {
		// Unexpected EOF, since we haven't read a full 16-byte block.
		return 0, io.ErrUnexpectedEOF
	}

	length := d.bufferSize & ^15
	d.mode.CryptBlocks(d.buffer[:length], d.buffer[:length])
	written, consumed := strippedCopy(p, d.buffer[:length], d.StripNul)

	// If the buffer can't fit everything we got, then save that remainder for the next
	// Read call.
	if consumed < length {
		d.decrypted = make([]byte, length-consumed)
		n, _ := strippedCopy(d.decrypted, d.buffer[consumed:length], d.StripNul)
		d.decrypted = d.decrypted[:n] // Might end up empty if all NUL
	}

	d.bufferSize -= length
	if d.bufferSize > 0 {
		copy(d.buffer, d.buffer[length:length+d.bufferSize])
	}

	if written == 0 {
		// Don't return if nothing was read. Redo and wait for more data.
		goto retry
	}

	return written, nil
}

// Wrap the given io.Reader with decryption. This must be done before any data is read,
// otherwise the encryption header will be corrupted.
//
// The key can either be a single key as a []byte value or a KeyRing containing named
// keys. The single key format will be named "default". Key values must be 16, 24, or 32
// bytes, resulting in AES-128, AES-192, or AES-256 decryption respectively.
//
// Errors:
//   - ErrInitFailed: invalid args or invalid stream data
//   - ErrMissingKey: the key ring does not contain the key requested by the client
//   - ErrKeyMismatch: the cipher test fails, indicating that the client has a different
//     key despite the same name.
func DecryptStream[T []byte | KeyRing](key T, stream io.ReadWriter) (io.ReadWriter, error) {
	// This is just a wrapper for type constraints.
	return decryptStream(key, stream)
}

func decryptStream(key any, stream io.ReadWriter) (io.ReadWriter, error) {
	switch keyval := key.(type) {
	case []byte:
		key = KeyRing{"default": keyval}
	case KeyRing:
	default:
		return nil, fmt.Errorf("%w: invalid key", ErrInitFailed)
	}

	keys := key.(KeyRing)

	// As the "decrypter" we are the "remote" side of the handshake.

	// Confirm that the first 4 bytes is our protocol signature "ON_1"
	{
		var header [4]byte
		if _, err := io.ReadFull(stream, header[:]); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, fmt.Errorf("%w: failed to read header: %w", ErrInitFailed, err)
		}

		if string(header[:]) != "ON_1" {
			return nil, fmt.Errorf("%w: unexpected header signature", ErrInitFailed)
		}
	}

	// Read the salt from the client. This is the IV for encryption.
	remoteSalt := [16]byte{}
	{
		_, err := io.ReadFull(stream, remoteSalt[:])
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, fmt.Errorf("%w: %w", ErrInitFailed, err)
		}
	}

	// Generate salt for the client to use for their encryption.
	clientSalt := generateSalt(aes.BlockSize)
	{
		if _, err := stream.Write(clientSalt[:]); err != nil {
			return nil, fmt.Errorf("%w: failed to write client salt: %w", ErrInitFailed, err)
		}
	}

	// Read the key name; this selects the key to use in the keyring.
	var keyNameBytes [8]byte
	if _, err := io.ReadFull(stream, keyNameBytes[:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("%w: failed to read key name: %w", ErrInitFailed, err)
	}
	keyName := string(keyNameBytes[:])
	keyName = strings.TrimRight(keyName, "\x00")

	cipherkey, ok := keys[keyName]
	if !ok {
		// Key not found in keyring.
		stream.Write([]byte("FAIL"))
		return nil, fmt.Errorf("%w: client requested key \"%s\" which is not in the keyring", ErrMissingKey, keyName)
	}

	if _, err := stream.Write([]byte("OKAY")); err != nil {
		return nil, fmt.Errorf("%w: failed to write status: %w", ErrInitFailed, err)
	}

	blockCipher, err := aes.NewCipher(cipherkey)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create cipher: %w", ErrInitFailed, err)
	}

	encrypter := &encrypter{
		mode: cipher.NewCBCEncrypter(blockCipher, remoteSalt[:]),
		out:  stream,
	}

	decrypter := &decrypter{
		mode: cipher.NewCBCDecrypter(blockCipher, clientSalt[:]),
		in:   stream,

		// The buffer is a working memory space for reading decrypted data.
		buffer: make([]byte, 2048),
	}

	cipherTest := generateSalt(16)

	if _, err := encrypter.Write(cipherTest); err != nil {
		return nil, fmt.Errorf("%w: failed to write cipher test: %w", ErrInitFailed, err)
	}

	{
		cipherTestRecv := make([]byte, 16)
		if _, err := io.ReadFull(decrypter, cipherTestRecv); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, fmt.Errorf("%w: failed to read cipher test: %w", ErrInitFailed, err)
		}

		if slices.Compare(cipherTest, cipherTestRecv) != 0 {
			return nil, fmt.Errorf("%w: cipher test failed; remote may have a different key", ErrKeyMismatch)
		}
	}

	// Strip NUL from the output from this point forward (switching to TEXT mode).
	decrypter.StripNul = true

	return &struct {
		io.Reader
		io.Writer
	}{decrypter, encrypter}, nil
}
