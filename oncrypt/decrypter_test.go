//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncrypt_test

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	"go.mukunda.com/oncache/oncrypt"
)

func makeTestKey() []byte {
	key := make([]byte, 0, 32)
	for i := 0; i < 32; i++ {
		key = append(key, byte(rand.Uint32()&255))
	}
	return key
}

// Generate a test command like "abab ba aab  a" 1-100 chars long.
func generateTestCommand() string {
	chars := "abcde "

	result := ""
	for i := 0; i < rand.Intn(100)+1; i++ {
		result += string(chars[rand.Intn(len(chars))])
	}

	return result
}

func TestDecrypter(t *testing.T) {
	// [SPEC] Reading data from the stream equals the data written to the stream.

	// The underlying protocol includes NUL padding that is stripped before being passed
	// to reader.

	key := makeTestKey()

	reader, writer := io.Pipe()

	dataWritten := []byte{}
	dataRead := []byte{}

	// For this test, send a lot of random "commands", and expect them to form the same
	// combined string when decoded.
	go func() {
		encrypter, err := oncrypt.EncryptStream(key, writer)
		if err != nil {
			t.Error(err)
			return
		}

		for i := 0; i < 50000; i++ {
			command := generateTestCommand()
			dataWritten = append(dataWritten, command...)
			encrypter.Write([]byte(command))
		}
		writer.Close()
	}()

	decrypter, err := oncrypt.DecryptStream(key, reader)
	if err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, 10000)
	iterations := 0
	for {
		n, err := decrypter.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		dataRead = append(dataRead, buffer[:n]...)
		iterations++
	}

	if string(dataWritten) != string(dataRead) {
		t.Error("Decrypted content does not match original.")
	}

	if iterations < 100 {
		t.Error("Too few iterations: something is wrong with the test.")
	}
}

func createRandomData(size int, alphabet string) []byte {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return data
}

func TestDecrypter2(t *testing.T) {
	// [SPEC] There is no visible effect to sending partial chunks of data at a time.
	//        Underneath, more padding is added to the data, but the decrypted data is
	//        still the same.
	// [SPEC] There is no minimum read size.

	// Same test as above, but testing with sending random chunks of the data with delays
	// and receiving with random, small block sizes.

	sourceData := createRandomData(200_000, "abcde \n")
	var receivedData []byte

	key := makeTestKey()
	reader, writer := io.Pipe()

	go func() {
		encrypter, err := oncrypt.EncryptStream(key, writer)
		if err != nil {
			t.Error(err)
			return
		}

		reader := 0
		for reader < len(sourceData) {
			time.Sleep(time.Millisecond)
			chunkSize := rand.Intn(500)
			if reader+chunkSize > len(sourceData) {

				chunkSize = len(sourceData) - reader
			}

			// [SPEC] The source data is copied, not modified,
			//        when we write to the encrypter.
			encrypter.Write(sourceData[reader : reader+chunkSize])
			reader += chunkSize
		}

		writer.Close()
	}()

	decrypter, err := oncrypt.DecryptStream(key, reader)
	if err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, 10000)
	iterations := 0
	for {
		n, err := decrypter.Read(buffer[0:rand.Intn(1000)])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		receivedData = append(receivedData, buffer[:n]...)
		iterations++
	}

	if string(sourceData) != string(receivedData) {
		t.Error("Decrypted content does not match original.")
	}

	if iterations < 100 {
		t.Error("Too few iterations: something is wrong with the test.")
	}
}

func TestBadDecryptKey(t *testing.T) {
	// [SPEC] A key must be 16, 24, or 32 bytes.

	reader, writer := io.Pipe()
	writer.Close()

	for i := 0; i < 33; i++ {
		key := make([]byte, i)
		_, err := oncrypt.DecryptStream(key, reader)
		if i == 16 || i == 24 || i == 32 {
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Error("Expected EOF, got error:", err)
			}
		} else {
			if !strings.Contains(err.Error(), "invalid key size") {
				t.Error("Expected key error. Got", err)
			}
		}
	}

}

func TestMinimumRead(t *testing.T) {
	// [SPEC] Reads return at least 1 byte of data until the stream is closed.
}

func TestBadDecryptHeader(t *testing.T) {
	// [SPEC] The first 4 bytes of the stream must be "ON_1". Otherwise the stream is
	//        rejected.

	reader, writer := io.Pipe()

	go func() {
		writer.Write([]byte("xxxx"))
		writer.Close()
	}()

	_, err := oncrypt.DecryptStream(makeTestKey(), reader)
	if !errors.Is(err, oncrypt.ErrInitFailed) {
		t.Error("Expected ErrInitFailed. Got", err)
	}
}

func TestBadDecryptIV(t *testing.T) {
	// [SPEC] If the IV is incomplete, ErrUnexpectedEOF is returned.

	reader, writer := io.Pipe()

	go func() {
		writer.Write([]byte("ON_1"))
		writer.Close()
	}()

	_, err := oncrypt.DecryptStream(makeTestKey(), reader)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Error("Expected io.ErrUnexpectedEOF. Got", err)
	}
}

func TestDecryptKeyMismatch(t *testing.T) {
	// [SPEC] If the key hash does not match, the stream is rejected with ErrKeyMismatch.

	reader, writer := io.Pipe()

	go func() {
		encrypter, err := oncrypt.EncryptStream(makeTestKey(), writer)
		if err != nil {
			t.Error(err)
			return
		}

		encrypter.Write([]byte("test"))
		writer.Close()
	}()

	_, err := oncrypt.DecryptStream(makeTestKey(), reader)
	if !errors.Is(err, oncrypt.ErrKeyMismatch) {
		t.Error("Mismatched keys should raise ErrKeyMismatch. Got", err)
	}
}

func TestDecryptUnexpectedEOF(t *testing.T) {
	// [SPEC] If during read, a full AES block is not available, io.ErrUnexpectedEOF is
	//        returned. The data length must be a multiple of 16 before the stream closes.
	key := makeTestKey()

	reader, writer := io.Pipe()

	go func() {
		_, err := oncrypt.EncryptStream(key, writer)
		if err != nil {
			t.Error(err)
			return
		}

		// Bypass the encrypter to write raw, partial data.
		writer.Write([]byte("xxxxxx"))
		writer.Close()
	}()

	decrypter, err := oncrypt.DecryptStream(key, reader)
	if err != nil {
		t.Error(err)
		return
	}

	buffer := make([]byte, 100)
	_, err = decrypter.Read(buffer)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Error("Expected io.ErrUnexpectedEOF. Got", err)
	}
}

func TestDecryptTinyRead(t *testing.T) {
	// [SPEC] It's valid to read 1 byte at a time.
	key := makeTestKey()

	reader, writer := io.Pipe()

	go func() {
		encrypter, err := oncrypt.EncryptStream(key, writer)
		if err != nil {
			t.Error(err)
			return
		}

		encrypter.Write([]byte("test"))
		writer.Close()
	}()

	decrypter, err := oncrypt.DecryptStream(key, reader)
	if err != nil {
		t.Error(err)
		return
	}

	output := make([]byte, 0)
	buffer := make([]byte, 1)
	for {
		n, err := decrypter.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
			return
		}

		if n != 1 {
			t.Error("Expected 1 byte read, got", n)
		}

		output = append(output, buffer[0])
	}

	if string(output) != "test" {
		t.Error("Unexpected output:", string(output))
	}
}

func TestDecryptEmptyRead(t *testing.T) {
	// [SPEC] If you try to read zero bytes, it's a no-op. This does not return EOF.
	key := makeTestKey()

	reader, writer := io.Pipe()

	go func() {
		_, err := oncrypt.EncryptStream(key, writer)
		if err != nil {
			t.Error(err)
			return
		}
		writer.Close()
	}()

	decrypter, _ := oncrypt.DecryptStream(key, reader)
	n, err := decrypter.Read(make([]byte, 0))
	if n != 0 || err != nil {
		t.Error("Unexpected n, err:", n, err)
	}
}

func getRawEncryptedData(key []byte, data []byte) []byte {
	reader, writer := io.Pipe()

	go func() {
		encrypter, err := oncrypt.EncryptStream(key, writer)
		if err != nil {
			return
		}

		encrypter.Write(data)
		writer.Close()
	}()

	output, _ := io.ReadAll(reader)
	return output
}

func readAllWithRandomBuffers(t *testing.T, reader io.Reader) []byte {
	t.Helper()
	document := []byte{}

	for {
		buffer := make([]byte, rand.Intn(1000))
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
			return document
		}

		document = append(document, buffer[:n]...)
	}
	return document
}

func sendAllWithRandomLengthsSlowly(t *testing.T, writer io.Writer, data []byte) {
	t.Helper()
	read := 0

	for read < len(data) {
		amount := rand.Intn(1000)
		remaining := len(data) - read
		if amount > remaining {
			amount = remaining
		}
		writer.Write(data[read : read+amount])
		read += amount
		time.Sleep(time.Millisecond)
	}
}

func TestPartialWrites(t *testing.T) {
	// [SPEC] The process should be resilient against arbitrary breaks in the data stream
	//        during processing.

	key := makeTestKey()
	sourceData := createRandomData(200_000, "abcde \n")
	rawEncrypted := getRawEncryptedData(key, sourceData)

	reader, writer := io.Pipe()
	go func() {
		sendAllWithRandomLengthsSlowly(t, writer, rawEncrypted)
		writer.Close()
	}()

	decrypter, _ := oncrypt.DecryptStream(key, reader)

	document := readAllWithRandomBuffers(t, decrypter)

	if string(sourceData) != string(document) {
		t.Error("Decrypted content does not match original.")
	}
}

func TestNulStripping(t *testing.T) {
	// [SPEC] Periods of NUL should not cause unexpected behavior, such as Read returning
	//        0 bytes read.

	key := makeTestKey()

	totalA := 0

	// Create data with large null spaces and patches of 'a'.
	sourceData := createRandomData(200_000, string([]byte{0}))
	sourceData[200] = 'a' // for first single-byte read test below
	totalA++
	for i := 2000; i < len(sourceData); i++ {
		sourceData[i] = 'a'
		totalA++
		if rand.Intn(100) < 50 {
			i += rand.Intn(200)
		}
	}

	if totalA == 0 {
		t.Fatal("No 'a' characters in source data. Oops!")
	}

	rawEncrypted := getRawEncryptedData(key, sourceData)

	reader, writer := io.Pipe()
	go func() {
		sendAllWithRandomLengthsSlowly(t, writer, rawEncrypted)
		writer.Close()
	}()

	decrypter, _ := oncrypt.DecryptStream(key, reader)

	document := []byte{}

	// First 2000 bytes, read one at a time, to test odd behaviors.
	for i := 0; i < 2000; i++ {
		buffer := make([]byte, 1)
		n, err := decrypter.Read(buffer)
		if err != nil || n != 1 {
			t.Error(err)
		}
		document = append(document, buffer[0])
	}

	document = append(document, readAllWithRandomBuffers(t, decrypter)...)

	if len(document) != totalA {
		t.Error("Decrypted content does not match original.")
	}

	document = bytes.ReplaceAll(document, []byte{'a'}, []byte{})
	if len(document) != 0 {
		t.Error("Unexpected data leftover in document:", string(document))
	}

}

func TestBrokenStream(t *testing.T) {
	// [SPEC] If the stream is broken, the reader should return an error.

	key := makeTestKey()
	sourceData := createRandomData(1000, "abc")
	rawEncrypted := getRawEncryptedData(key, sourceData)

	reader, writer := io.Pipe()
	go func() {
		writer.Write(rawEncrypted[:4+16+32+8])
		writer.Close()
	}()

	decrypter, err := oncrypt.DecryptStream(key, reader)
	if err != nil {
		t.Error(err)
		return
	}

	reader.Close()
	{
		_, err := decrypter.Read(make([]byte, 100))
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Error("Error during read:", err)
			return
		}
	}

}
