package proto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
)

func makeSauce() []byte {
	sauce := make([]byte, 32) // AES-256 secret is 32 bytes
	_, err := rand.Read(sauce)
	if err != nil {
		panic("crypto/rand failed")
	}
	return sauce
}

var sauce = makeSauce()

// generateIV generates a random 16-byte IV for AES-CBC
func generateIV() []byte {
	iv := make([]byte, aes.BlockSize) // AES block size is 16 bytes
	_, err := rand.Read(iv)
	if err != nil {
		panic("crypto/rand failed")
	}
	return iv
}

type encrypter struct {
}

type decrypter struct {
}

// Pad the data with space to a multiple of the block size.
func padBlock(data []byte) []byte {
	padding := (16 - len(data)%16)
	if padding == 0 {
		padding = 16
	}
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(data, padtext...)
}

func unpadBytes(data []byte) []byte {
	padding := data[len(data)-1]
	return data[:len(data)-int(padding)]
}

func (c *encrypter) Write(str string) []byte {
	blockCipher, err := aes.NewCipher(sauce)
	if err != nil {
		panic("AES cipher creation failed")
	}
	plain := padBlock([]byte(str))
	ciphertext := make([]byte, len(plain))
	iv := generateIV()

	mode := cipher.NewCBCEncrypter(blockCipher, iv)
	mode.CryptBlocks(ciphertext, plain)

	return ciphertext
}

func (c *decrypter) Read(encrypted []byte) string {
	return ""
}

// func TestCryptStream(t *testing.T) {
// 	encrypter := &encrypter{}
// 	decrypter := &decrypter{}

// 	// Encrypter writes to the decrypter.
// 	msg := encrypter.Write("Hello, world!")

// 	msg2 := decrypter.Read(msg)

// 	if msg2 != "Hello, world!" {
// 		t.Error("Decrypted message does not match original.")
// 	}
// }
