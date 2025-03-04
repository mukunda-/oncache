## Oncrypt Encryption Protocol

This package uses AES-CBC encryption to secure communication between peers. A shared
secret is provided during initialization. All peers must have access to the same shared
secret in order to communicate. The shared secret length controls the encryption strength.
Weaker keys are more performant.

- 16-byte secret : AES-128
- 24-byte secret : AES-192
- 32-byte secret : AES-256

To establish a secure connection, this handshake is initiated by the client:

```plaintext
SEND [4 bytes] "ON_1" protocol ID string
SEND [16 bytes] Remote salt (decided by client). Salt for remote->client communication.
                This initializes the cipher on the remote side.
RECV [16 bytes] Client salt (decided by remote). Salt for client->remote communication.
                This initializes the cipher on the client side.
SEND [8 bytes] Key selector. This is the name of the key to use, similar to the public
               part of a keypair. Pad the key name with NUL bytes to 8 bytes.
RECV [4 bytes] Status. "OKAY", or "FAIL" if the key is not recognized.

(Data is encrypted beyond this point)

RECV [16 bytes] Cipher test (random bytes). This test is mirrored back to the remote to
                verify that you have the key and are using the same cipher algorithm.
SEND [16 bytes] Cipher test (mirrored random bytes).

(Stream encryption is established)
```

After `FAIL` or any unexpected data, the connection is dropped. After the 4 bytes `OKAY`,
the stream is encrypted.

Stream chunks are padded with `NUL` bytes to a multiple of 16. (AES block size) using
`NUL`. After decryption, `NUL` bytes are discarded.

Example stream:

```plaintext
KEY = 23 60 57 1f 8d dd 95 4e 92 6b ff 50 f4 9f 47 52

SEND 4f 4e 5f 31  // "ON_1" protocol ID
SEND 64 e4 b4 3f 74 aa e8 3f fa 1e 33 19 34 f9 c2 e9  // 16 byte Remote salt
RECV d5 c6 82 98 d3 5b 9f 46 e3 c7 6e 2b eb ec ff 8c  // 16 byte Client salt
SEND 0f 97 df 0b e7 40 ea d1 d8 24 45 8c c6 a2 10 0e 
     27 12 91 2d 7c 1a 7a d9 a5 3e 64 55 4f 94 19 89  // 32-byte Key hash
RECV 4f 4b 41 59  // "OKAY"

```