This package provides a SIEVE cache implementation.
https://junchengyang.com/publication/nsdi24-SIEVE.pdf

This implementation includes a time-to-live (TTL) option for values.

One implementation specific detail is how the cache handles setting a key multiple times.
When updating an existing key, the "visited" flag is not set. If the value has a TTL, then
the value is moved to the head.
