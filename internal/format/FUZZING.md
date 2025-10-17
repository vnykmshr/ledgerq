# Fuzzing Tests for LedgerQ

This directory contains fuzzing tests for the format package.

## Running Fuzz Tests

Fuzz tests use Go's built-in fuzzing support (Go 1.18+).

### Run individual fuzz tests

```bash
# Fuzz entry encoding/decoding
go test -fuzz=FuzzEntry -fuzztime=30s ./internal/format

# Fuzz unmarshaling with random input
go test -fuzz=FuzzUnmarshal -fuzztime=30s ./internal/format

# Fuzz CRC32 calculations
go test -fuzz=FuzzCRC32 -fuzztime=30s ./internal/format
```

### Run all fuzz tests briefly

```bash
# Test all fuzz functions for 10 seconds each
for fuzz in FuzzEntry FuzzUnmarshal FuzzCRC32; do
    echo "Testing $fuzz..."
    go test -fuzz=$fuzz -fuzztime=10s ./internal/format
done
```

## What is Being Tested

- **FuzzEntry**: Tests round-trip encoding/decoding of entries with random payloads
- **FuzzUnmarshal**: Tests unmarshaling with completely random/malformed input
- **FuzzCRC32**: Tests CRC32 checksum calculation for correctness and determinism

## Fuzz Corpus

Go automatically builds a corpus of interesting inputs in `testdata/fuzz/`.
These are saved between runs to improve coverage over time.

## Expected Behavior

- Fuzz tests should not panic or crash
- Invalid input should return errors gracefully
- Valid input should round-trip correctly
- CRC32 calculations should be deterministic
