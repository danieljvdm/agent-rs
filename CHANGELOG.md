# Changelog

## 0.1.2 (2026-11-12)

### Features

#### Add content-free engine tracing

Expose run, model, and tool timings and outcomes through `tracing`, with safe call identifiers and warnings for timeouts, failed cleanup, recovery, and unknown effects. Keep message content out of telemetry and preserve no-subscriber allocation budgets.

## 0.1.1 (2026-11-11)

### Features

#### Encrypt the SQLite store with a key

`SqliteStore::open_with(path, SqliteOptions::default().with_key(SqliteKey::new(bytes)))` encrypts
the database file and its write-ahead log with a 256-bit key you supply. It needs the new
`sqlcipher` feature, which builds SQLite as SQLCipher. Opening with a key on a build without
encryption fails instead of writing an unencrypted file, and a key that does not open the file
is reported as `Error::Config`. `SqliteStore::open` is unchanged.

#### Run agents on a local llama.cpp server

Use `openai::LlamaModel` to run agents on llama.cpp's `llama-server`: string outputs stream as plain text, other outputs are grammar-constrained, and the server's error messages are reported. A reasoning model's thinking streams as the new `ProgressKind::ReasoningDelta`.

## 0.1.0 (2026-11-11)

Initial release.
