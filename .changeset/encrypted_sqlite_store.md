---
default: minor
---

# Encrypt the SQLite store with a key

`SqliteStore::open_with(path, SqliteOptions::default().with_key(SqliteKey::new(bytes)))` encrypts
the database file and its write-ahead log with a 256-bit key you supply. It needs the new
`sqlcipher` feature, which builds SQLite as SQLCipher. Opening with a key on a build without
encryption fails instead of writing an unencrypted file, and a key that does not open the file
is reported as `Error::Config`. `SqliteStore::open` is unchanged.
