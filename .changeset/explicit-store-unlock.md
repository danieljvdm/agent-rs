---
default: patch
---

# A closed SQLite store can be reopened at once

`SqliteStore` now unlocks its `.lock` file when it closes or is dropped, instead of relying on the
file closing. A process the host spawned while the store was open could hold a duplicate of the
descriptor until it exec'd, keeping the lock held after `close()`, so reopening the same path failed
with `Error::StoreBusy`.
