---
default: minor
---

# Run agents on a local llama.cpp server

Use `openai::LlamaModel` to run agents on llama.cpp's `llama-server`: string outputs stream as plain text, other outputs are grammar-constrained, and the server's error messages are reported. A reasoning model's thinking streams as the new `ProgressKind::ReasoningDelta`.
