---
default: minor
---

# Describe tool results with an output schema

Read a tool's result schema from `Tool::output_schema`: `typed_tool` derives it from the output type, which must now implement `JsonSchema`, and an MCP tool's wraps the server's declared output schema. Code mode declares each function's return type from it instead of `unknown`; `codemode::description` now takes the tools and `codemode::declaration` takes the output schema beside the spec.
