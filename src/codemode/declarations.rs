//! What the model is told about code mode: each allowed tool as a TypeScript declaration, after a
//! fixed description of the contract. Rendering never fails; a schema it cannot render, or one
//! too large, becomes `unknown`.
use crate::ToolSpec;
use serde_json::{Map, Value};
use std::fmt::Write;

/// Bytes one input type may take before it is declared `unknown`.
const MAX_TYPE_BYTES: usize = 4 * 1024;
/// How deep a type may nest, counting `$ref`s followed, before it is `unknown`.
const MAX_DEPTH: usize = 8;

/// Words a tool function must not be named, because the language reserves them or because the
/// script needs the global they name.
const RESERVED: &[&str] = &[
    "arguments",
    "await",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "debugger",
    "default",
    "delete",
    "do",
    "else",
    "enum",
    "eval",
    "export",
    "extends",
    "false",
    "finally",
    "for",
    "function",
    "if",
    "implements",
    "import",
    "in",
    "instanceof",
    "interface",
    "let",
    "new",
    "null",
    "package",
    "private",
    "protected",
    "public",
    "return",
    "static",
    "super",
    "switch",
    "this",
    "throw",
    "true",
    "try",
    "typeof",
    "var",
    "void",
    "while",
    "with",
    "yield",
    // Globals a script relies on.
    "Array",
    "Boolean",
    "Error",
    "Function",
    "Infinity",
    "JSON",
    "Map",
    "Math",
    "NaN",
    "Number",
    "Object",
    "Promise",
    "Proxy",
    "Reflect",
    "RegExp",
    "Set",
    "String",
    "Symbol",
    "TypeError",
    "console",
    "globalThis",
    "undefined",
];

/// The identifier a tool's function is called by: the tool's name with each character that
/// cannot appear in an identifier replaced by `_`, prefixed with `_` if it would start with a
/// digit, and suffixed with `_` if it is a reserved word.
pub fn identifier(name: &str) -> String {
    let mut out: String = (name.chars())
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '$' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    if RESERVED.contains(&out.as_str()) {
        out.push('_');
    }
    out
}

fn is_identifier(name: &str) -> bool {
    identifier(name) == name
}

/// The fixed text before the declarations.
const PREAMBLE: &str = "\
Run JavaScript that calls the tools below as async functions, and get back the value it returns.

`code` is the body of an async function. Use `await`, and `return` the result, which must be JSON.
- Await every call. A call that is not awaited when the code returns is never made.
- Calls awaited one after another run one after another; `Promise.all` runs calls in parallel.
- A failed call throws a `ToolError` whose `value` is the tool's error result. Catch it to carry on. \
The effects of calls that already ran are not undone.
- There is no clock, no timers, no randomness and no network or file access other than through \
these functions. `console.log` lines are returned with the result.

The result is `{result, logs}`. Code that throws is an error result `{error, logs, calls}`, where \
`calls` lists every call it made and how each ended.

Functions:
";

/// The description of a code mode tool for `tools`: the fixed contract, then one declaration per
/// tool. Byte-stable for a given list of specs.
pub fn description(tools: &[ToolSpec]) -> String {
    let mut out = String::from(PREAMBLE);
    out.push_str("```ts\n");
    for spec in tools {
        out.push_str(&declaration(spec));
    }
    out.push_str("```\n");
    out
}

/// One tool's declaration, with its description as a doc comment.
pub fn declaration(spec: &ToolSpec) -> String {
    let mut out = String::new();
    doc_comment(&mut out, &spec.description, "");
    let name = identifier(&spec.name);
    let schema = &spec.input_schema;
    if is_empty_object(schema) {
        let _ = writeln!(out, "declare function {name}(): Promise<unknown>;");
    } else {
        let input = input_type(schema);
        let _ = writeln!(
            out,
            "declare function {name}(input: {input}): Promise<unknown>;"
        );
    }
    out
}

/// The TypeScript type of an input schema, or `unknown` if it cannot be rendered within
/// `MAX_TYPE_BYTES`.
pub fn input_type(schema: &Value) -> String {
    let mut renderer = Renderer { root: schema };
    match renderer.render(schema, 0, "") {
        Some(ty) if ty.len() <= MAX_TYPE_BYTES => ty,
        _ => "unknown".into(),
    }
}

fn doc_comment(out: &mut String, text: &str, indent: &str) {
    let text = text.trim();
    if text.is_empty() {
        return;
    }
    let text = text.replace("*/", "*\\/");
    let mut lines = text.lines();
    match (lines.next(), lines.next()) {
        (Some(only), None) => {
            let _ = writeln!(out, "{indent}/** {only} */");
        }
        _ => {
            let _ = writeln!(out, "{indent}/**");
            for line in text.lines() {
                let _ = writeln!(out, "{indent} * {}", line.trim_end());
            }
            let _ = writeln!(out, "{indent} */");
        }
    }
}

/// An object schema with no properties and no other keys allowed, or none described.
fn is_empty_object(schema: &Value) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    object.get("type").and_then(Value::as_str) == Some("object")
        && (object.get("properties")).is_none_or(|p| p.as_object().is_some_and(Map::is_empty))
        && (object.get("additionalProperties")).is_none_or(|a| a == &Value::Bool(false))
        && !["anyOf", "oneOf", "allOf", "$ref", "patternProperties"]
            .iter()
            .any(|key| object.contains_key(*key))
}

struct Renderer<'a> {
    root: &'a Value,
}
impl<'a> Renderer<'a> {
    /// Renders `schema`, or `None` if any part of it cannot be rendered. Nested types are
    /// indented by `indent`.
    fn render(&mut self, schema: &'a Value, depth: usize, indent: &str) -> Option<String> {
        if depth > MAX_DEPTH {
            return None;
        }
        let object = match schema {
            Value::Bool(true) => return Some("unknown".into()),
            Value::Bool(false) => return Some("never".into()),
            Value::Object(object) => object,
            _ => return None,
        };
        if let Some(reference) = object.get("$ref") {
            let target = self.resolve(reference.as_str()?)?;
            return self.render(target, depth + 1, indent);
        }
        if let Some(value) = object.get("const") {
            return literal(value);
        }
        if let Some(values) = object.get("enum") {
            let literals = (values.as_array()?.iter())
                .map(literal)
                .collect::<Option<Vec<_>>>()?;
            return (!literals.is_empty()).then(|| literals.join(" | "));
        }
        for (key, separator) in [("anyOf", " | "), ("oneOf", " | "), ("allOf", " & ")] {
            if let Some(variants) = object.get(key) {
                let rendered = (variants.as_array()?.iter())
                    .map(|variant| self.render(variant, depth + 1, indent).map(parenthesize))
                    .collect::<Option<Vec<_>>>()?;
                return (!rendered.is_empty()).then(|| rendered.join(separator));
            }
        }
        match object.get("type") {
            None if object.contains_key("properties") => self.object(object, depth, indent),
            None => Some("unknown".into()),
            Some(Value::String(ty)) => self.typed(ty, object, depth, indent),
            Some(Value::Array(types)) => {
                let rendered = (types.iter())
                    .map(|ty| {
                        self.typed(ty.as_str()?, object, depth, indent)
                            .map(parenthesize)
                    })
                    .collect::<Option<Vec<_>>>()?;
                (!rendered.is_empty()).then(|| rendered.join(" | "))
            }
            Some(_) => None,
        }
    }
    fn typed(
        &mut self,
        ty: &str,
        object: &'a Map<String, Value>,
        depth: usize,
        indent: &str,
    ) -> Option<String> {
        Some(match ty {
            "string" => "string".into(),
            "number" | "integer" => "number".into(),
            "boolean" => "boolean".into(),
            "null" => "null".into(),
            "array" => {
                if let Some(items) = object.get("prefixItems").and_then(Value::as_array) {
                    let items = (items.iter())
                        .map(|item| self.render(item, depth + 1, indent))
                        .collect::<Option<Vec<_>>>()?;
                    format!("[{}]", items.join(", "))
                } else {
                    match object.get("items") {
                        None => "unknown[]".into(),
                        Some(items) => {
                            format!("{}[]", parenthesize(self.render(items, depth + 1, indent)?))
                        }
                    }
                }
            }
            "object" => self.object(object, depth, indent)?,
            _ => return None,
        })
    }
    fn object(
        &mut self,
        object: &'a Map<String, Value>,
        depth: usize,
        indent: &str,
    ) -> Option<String> {
        let required: Vec<&str> = (object.get("required"))
            .and_then(Value::as_array)
            .map(|names| names.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let properties = match object.get("properties") {
            None => None,
            Some(properties) => Some(properties.as_object()?),
        };
        let extra = match object.get("additionalProperties") {
            None | Some(Value::Bool(false)) => None,
            Some(schema) => Some(schema),
        };
        if properties.is_none_or(Map::is_empty) {
            return Some(match extra {
                None => "{}".into(),
                Some(schema) => format!(
                    "Record<string, {}>",
                    self.render(schema, depth + 1, indent)?
                ),
            });
        }
        let inner = format!("{indent}  ");
        let mut out = String::from("{\n");
        for (name, schema) in properties? {
            if let Some(description) = schema.get("description").and_then(Value::as_str) {
                doc_comment(&mut out, description, &inner);
            }
            let key = if is_identifier(name) || RESERVED.contains(&name.as_str()) {
                name.clone()
            } else {
                serde_json::to_string(name).ok()?
            };
            let optional = if required.contains(&name.as_str()) {
                ""
            } else {
                "?"
            };
            let ty = self.render(schema, depth + 1, &inner)?;
            let _ = writeln!(out, "{inner}{key}{optional}: {ty};");
        }
        if let Some(schema) = extra {
            let ty = self.render(schema, depth + 1, &inner)?;
            let _ = writeln!(out, "{inner}[key: string]: {ty};");
        }
        out.push_str(indent);
        out.push('}');
        Some(out)
    }
    /// A local reference, `#/$defs/...` or `#/definitions/...`, as a JSON pointer into the root.
    fn resolve(&self, reference: &str) -> Option<&'a Value> {
        let pointer = reference.strip_prefix('#')?;
        self.root.pointer(pointer)
    }
}

/// A JSON value as a TypeScript literal type, for strings, numbers, booleans and null.
fn literal(value: &Value) -> Option<String> {
    match value {
        Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => {
            serde_json::to_string(value).ok()
        }
        _ => None,
    }
}

/// Wraps a union or intersection in parentheses, so it can be an array element or a member of
/// another one.
fn parenthesize(ty: String) -> String {
    let mut depth = 0_i32;
    let mut top_level = false;
    let mut in_string = false;
    let mut escaped = false;
    for c in ty.chars() {
        if in_string {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '{' | '[' | '(' | '<' => depth += 1,
            '}' | ']' | ')' | '>' => depth -= 1,
            '|' | '&' if depth == 0 => top_level = true,
            _ => {}
        }
    }
    if top_level { format!("({ty})") } else { ty }
}
