//! Fixed protocol fixtures, independent of the adapter serializers/parsers.
#![allow(dead_code)]
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Api {
    Chat,
    Responses,
}
impl Api {
    pub const ALL: [Self; 2] = [Self::Chat, Self::Responses];

    pub fn path(self) -> &'static str {
        match self {
            Self::Chat => "/v1/chat/completions",
            Self::Responses => "/v1/responses",
        }
    }

    pub fn encode(self, frames: &[Value]) -> String {
        let mut source: String = frames
            .iter()
            .map(|value| format!("data: {value}\r\n\r\n"))
            .collect();
        if self == Self::Chat {
            source.push_str("data: [DONE]\r\n\r\n");
        }
        source
    }

    pub fn frames(self, text: Option<&str>, calls: &[(&str, &str, &str)]) -> Vec<Value> {
        if self == Self::Chat {
            let mut frames = vec![];
            if let Some(text) = text {
                frames.push(chat_delta(json!({"content":text}), Value::Null));
            }
            for (index, (id, name, arguments)) in calls.iter().enumerate() {
                frames.push(chat_delta(
                    json!({"tool_calls":[{"index":index,"id":id,"type":"function",
                    "function":{"name":name,"arguments":arguments}}]}),
                    Value::Null,
                ));
            }
            frames.push(chat_delta(
                json!({}),
                json!(if calls.is_empty() {
                    "stop"
                } else {
                    "tool_calls"
                }),
            ));
            frames.push(json!({"choices":[], "usage":{"prompt_tokens":9,"completion_tokens":4}}));
            return frames;
        }
        let mut frames = vec![
            json!({"type":"response.created", "response":{"id":"resp-fixture","status":"in_progress"}}),
            json!({"type":"response.in_progress", "response":{"id":"resp-fixture","status":"in_progress"}}),
        ];
        let mut output = vec![];
        if let Some(text) = text {
            let part = json!({"type":"output_text","text":text,"annotations":[]});
            let item = json!({"type":"message","id":"msg-fixture","role":"assistant","status":"completed","content":[part]});
            frames.extend([
                json!({"type":"response.output_item.added","output_index":0,"item":{
                    "type":"message","id":"msg-fixture","role":"assistant","status":"in_progress","content":[]}}),
                json!({"type":"response.content_part.added","output_index":0,"item_id":"msg-fixture","content_index":0,
                    "part":{"type":"output_text","text":"","annotations":[]}}),
                json!({"type":"response.output_text.delta","output_index":0,"item_id":"msg-fixture","content_index":0,"delta":text}),
                json!({"type":"response.output_text.done","output_index":0,"item_id":"msg-fixture","content_index":0,"text":text}),
                json!({"type":"response.content_part.done","output_index":0,"item_id":"msg-fixture","content_index":0,"part":part}),
                json!({"type":"response.output_item.done","output_index":0,"item":item}),
            ]);
            output.push(item);
        }
        for (id, name, arguments) in calls {
            let index = output.len();
            let item_id = format!("fc-{index}");
            let item = json!({"type":"function_call","id":item_id,"call_id":id,"name":name,"arguments":arguments,"status":"completed"});
            // Two deltas exercise argument assembly, including empty arguments.
            let split = arguments
                .char_indices()
                .nth(1)
                .map_or(arguments.len(), |(i, _)| i);
            frames.extend([
                json!({"type":"response.output_item.added","output_index":index,"item":{
                    "type":"function_call","id":item_id,"call_id":id,"name":name,"arguments":"","status":"in_progress"}}),
                json!({"type":"response.function_call_arguments.delta","output_index":index,"item_id":item_id,"delta":&arguments[..split]}),
                json!({"type":"response.function_call_arguments.delta","output_index":index,"item_id":item_id,"delta":&arguments[split..]}),
                json!({"type":"response.function_call_arguments.done","output_index":index,"item_id":item_id,"arguments":arguments}),
                json!({"type":"response.output_item.done","output_index":index,"item":item}),
            ]);
            output.push(item);
        }
        frames.push(json!({"type":"response.completed","response":{"id":"resp-fixture","status":"completed","output":output,
            "usage":{"input_tokens":9,"output_tokens":4,"total_tokens":13}}}));
        frames
    }

    pub fn text(self, text: &str) -> String {
        self.encode(&self.frames(Some(text), &[]))
    }

    /// `text` as one answer streamed in deltas of at most `piece` bytes: one SSE event per
    /// element, each meant as its own network read.
    pub fn streamed_text(self, text: &str, piece: usize) -> Vec<Vec<u8>> {
        let mut frames = self.frames(Some(text), &[]);
        // The single text delta `frames` produces, replaced by one per piece.
        let at = match self {
            Self::Chat => 0,
            Self::Responses => 4,
        };
        let template = frames.remove(at);
        let mut start = 0;
        let mut deltas = vec![];
        while start < text.len() {
            let mut end = (start + piece).min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            let mut frame = template.clone();
            match self {
                Self::Chat => frame["choices"][0]["delta"]["content"] = json!(&text[start..end]),
                Self::Responses => frame["delta"] = json!(&text[start..end]),
            }
            deltas.push(frame);
            start = end;
        }
        frames.splice(at..at, deltas);
        let mut events: Vec<Vec<u8>> = frames
            .iter()
            .map(|frame| format!("data: {frame}\r\n\r\n").into_bytes())
            .collect();
        if self == Self::Chat {
            events.push(b"data: [DONE]\r\n\r\n".to_vec());
        }
        events
    }

    pub fn truncate(self, source: &str) -> String {
        let offset = match self {
            Self::Chat => source.rfind("data: [DONE]").unwrap(),
            Self::Responses => source.rfind("data: ").unwrap(),
        };
        source[..offset].to_owned()
    }
}
fn chat_delta(delta: Value, finish: Value) -> Value {
    json!({"choices":[{"index":0,"delta":delta,"finish_reason":finish}]})
}
