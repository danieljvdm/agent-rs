//! The HTTP transport and bounded SSE framing the OpenAI-protocol adapters share.
//!
//! Errors keep the adapters' privacy contract: request URLs and credentials never reach an
//! error's message or cause, because error text is persisted with a failed run. Server bodies
//! never do either, except the error message a provider that [reports them](Provider::Llama)
//! sends with a failed status, bounded in length.

use crate::{Error, ErrorDetail, ModelEvent, ModelStream, Result};
use futures::stream;
use reqwest::{
    Client, ClientBuilder, Response, Url,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue},
};
use serde_json::{Value, json};
use std::{collections::VecDeque, time::Duration};
use tokio_util::sync::CancellationToken;

/// Wire bytes one response may use, as a multiple of `max_response_bytes`. Responses repeats its
/// text in up to five events, each of which JSON escaping can expand sixfold.
const WIRE_FACTOR: usize = 32;
/// Bytes one SSE line or event may use, as a multiple of `max_response_bytes`: one copy of the
/// payload after JSON escaping, which can expand it sixfold.
const FRAME_FACTOR: usize = 6;
/// SSE events one response may contain.
const MAX_EVENTS: usize = 65_536;
/// Bytes of a failed response's body read for its error message.
const ERROR_BODY_BYTES: usize = 8_192;
/// Bytes of a server's error message kept in an error.
const ERROR_MESSAGE_BYTES: usize = 512;

/// The server an adapter speaks to, which names its errors and binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Provider {
    /// OpenAI's hosted API. Its error bodies are never reported.
    OpenAi,
    /// llama.cpp's `llama-server`. Its error messages are reported, because they say what to fix
    /// (a full context, a model that failed to load) and carry no credentials.
    Llama,
}
impl Provider {
    /// The prefix of every error the adapter reports.
    pub fn label(self) -> &'static str {
        match self {
            Self::OpenAi => "OpenAI",
            Self::Llama => "llama-server",
        }
    }
    /// A protocol error, prefixed with the provider's label.
    pub fn error(self, message: &str) -> Error {
        Error::provider(format!("{}: {message}", self.label()))
    }
    /// The provider's rejection of a request as larger than the model's context window,
    /// prefixed with the provider's label.
    pub fn context_overflow(self, message: &str) -> Error {
        Error::context_overflow(format!("{}: {message}", self.label()))
    }
    /// A protocol failure caused by `error`, which is kept as the source and shown in the
    /// message. Pass only causes whose text cannot quote server content or the request URL.
    pub fn error_caused_by(
        self,
        message: &str,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Error {
        Error::Provider(ErrorDetail::caused_by(
            format!("{}: {message}", self.label()),
            error,
        ))
    }
    fn binding_name(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Llama => "llama.cpp",
        }
    }
}

/// The default client: a connect timeout, no redirects, and `configure`'s other settings.
pub(super) fn client(configure: impl FnOnce(ClientBuilder) -> ClientBuilder) -> Result<Client> {
    configure(
        Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none()),
    )
    .build()
    .map_err(|error| Error::Config(ErrorDetail::caused_by("could not build HTTP client", error)))
}

/// One endpoint with its credentials. Cloning shares the client.
#[derive(Clone)]
pub(super) struct Transport {
    provider: Provider,
    client: Client,
    authorization: Option<HeaderValue>,
    endpoint: Url,
}

impl Transport {
    /// `path` under `https://api.openai.com/v1`, with a client that does not follow redirects.
    pub fn new(api_key: impl Into<String>, path: &str) -> Result<Self> {
        Self::with_client(
            client(|builder| builder)?,
            api_key,
            "https://api.openai.com/v1",
            path,
        )
    }

    /// An OpenAI endpoint: `path` under `base_url`, sent with `api_key`.
    pub fn with_client(
        client: Client,
        api_key: impl Into<String>,
        base_url: impl AsRef<str>,
        path: &str,
    ) -> Result<Self> {
        Self::endpoint(Provider::OpenAi, client, base_url, path)?.with_api_key(api_key)
    }

    /// `path` under `base_url` for `provider`, without credentials. `base_url` must be HTTPS (or
    /// loopback HTTP) without credentials, query or fragment.
    pub fn endpoint(
        provider: Provider,
        client: Client,
        base_url: impl AsRef<str>,
        path: &str,
    ) -> Result<Self> {
        let label = provider.label();
        let endpoint = Url::parse(&format!(
            "{}/{path}",
            base_url.as_ref().trim_end_matches('/')
        ))
        .map_err(|error| {
            Error::Config(ErrorDetail::caused_by(
                format!("invalid {label} base URL"),
                error,
            ))
        })?;
        let local = matches!(
            endpoint.host_str(),
            Some("localhost" | "127.0.0.1" | "[::1]")
        );
        if !(endpoint.scheme() == "https" || (endpoint.scheme() == "http" && local))
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(Error::config(format!(
                "{label} base URL requires HTTPS (or loopback HTTP), without credentials/query/fragment"
            )));
        }
        Ok(Self {
            provider,
            client,
            authorization: None,
            endpoint,
        })
    }

    /// Sends `api_key` as a bearer token. Fails for an empty or unusable key.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Result<Self> {
        let label = self.provider.label();
        let key = api_key.into();
        if key.trim().is_empty() {
            return Err(Error::config(format!("empty {label} API key")));
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {key}"))
            .map_err(|_| Error::config(format!("invalid {label} API key header")))?;
        authorization.set_sensitive(true);
        self.authorization = Some(authorization);
        Ok(self)
    }

    /// The adapter's durable identity: provider, API, protocol version and endpoint. The
    /// endpoint holds no credentials, so rotating a key keeps the binding.
    pub fn binding(&self, api: &str) -> Value {
        json!({"provider":self.provider.binding_name(), "api":api, "version":1, "endpoint":self.endpoint.as_str()})
    }

    /// The JSON document at `path` beside the endpoint, under the same API root: `models` for
    /// an endpoint of `.../v1/chat/completions` is `.../v1/models`. At most `max_bytes` of body
    /// are read. Errors keep the transport's privacy contract and quote nothing the server sent.
    pub async fn get(&self, endpoint_path: &str, path: &str, max_bytes: usize) -> Result<Value> {
        let provider = self.provider;
        let root = (self.endpoint.as_str().strip_suffix(endpoint_path))
            .ok_or_else(|| provider.error("endpoint has no API root"))?;
        let url = Url::parse(&format!("{root}{path}"))
            .map_err(|error| provider.error_caused_by("invalid request path", error))?;
        let mut request = self.client.get(url).header("Accept", "application/json");
        if let Some(authorization) = &self.authorization {
            request = request.header(AUTHORIZATION, authorization.clone());
        }
        let mut response = request.send().await.map_err(|error| {
            provider.error_caused_by("HTTP request failed", error.without_url())
        })?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            return Err(provider.error(&format!("HTTP status {status}")));
        }
        let mut body = Vec::new();
        loop {
            let chunk = response.chunk().await.map_err(|error| {
                provider.error_caused_by("HTTP response failed", error.without_url())
            })?;
            let Some(chunk) = chunk else { break };
            if body.len() + chunk.len() > max_bytes {
                return Err(provider.error("response exceeds byte limit"));
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|_| provider.error("malformed JSON response"))
    }

    /// Posts `body` and streams the events `parser` reads from the SSE response.
    pub async fn stream<P: EventParser + Send + 'static>(
        &self,
        body: &Value,
        parser: P,
        limit: usize,
        cancel: CancellationToken,
    ) -> Result<ModelStream> {
        let provider = self.provider;
        let mut request = self
            .client
            .post(self.endpoint.clone())
            .header("Accept", "text/event-stream")
            .json(body);
        if let Some(authorization) = &self.authorization {
            request = request.header(AUTHORIZATION, authorization.clone());
        }
        let response = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Interrupted),
            response = request.send() =>
                response.map_err(|error| provider.error_caused_by("HTTP request failed", error.without_url()))?,
        };
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let error = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::Interrupted),
                error = error_body(response) => error,
            };
            let message = match provider {
                Provider::OpenAi => None,
                Provider::Llama => error.get("message").and_then(server_message),
            };
            let message = match message {
                Some(message) => format!("HTTP status {status}: {message}"),
                None => format!("HTTP status {status}"),
            };
            // 413 is a request too large in bytes, which a smaller context also answers.
            return Err(match status == 413 || is_context_overflow(&error) {
                true => provider.context_overflow(&message),
                false => provider.error(&message),
            });
        }
        if !response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.split(';')
                    .next()
                    .is_some_and(|v| v.trim().eq_ignore_ascii_case("text/event-stream"))
            })
        {
            return Err(provider.error("expected text/event-stream"));
        }
        Ok(events(
            Body::Http(response),
            Decoder::new(parser, limit),
            cancel,
        ))
    }
}

/// The `error` of a failed response's JSON body, read up to a bound, or null when the body has
/// none. It is read for every provider, to tell what kind of failure it is; only
/// [`server_message`] decides what an error may quote from it.
async fn error_body(mut response: Response) -> Value {
    let mut body = Vec::new();
    while body.len() < ERROR_BODY_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => body.extend_from_slice(&chunk),
            _ => break,
        }
    }
    let body: Option<Value> = serde_json::from_slice(&body).ok();
    body.map_or(Value::Null, |mut body| body["error"].take())
}

/// Whether a provider's `error` says the request does not fit the model's context window.
///
/// llama-server and OpenAI mark it, with an `exceed_context_size_error` type and a
/// `context_length_exceeded` code. Other providers reached through an OpenAI-compatible
/// endpoint say it only in the message, so the wording the larger ones use is recognised too.
/// The message is read here and never quoted. Taking another error for this one costs a
/// compaction; missing this one fails the run.
pub(super) fn is_context_overflow(error: &Value) -> bool {
    const WORDING: [&str; 8] = [
        "context length",
        "context window",
        "context size",
        "prompt is too long",
        "prompt too long",
        "input token count",
        "maximum prompt length",
        "reduce the length of the messages",
    ];
    if error["type"] == "exceed_context_size_error" || error["code"] == "context_length_exceeded" {
        return true;
    }
    (error["message"].as_str()).is_some_and(|message| {
        let message = message.to_ascii_lowercase();
        WORDING.iter().any(|wording| message.contains(wording))
    })
}

/// A server's error message as an error may quote it: non-empty, without control characters, and
/// at most [`ERROR_MESSAGE_BYTES`] long.
pub(super) fn server_message(message: &Value) -> Option<String> {
    let message: String = message
        .as_str()?
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let message = message.trim();
    if message.is_empty() {
        return None;
    }
    if message.len() <= ERROR_MESSAGE_BYTES {
        return Some(message.to_owned());
    }
    let mut end = ERROR_MESSAGE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    Some(format!("{}…", &message[..end]))
}

/// Where a stream's bytes come from.
pub(super) enum Body {
    Http(Response),
    /// Fixed chunks, so tests can drive the streaming loop without a socket.
    #[cfg(test)]
    Chunks(VecDeque<Vec<u8>>),
}

/// The events decoded from `body`. Cancellation ends the stream with [`Error::Interrupted`] and
/// discards whatever was decoded but not yet yielded, including a complete response.
pub(super) fn events<P: EventParser + Send + 'static>(
    body: Body,
    decoder: Decoder<P>,
    cancel: CancellationToken,
) -> ModelStream {
    Box::pin(stream::try_unfold(
        (body, decoder, cancel),
        |(mut body, mut decoder, cancel)| async move {
            loop {
                if cancel.is_cancelled() {
                    return Err(Error::Interrupted);
                }
                if let Some(event) = decoder.pending.pop_front() {
                    return Ok(Some((event?, (body, decoder, cancel))));
                }
                if decoder.parser.done() {
                    return Ok(None);
                }
                let fed = match &mut body {
                    Body::Http(response) => {
                        let chunk = tokio::select! {
                            biased;
                            _ = cancel.cancelled() => return Err(Error::Interrupted),
                            chunk = response.chunk() => chunk.map_err(|error| decoder.parser.provider().error_caused_by("HTTP response read failed", error.without_url()))?,
                        };
                        chunk.map(|bytes| decoder.feed(&bytes)).is_some()
                    }
                    #[cfg(test)]
                    Body::Chunks(chunks) => chunks
                        .pop_front()
                        .map(|bytes| decoder.feed(&bytes))
                        .is_some(),
                };
                if !fed {
                    return Err(decoder.parser.provider().error("truncated SSE response"));
                }
            }
        },
    ))
}

/// One API's reading of SSE event data.
pub(super) trait EventParser {
    /// Reads one event's data, pushing the model events it completes onto `events`.
    fn event(&mut self, data: &str, events: &mut Vec<ModelEvent>) -> Result<()>;
    /// Whether the response is complete; nothing but whitespace may follow.
    fn done(&self) -> bool;
    /// The provider that names this parser's errors.
    fn provider(&self) -> Provider;
}

/// Bounded SSE framing for both APIs. On an error it keeps the usage and text already decoded
/// from the same chunk but discards a complete response, which must not outlive a failure.
pub(super) struct Decoder<P> {
    parser: P,
    wire_limit: usize,
    frame_limit: usize,
    wire_bytes: usize,
    events: usize,
    line: Vec<u8>,
    data: String,
    decoded: Vec<ModelEvent>,
    pub pending: VecDeque<Result<ModelEvent>>,
}
impl<P: EventParser> Decoder<P> {
    pub fn new(parser: P, limit: usize) -> Self {
        Self {
            parser,
            wire_limit: limit.saturating_mul(WIRE_FACTOR).saturating_add(65_536),
            frame_limit: limit.saturating_mul(FRAME_FACTOR).saturating_add(16_384),
            wire_bytes: 0,
            events: 0,
            line: vec![],
            data: String::new(),
            decoded: vec![],
            pending: VecDeque::new(),
        }
    }

    /// Decodes one chunk onto [`pending`](Self::pending), ending with the error if it fails.
    pub fn feed(&mut self, bytes: &[u8]) {
        let result = self.decode(bytes);
        if result.is_err() {
            self.decoded
                .retain(|event| !matches!(event, ModelEvent::Response(_)));
        }
        self.pending.extend(self.decoded.drain(..).map(Ok));
        if let Err(error) = result {
            self.pending.push_back(Err(error));
        }
    }

    fn decode(&mut self, bytes: &[u8]) -> Result<()> {
        let provider = self.parser.provider();
        self.wire_bytes = self.wire_bytes.saturating_add(bytes.len());
        if self.wire_bytes > self.wire_limit {
            return Err(provider.error("wire byte limit exceeded"));
        }
        for &byte in bytes {
            if self.parser.done() {
                if !byte.is_ascii_whitespace() {
                    return Err(provider.error("data after SSE terminator"));
                }
                continue;
            }
            if byte != b'\n' {
                if self.line.len() >= self.frame_limit {
                    return Err(provider.error("SSE line limit exceeded"));
                }
                self.line.push(byte);
                continue;
            }
            if self.line.last() == Some(&b'\r') {
                self.line.pop();
            }
            // Buffers are reused, so framing allocates only while they grow.
            let line = std::str::from_utf8(&self.line)
                .map_err(|error| provider.error_caused_by("invalid UTF-8 in SSE", error))?;
            if line.is_empty() {
                if !self.data.is_empty() {
                    self.events += 1;
                    if self.events > MAX_EVENTS {
                        return Err(provider.error("SSE event count exceeds limit"));
                    }
                    self.parser
                        .event(self.data.trim_end_matches('\n'), &mut self.decoded)?;
                    self.data.clear();
                }
            } else if let Some(value) = line.strip_prefix("data:") {
                let value = value.strip_prefix(' ').unwrap_or(value);
                if self
                    .data
                    .len()
                    .saturating_add(value.len())
                    .saturating_add(1)
                    > self.frame_limit
                {
                    return Err(provider.error("SSE event limit exceeded"));
                }
                self.data.push_str(value);
                self.data.push('\n');
            }
            self.line.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
impl<P: EventParser> Decoder<P> {
    /// Feeds `bytes` and takes the pending events, or the first error.
    pub fn feed_events(&mut self, bytes: &[u8]) -> Result<Vec<ModelEvent>> {
        self.feed(bytes);
        self.pending.drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    /// Completes after one `data: done` event; every other event is a text delta.
    #[derive(Default)]
    struct Lines {
        done: bool,
    }
    impl EventParser for Lines {
        fn event(&mut self, data: &str, events: &mut Vec<ModelEvent>) -> Result<()> {
            match data {
                "done" => {
                    self.done = true;
                    events.push(ModelEvent::Response(crate::ModelResponse::final_output(
                        json!("done"),
                    )));
                }
                "fail" => return Err(Provider::OpenAi.error("failed")),
                text => events.push(ModelEvent::TextDelta(text.to_owned())),
            }
            Ok(())
        }
        fn done(&self) -> bool {
            self.done
        }
        fn provider(&self) -> Provider {
            Provider::OpenAi
        }
    }
    fn chunks(chunks: &[&str]) -> Body {
        Body::Chunks(chunks.iter().map(|c| c.as_bytes().to_vec()).collect())
    }

    #[test]
    fn a_failing_chunk_keeps_earlier_events_but_never_its_response() {
        let mut decoder = Decoder::new(Lines::default(), 1024);
        decoder.feed(b"data: a\n\ndata: done\n\ndata: fail\n\n");
        let events: Vec<_> = decoder.pending.drain(..).collect();
        assert!(matches!(&events[0], Ok(ModelEvent::TextDelta(text)) if text == "a"));
        assert!(matches!(&events[1], Err(Error::Provider(_))));
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn utf8_errors_keep_a_cause_that_quotes_no_content() {
        let error = Decoder::new(Lines::default(), 1024)
            .feed_events(b"data: leaked\xff\n\n")
            .unwrap_err();
        assert!(!error.to_string().contains("leaked"), "{error}");
        assert!(std::error::Error::source(&error).is_some());
    }

    #[tokio::test]
    async fn streams_until_done_and_rejects_truncation() {
        let mut stream = events(
            chunks(&["data: a\n", "\ndata: do", "ne\n\n"]),
            Decoder::new(Lines::default(), 1024),
            CancellationToken::new(),
        );
        assert!(matches!(
            stream.next().await,
            Some(Ok(ModelEvent::TextDelta(_)))
        ));
        assert!(matches!(
            stream.next().await,
            Some(Ok(ModelEvent::Response(_)))
        ));
        assert!(stream.next().await.is_none());

        let mut stream = events(
            chunks(&["data: a\n\n"]),
            Decoder::new(Lines::default(), 1024),
            CancellationToken::new(),
        );
        assert!(matches!(
            stream.next().await,
            Some(Ok(ModelEvent::TextDelta(_)))
        ));
        let error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(
            error.to_string(),
            "provider: OpenAI: truncated SSE response"
        );
    }

    #[tokio::test]
    async fn cancellation_discards_a_buffered_complete_response() {
        let cancel = CancellationToken::new();
        let mut stream = events(
            chunks(&["data: a\n\ndata: done\n\n"]),
            Decoder::new(Lines::default(), 1024),
            cancel.clone(),
        );
        assert!(matches!(
            stream.next().await,
            Some(Ok(ModelEvent::TextDelta(_)))
        ));
        cancel.cancel();
        assert!(matches!(stream.next().await, Some(Err(Error::Interrupted))));
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn frames_events_and_wire_bytes_are_bounded() {
        let limit = 10;
        let frame = limit * FRAME_FACTOR + 16_384;
        assert!(
            Decoder::new(Lines::default(), limit)
                .feed_events(&vec![b'x'; frame + 1])
                .is_err()
        );
        assert!(
            Decoder::new(Lines::default(), limit)
                .feed_events(&vec![b'x'; frame])
                .is_ok()
        );
        assert!(
            Decoder::new(Lines::default(), limit)
                .feed_events(&vec![b'\n'; limit * WIRE_FACTOR + 65_537])
                .is_err()
        );
        let many = "data: x\n\n".repeat(MAX_EVENTS + 1);
        assert!(
            Decoder::new(Lines::default(), 1 << 20)
                .feed_events(many.as_bytes())
                .is_err()
        );
    }

    #[tokio::test]
    async fn request_errors_omit_the_url_and_credentials() {
        // Nothing listens on port 1, or the sandbox refuses the connection: either way the
        // request fails before any response.
        let transport = Transport::with_client(
            Client::builder().no_proxy().build().unwrap(),
            "secret-test-key",
            "http://127.0.0.1:1/v1/private-route",
            "responses",
        )
        .unwrap();
        let error = match transport
            .stream(&json!({}), Lines::default(), 1024, CancellationToken::new())
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("request to a closed port succeeded"),
        };
        // What a failed run persists: no URL at all.
        let persisted = error.to_string();
        assert!(
            persisted.starts_with("provider: OpenAI: HTTP request failed: "),
            "{persisted}"
        );
        assert!(!persisted.contains("127.0.0.1"), "{persisted}");
        // Every cause, displayed or debugged, omits the path and the key. (The connector's
        // debug form of a connect error names the peer socket address, not the URL.)
        let mut text = format!("{error} {error:?}");
        let mut source = std::error::Error::source(&error);
        while let Some(cause) = source {
            text.push_str(&format!(" {cause} {cause:?}"));
            source = cause.source();
        }
        for secret in ["private-route", "secret-test-key", "Bearer"] {
            assert!(!text.contains(secret), "{secret} in {text}");
        }
    }

    #[test]
    fn bindings_name_the_endpoint_but_never_the_key() {
        let transport = |key: &str, base: &str| {
            Transport::with_client(Client::new(), key, base, "responses").unwrap()
        };
        let binding = transport("first-key", "https://example.test/v1/").binding("responses");
        assert_eq!(
            binding,
            json!({"provider":"openai","api":"responses","version":1,
                "endpoint":"https://example.test/v1/responses"})
        );
        assert_eq!(
            binding,
            transport("rotated-key", "https://example.test/v1").binding("responses")
        );
        assert!(!binding.to_string().contains("key"));
        for base in [
            "http://example.com/v1",
            "https://user:pass@example.com/v1",
            "https://example.com/v1?key=x",
            "https://example.com/v1#key",
        ] {
            assert!(matches!(
                Transport::with_client(Client::new(), "key", base, "responses"),
                Err(Error::Config(_))
            ));
        }
    }
}
