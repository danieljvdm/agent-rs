//! Deterministic probes of the adapters' private hot paths, so `tests/cost_budget.rs` can hold
//! their allocation and byte budgets without a network. **Not a supported API**: hidden from
//! the documentation, and it may change or disappear in any release.

use super::{Answer, llama, responses, transport::Decoder, transport::Provider};
use crate::{ModelEvent, ModelRequest, Result};

/// The adapter to probe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Api {
    /// [`OpenAiModel`](super::OpenAiModel).
    ChatCompletions,
    /// [`OpenAiResponsesModel`](super::OpenAiResponsesModel).
    Responses,
    /// [`LlamaModel`](super::LlamaModel), decoding a plain-text answer.
    Llama,
}

/// The request body the adapter would send for `request`, encoded as it is sent.
pub fn request_body(api: Api, request: ModelRequest) -> Result<Vec<u8>> {
    let body = match api {
        Api::ChatCompletions => super::request_body(request)?,
        Api::Responses => responses::request_body(request)?,
        Api::Llama => llama::request_body(request)?,
    };
    // As `reqwest::RequestBuilder::json` encodes it.
    Ok(serde_json::to_vec(&body).expect("a JSON value encodes"))
}

/// Feeds `chunks` to the adapter's SSE decoder, one network read each, and calls `observe` with
/// every event it decodes, as the adapter's stream would yield them. Stops at the first error.
pub fn decode(
    api: Api,
    max_response_bytes: usize,
    chunks: &[&[u8]],
    mut observe: impl FnMut(ModelEvent),
) -> Result<()> {
    match api {
        Api::ChatCompletions => run(
            Decoder::new(super::Parser::new(max_response_bytes), max_response_bytes),
            chunks,
            &mut observe,
        ),
        Api::Responses => run(
            Decoder::new(
                responses::Parser::new(max_response_bytes),
                max_response_bytes,
            ),
            chunks,
            &mut observe,
        ),
        Api::Llama => run(
            Decoder::new(
                super::Parser::for_provider(Provider::Llama, Answer::Text, max_response_bytes),
                max_response_bytes,
            ),
            chunks,
            &mut observe,
        ),
    }
}

fn run<P: super::EventParser>(
    mut decoder: Decoder<P>,
    chunks: &[&[u8]],
    observe: &mut impl FnMut(ModelEvent),
) -> Result<()> {
    for chunk in chunks {
        decoder.feed(chunk);
        while let Some(event) = decoder.pending.pop_front() {
            observe(event?);
        }
    }
    Ok(())
}
