"use strict";

const $ = id => document.getElementById(id);
const state = {
  token: "", connected: false, selected: location.hash.slice(1), refreshing: false,
  timer: null, conversations: [], conversationPages: 1, historyPages: 1, inboxPages: 1,
  pendingSubmission: null, pendingConversation: null, sending: false,
  networkError: false,
  transcriptSignature: "", sidebarSignature: "", decisionNodes: new Map(),
};

function node(tag, text, className) {
  const element = document.createElement(tag);
  if (text !== undefined) element.textContent = text;
  if (className) element.className = className;
  return element;
}

function valueText(value) {
  return typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

async function api(path, body) {
  const response = await fetch(`/api${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: { Authorization: `Bearer ${state.token}`, ...(body === undefined ? {} : { "Content-Type": "application/json" }) },
    body: body === undefined ? undefined : JSON.stringify(body),
    cache: "no-store",
    signal: AbortSignal.timeout(15000),
  });
  if (!response.ok) {
    const data = await response.json().catch(() => null);
    const error = new Error(data?.error?.message || `Request failed (${response.status}).`);
    error.status = response.status;
    if (response.status === 401 && state.connected) disconnect("Your token was not accepted. Connect again to continue.");
    throw error;
  }
  if (response.status === 204 || response.headers.get("content-length") === "0") return null;
  const text = await response.text();
  return text ? JSON.parse(text) : null;
}

async function pages(path, count, field) {
  let after = 0;
  let result;
  const items = [];
  for (let i = 0; i < count; i++) {
    result = await api(`${path}?after=${after}&limit=30`);
    const page = field ? result[field] : result;
    items.push(...page.items);
    after = page.next_cursor;
    if (after === null) break;
  }
  return { items, more: after !== null, raw: result };
}

function notice(message) {
  $("notice").textContent = message;
  $("notice").hidden = !message;
}

function connectivity(message, color) {
  $("connection-status").textContent = message;
  $("connection-dot").className = `dot ${color || ""}`;
}

function disconnect(message = "") {
  state.token = "";
  state.connected = false;
  clearTimeout(state.timer);
  $("token").value = "";
  $("connect-error").textContent = message;
  $("connect-screen").hidden = false;
  $("workspace").hidden = true;
  $("disconnect").hidden = true;
  state.conversations = [];
  state.pendingSubmission = null;
  state.pendingConversation = null;
  state.transcriptSignature = "";
  state.sidebarSignature = "";
  state.decisionNodes.clear();
  $("transcript").replaceChildren();
  $("conversation-list").replaceChildren();
  $("inbox").replaceChildren();
  $("pending-admissions").replaceChildren();
  $("message").value = "";
  connectivity("Not connected");
}

$("connect-form").addEventListener("submit", async event => {
  event.preventDefault();
  const button = event.currentTarget.querySelector("button");
  button.disabled = true;
  state.token = $("token").value;
  $("connect-error").textContent = "";
  try {
    const session = await api("/session");
    $("token").value = "";
    state.connected = true;
    $("connect-screen").hidden = true;
    $("workspace").hidden = false;
    $("disconnect").hidden = false;
    $("agent-label").textContent = session.agent_id === "notebook-demo-v1" ? "Local notebook demo" : "Personal notebook agent";
    notice(session.healthy ? "" : "The agent needs attention. Saved history and pending decisions are still available.");
    await refresh();
  } catch (error) {
    state.token = "";
    $("connect-error").textContent = error.message;
  } finally { button.disabled = false; }
});
$("disconnect").addEventListener("click", () => disconnect());
$("refresh").addEventListener("click", () => refresh());

function selectConversation(id) {
  state.selected = id;
  state.historyPages = 1;
  state.transcriptSignature = "";
  location.hash = id;
  notice("");
  refresh();
}

function renderConversations(data) {
  state.conversations = data.items;
  if (!state.selected && data.items.length) state.selected = data.items[0].id;
  const signature = JSON.stringify([data.items, state.selected]);
  if (signature !== state.sidebarSignature) {
    const buttons = data.items.map(conversation => {
      const button = node("button", conversation.title, "conversation-item");
      button.type = "button";
      if (conversation.id === state.selected) button.setAttribute("aria-current", "page");
      button.append(node("small", new Date(conversation.created_at_ms).toLocaleDateString(undefined, { month: "short", day: "numeric" })));
      button.addEventListener("click", () => selectConversation(conversation.id));
      return button;
    });
    $("conversation-list").replaceChildren(...buttons);
    state.sidebarSignature = signature;
  }
  $("no-conversations").hidden = data.items.length !== 0;
  $("more-conversations").hidden = !data.more;
}

function statusFor(run) {
  if (typeof run.phase === "object" && run.phase.Settled) {
    const settled = run.phase.Settled;
    if (settled === "Cancelled") return ["Cancelled", ""];
    if ("Completed" in settled) return ["Completed", ""];
    if ("Failed" in settled) return ["Failed", "failed"];
    return ["Stopped · limit reached", "failed"];
  }
  if (run.cancel_requested) return ["Cancelling", "attention"];
  if (run.last_error) return ["Needs attention", "failed"];
  if (run.decisions.length) return ["Waiting for you", "attention"];
  if (run.operations.some(op => op.status?.Waiting?.Until)) return ["Waiting until scheduled time", "attention"];
  return [run.phase === "Ready" ? "Queued" : "Working", ""];
}

function message(speaker, text, kind) {
  const element = node("div", undefined, `message ${kind || ""}`);
  element.append(node("div", speaker, "speaker"), node("p", text, "message-text"));
  return element;
}

function details(label, value) {
  const element = node("details");
  element.append(node("summary", label), node("pre", valueText(value)));
  return element;
}

function renderHistory(data) {
  $("conversation-title").textContent = data.raw.conversation.title;
  $("more-history").hidden = !data.more;
  const signature = JSON.stringify(data.items);
  if (signature !== state.transcriptSignature) {
    const blocks = [...data.items].reverse().map(item => {
      const section = node("article", undefined, "run");
      const run = item.run;
      const top = node("div", undefined, "run-top");
      const [label, style] = run ? statusFor(run) : [item.admission === "rejected" ? "Not submitted" : "Recovering submission", "attention"];
      const identity = node("span", run ? `Run ${run.id.slice(0, 8)}` : `Request ${item.key.slice(0, 8)}`, "run-meta");
      identity.title = run?.id || item.key;
      top.append(identity, node("span", label, `status ${style}`));
      section.append(top, message("You", valueText(item.input), "user"));
      if (!run) {
        section.append(node("p", item.error || "Your request is saved and will be recovered automatically.", "notice"));
        return section;
      }
      // The run's input is shown above. A steering instruction is user input the model saw
      // mid-run, so it is shown in place even though this application does not send one.
      for (const entry of run.history) {
        if (entry.Steering) { section.append(message("You · instruction", entry.Steering.instruction, "user")); continue; }
        if (!entry.Exchange) continue;
        const exchange = entry.Exchange;
        if (exchange.response.output !== null && exchange.response.output !== undefined) section.append(message("Agent", valueText(exchange.response.output)));
      }
      if (run.operations.length) {
        const activity = node("div", undefined, "activity");
        activity.append(node("p", `${run.operations.length} operation${run.operations.length === 1 ? "" : "s"} · ${run.usage.model_calls} model turn${run.usage.model_calls === 1 ? "" : "s"}`));
        for (const op of run.operations) {
          const opStatus = typeof op.status === "string" ? op.status : Object.keys(op.status)[0];
          activity.append(details(`${op.call.name} · ${opStatus}`, { call_id: op.call.id, arguments: op.call.arguments, outcome: op.status }));
        }
        section.append(activity);
      }
      const settled = run.phase?.Settled;
      if (settled?.Failed) section.append(message("Run failed", settled.Failed));
      if (settled?.Stopped) section.append(message("Run stopped", `The run reached its ${settled.Stopped} limit. Review its history before starting another request.`));
      if (run.last_error) section.append(node("p", run.last_error, "notice"));
      if (run.can_cancel) {
        const cancel = node("button", "Cancel run", "quiet small danger");
        cancel.addEventListener("click", async () => {
          cancel.disabled = true;
          try { await api(`/runs/${run.id}/cancel`, {}); notice("Cancellation requested. Any uncertain outcome will stay in the inbox."); await refresh(); }
          catch (error) { notice(error.message); cancel.disabled = false; }
        });
        section.append(cancel);
      }
      return section;
    });
    if (!blocks.length) {
      const empty = node("div", undefined, "empty");
      empty.append(node("h2", "What’s on your mind?"), node("p", "Send your first message. The agent will ask when it needs your input or approval."));
      blocks.push(empty);
    }
    $("transcript").replaceChildren(...blocks);
    state.transcriptSignature = signature;
  }
  const busy = data.items.some(item => item.admission === "pending" || (item.run && !item.run.phase?.Settled));
  $("message").disabled = state.sending || busy;
  $("send-message").disabled = state.sending || busy;
  $("composer-hint").textContent = busy ? "Finish the current run or cancel it to send another message." : "The agent will ask before saving a note.";
}

async function applyDecision(card, decision) {
  const current = card.decision;
  const controls = [...card.querySelectorAll("button, textarea, select")];
  controls.forEach(control => { control.disabled = true; });
  card.error.textContent = "";
  try {
    await api(`/runs/${current.run_id}/resolve`, {
      call_id: current.call_id, generation: current.generation, revision: current.revision, decision,
    });
    notice("Decision saved.");
    await refresh();
  } catch (error) {
    card.error.textContent = error.message;
    if (error.status === 409) await refresh();
  } finally { controls.forEach(control => { control.disabled = false; }); }
}

function decisionCard(decision, conversationId) {
  const card = node("article", undefined, "decision");
  card.decision = decision;
  const labels = { approval: "Approval requested", input: "A question for you", reconciliation: "Outcome needs checking" };
  const prompt = decision.kind === "approval" && decision.tool === "save_note" ? "Save this note?" : decision.prompt;
  card.append(node("p", labels[decision.kind], "eyebrow"), node("h3", prompt));
  if (decision.kind === "approval") {
    if (decision.tool === "save_note") {
      const preview = node("div", undefined, "note-preview");
      preview.append(node("strong", decision.arguments.title), node("p", decision.arguments.body));
      card.append(preview);
    } else card.append(node("pre", valueText(decision.arguments)));
  }
  const identity = node("div", undefined, "identity");
  const open = node("button", `Run ${decision.run_id.slice(0, 8)} · ${decision.call_id}`);
  open.type = "button";
  open.title = `Run ${decision.run_id}\nOperation ${decision.call_id}`;
  open.addEventListener("click", () => selectConversation(conversationId));
  identity.append(open);
  card.append(identity, details(`${decision.tool} · proposed details`, { run_id: decision.run_id, call_id: decision.call_id, arguments: decision.arguments }));
  if (decision.kind === "approval") {
    const actions = node("div", undefined, "actions");
    const deny = node("button", "Deny", "danger");
    const approve = node("button", "Approve", "primary");
    deny.addEventListener("click", () => applyDecision(card, { kind: "deny" }));
    approve.addEventListener("click", () => applyDecision(card, { kind: "approve" }));
    actions.append(deny, approve);
    card.append(actions);
  } else {
    const form = node("form");
    const id = `reply-${decision.run_id}-${encodeURIComponent(decision.call_id)}-${decision.kind}`;
    const label = node("label", decision.kind === "input" ? "Your reply" : "Verified outcome (JSON)");
    label.htmlFor = id;
    const input = node("textarea");
    input.id = id; input.required = true; input.maxLength = 16000; input.rows = 3;
    form.append(label, input);
    let outcome;
    if (decision.kind === "reconciliation") {
      form.prepend(node("p", "Check the destination before recording a result. This records what happened; it does not repeat the action.", "hint"));
      const outcomeLabel = node("label", "What happened?");
      outcome = node("select"); outcome.id = `${id}-outcome`; outcomeLabel.htmlFor = outcome.id; outcome.required = true;
      for (const [value, text] of [["", "Choose a verified outcome"], ["success", "The action succeeded"], ["failure", "The action failed"]]) {
        const option = node("option", text); option.value = value; outcome.append(option);
      }
      form.append(outcomeLabel, outcome);
    }
    const actions = node("div", undefined, "actions");
    actions.append(node("button", decision.kind === "input" ? "Send reply" : "Record outcome", "primary"));
    form.append(actions);
    form.addEventListener("submit", event => {
      event.preventDefault();
      if (decision.kind === "input") applyDecision(card, { kind: "input", value: input.value });
      else {
        try { applyDecision(card, { kind: "reconcile", value: JSON.parse(input.value), is_error: outcome.value === "failure" }); }
        catch { card.error.textContent = "Enter valid JSON, such as a quoted message or an object with a receipt ID."; }
      }
    });
    card.append(form);
  }
  card.error = node("p", "", "error");
  card.error.setAttribute("role", "alert");
  card.append(card.error);
  return card;
}

function renderInbox(data) {
  const active = new Set();
  const cards = [];
  const pending = [];
  for (const item of data.items) {
    if (!item.run) pending.push(node("p", item.error || "Recovering a saved submission…", "notice"));
    for (const decision of item.run?.decisions || []) {
      const key = `${decision.run_id}/${decision.call_id}/${decision.kind}`;
      active.add(key);
      let card = state.decisionNodes.get(key);
      if (!card) { card = decisionCard(decision, item.conversation_id); state.decisionNodes.set(key, card); }
      // Preserve typed replies and keyboard focus through background refreshes.
      card.decision = decision;
      cards.push(card);
    }
  }
  for (const [key, card] of state.decisionNodes) {
    if (!active.has(key)) { card.remove(); state.decisionNodes.delete(key); }
  }
  for (const card of cards) if (card.parentElement !== $("inbox")) $("inbox").append(card);
  $("decision-count").textContent = `${cards.length}${data.more ? "+" : ""}`;
  $("inbox-empty").hidden = cards.length > 0;
  $("pending-admissions").replaceChildren(...pending);
  $("more-inbox").hidden = !data.more;
}

async function refresh() {
  if (!state.connected || state.refreshing) return;
  clearTimeout(state.timer);
  state.refreshing = true;
  try {
    const [conversations, inbox] = await Promise.all([pages("/conversations", state.conversationPages), pages("/inbox", state.inboxPages)]);
    if (!state.connected) return;
    renderConversations(conversations);
    renderInbox(inbox);
    const selected = state.selected;
    if (selected) {
      const history = await pages(`/conversations/${encodeURIComponent(selected)}`, state.historyPages, "submissions");
      if (state.connected && state.selected === selected) renderHistory(history);
    }
    connectivity("Saved state connected", "green");
    if (state.networkError) { notice(""); state.networkError = false; }
  } catch (error) {
    if (state.connected) { state.networkError = true; connectivity("Connection lost · retrying", "amber"); notice(`${error.message} Your saved work will reappear when the connection returns.`); }
  } finally {
    state.refreshing = false;
    if (state.connected) state.timer = setTimeout(refresh, 1500);
  }
}

$("new-conversation").addEventListener("click", () => { $("new-conversation-form").hidden = false; $("conversation-name").focus(); });
$("cancel-new").addEventListener("click", () => { $("new-conversation-form").hidden = true; state.pendingConversation = null; });
$("new-conversation-form").addEventListener("submit", async event => {
  event.preventDefault();
  const button = event.currentTarget.querySelector("button"); button.disabled = true;
  const title = $("conversation-name").value.trim();
  if (!state.pendingConversation || state.pendingConversation.title !== title) state.pendingConversation = { id: crypto.randomUUID(), title };
  try {
    const conversation = await api("/conversations", state.pendingConversation);
    state.pendingConversation = null;
    $("new-conversation-form").hidden = true; $("conversation-name").value = "";
    selectConversation(conversation.id);
  } catch (error) { notice(error.message); }
  finally { button.disabled = false; }
});

$("message-form").addEventListener("submit", async event => {
  event.preventDefault();
  if (!state.selected || state.sending) return;
  const input = $("message").value;
  if (!state.pendingSubmission || state.pendingSubmission.input !== input || state.pendingSubmission.conversation !== state.selected) {
    state.pendingSubmission = { conversation: state.selected, input, key: crypto.randomUUID() };
  }
  state.sending = true;
  $("send-message").disabled = true;
  try {
    const request = state.pendingSubmission;
    await api(`/conversations/${request.conversation}/runs`, { key: request.key, input: request.input, expires_at_ms: null });
    $("message").value = "";
    state.pendingSubmission = null;
    notice("");
  } catch (error) {
    // Transport failures keep the key for a safe retry. A definite rejection needs a new key.
    if (error.status && error.status < 500) state.pendingSubmission = null;
    notice(error.message);
  } finally {
    state.sending = false;
    $("send-message").disabled = false;
    await refresh();
  }
});

for (const [id, field] of [["more-conversations", "conversationPages"], ["more-history", "historyPages"], ["more-inbox", "inboxPages"]]) {
  $(id).addEventListener("click", () => { state[field]++; refresh(); });
}
window.addEventListener("online", () => refresh());
