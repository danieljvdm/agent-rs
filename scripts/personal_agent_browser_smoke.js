// Run with playwright-cli run-code --filename against an isolated --demo server.
// This public fixture token is only for disposable local test data.
async page => {
  const token = "test-only-personal-agent-access-token";
  const title = `Browser check ${Date.now()}`;
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  const connect = async () => {
    await page.getByLabel("Application access token").fill(token);
    await page.getByRole("button", { name: "Connect", exact: true }).click();
    await page.getByRole("heading", { name: "Conversations", exact: true }).waitFor();
  };
  await page.reload();
  await connect();
  await page.getByRole("button", { name: "New conversation", exact: true }).click();
  await page.getByLabel("Conversation name").fill(title);
  await page.getByRole("button", { name: "Create", exact: true }).click();
  await page.getByRole("heading", { name: title, exact: true }).waitFor();
  await page.getByLabel("Message your agent").fill("Do not save this note");
  await page.getByRole("button", { name: "Send message", exact: true }).click();
  const reply = page.getByLabel("Your reply").last();
  await reply.waitFor();
  const body = "<b>Book the dentist</b> and check passport expiry.";
  await reply.fill(body);
  await page.waitForResponse(response => response.url().includes("/api/inbox") && response.ok());
  if (await reply.inputValue() !== body) throw new Error("Refresh lost the draft reply");
  if (!await reply.evaluate(element => document.activeElement === element)) throw new Error("Refresh lost keyboard focus");
  await page.getByRole("button", { name: "Send reply", exact: true }).last().click();
  const card = page.getByRole("complementary", { name: "Decision inbox" }).getByRole("article").filter({ hasText: "Do not save this note" });
  await card.getByRole("button", { name: "Deny", exact: true }).waitFor();
  if (await card.locator("b").count()) throw new Error("Untrusted note content was interpreted as HTML");

  await page.reload();
  await connect();
  await card.getByRole("button", { name: "Deny", exact: true }).waitFor();
  try {
    await page.context().setOffline(true);
    await page.getByText("Connection lost · retrying", { exact: true }).waitFor();
  } finally { await page.context().setOffline(false); }
  await page.getByText("Saved state connected", { exact: true }).waitFor();
  if (await page.getByText(/Your saved work will reappear/).isVisible()) throw new Error("Reconnect kept a stale connection error");
  await card.getByRole("button", { name: "Deny", exact: true }).click();
  await page.getByText(/The note was not saved/).waitFor();

  await page.getByLabel("Message your agent").fill("Saved browser note");
  await page.getByRole("button", { name: "Send message", exact: true }).click();
  await page.getByLabel("Your reply").last().fill("Book the dentist and check the passport expiry date.");
  await page.getByRole("button", { name: "Send reply", exact: true }).last().click();
  const approved = page.getByRole("complementary", { name: "Decision inbox" }).getByRole("article").filter({ hasText: "Saved browser note" });
  await approved.getByRole("button", { name: "Approve", exact: true }).click();
  await page.getByText("Saved “Saved browser note” to your local notebook.", { exact: true }).waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)) throw new Error("Mobile layout overflows horizontally");
  await page.setViewportSize({ width: 1440, height: 1000 });
  if (errors.length) throw new Error(errors.join("\n"));
  return { passed: true, checks: ["draft and focus survive refresh", "note content renders as text", "reload restores approval", "offline reconnect", "denial", "approved completion", "mobile width"] };
}
