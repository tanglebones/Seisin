# Coding Guidelines

Default coding conventions for Claude Code (and any other coding agent) to follow in this environment — general principles plus backend, frontend, database, infra, and game-dev specifics. Trim, merge, or extend as conventions evolve; this is a living reference, not a historical record.

---

## How the Agent Should Use This Document

**These are defaults, not hard rules.** Follow every guideline below automatically, without being asked, whenever it applies to the task at hand. But use judgment about when a guideline doesn't fit — and when it doesn't, don't silently comply and don't silently ignore it either. Instead:

1. **Follow by default.** Apply the relevant guidelines below to any code you write or modify, without waiting to be told.
2. **Flag tension before overriding.** If following a guideline would be actively wrong for the task — it conflicts with an explicit user instruction, contradicts an established pattern already used elsewhere in the codebase, doesn't fit the language/framework/tooling actually in play, or the underlying tradeoff clearly doesn't apply here — stop and ask the user for an explicit exception before proceeding. Say which guideline is in tension and why you think an exception is warranted. Don't guess at whether the user would be fine with it; ask.
3. **User instructions win, but still get flagged.** If the user has explicitly asked for something that conflicts with a guideline, follow the user's instruction — but still call out the conflict rather than silently complying, and document it per rule 4.
4. **Always report and document a deviation, once one happens.** Whether the exception came from the user granting it, from an explicit user instruction, or from your own judgment call in a case too minor to interrupt for:
   - **Tell the user in your response** which guideline was not followed and why.
   - **Leave a short comment in the code at the point of deviation** explaining *why* the guideline wasn't followed — not what the code does. For example: `// Deviates from indexing guidance: no index on this FK — table is <100 rows and never queried by it.`
5. **When genuinely unsure whether a guideline applies**, ask rather than assume either way — silence is never treated as permission to skip a guideline.
6. **Building a specific subsystem this repo has dedicated reference material for** (e.g. a login/auth system) — check the `systems/` directory (`.guidelines/systems/` in a consumer repo) for a matching `systems/<name>.md` before starting, and read it. These are deliberately *not* part of the always-loaded guideline set above — they're niche enough to only matter for that specific type of work, so they have to be sought out explicitly rather than expected to already be in context.

---

## General Principles (language-agnostic)

- **Match existing style first.** Investigate the surrounding code/conventions before changing anything; keep diffs minimal and focused on the task at hand.
- **No dead weight.** Remove dead code, unused variables, write-only variables, leftover debug output, and stale TODOs as you touch a file.
- **Explicit dependency passing over hidden globals/IoC containers.** Prefer a composition root that wires real dependencies once, with libraries taking dependencies as plain constructor/function parameters, over DI containers or service-locators.
  ```ts
  // Dependencies flow in as ordinary params, not resolved from a container.
  async function createWidgetsInStore(
    storeClient: StoreClientType,
    dbProvider: DbProviderType,
    widgetsToCreate: WidgetCreateType[],
  ): Promise<CreateResultType> { ... }
  ```
- **Two-phase plan → execute for anything destructive or hard to reverse.** Dry-run/plan first, re-verify state, then execute; make execution idempotent and resumable; log every action and error to a durable, timestamped file.
- **Dry-run by default** for any tool that mutates production state or deletes things; require an explicit flag (`-Execute`, `--apply`, etc.) to actually mutate.
  ```powershell
  param([switch]$Execute)
  $dryRun = -not $Execute.IsPresent
  if ($dryRun) { Write-Host "DRY RUN: pass -Execute to actually remove '$name'" }
  else { Remove-Item $name -Recurse } # only reached with -Execute
  ```
- **Small, single-purpose, reusable modules** over large monoliths or inline duplication; hoist generic helpers into a shared library/package rather than copy-pasting.
- **Exceptions are for the unexpected, not control flow.** Never silently catch-and-log without a comment explaining why; never swallow or re-wrap an exception in a way that loses the stack trace (`throw;`, not `throw ex;`; not `new Exception(ex.Message)`).
  ```csharp
  try { await ProcessAsync(item); }
  catch (Exception ex) {
      _logger.LogError(ex, "Failed processing {ItemId}", item.Id);
      throw; // preserves the original stack trace — never `throw ex;`
  }
  ```
- **Never let a client-supplied value control a security decision** (auth bypass, 2FA skip, etc.) — that must always be a server-derived decision. Treat "off by default" security toggles as fragile; test the off-state explicitly.
  ```csharp
  // Anti-pattern actually seen in the wild — don't do this:
  if (model.SkipTwoFactorCheck) await _userManager.SetTwoFactorSkipAsync(model.UserName);
  // A client can set SkipTwoFactorCheck=true on the request and bypass 2FA entirely.
  // The decision to skip 2FA must be derived server-side (e.g. from a trusted device claim).
  ```
- **Secrets never hardcoded** — environment variables, CI-token-replacement, or encrypted-at-rest, always documented, never committed in plaintext.
- **Use cryptographically strong randomness** for anything security- or ID-relevant (e.g. `Guid.NewGuid()`, never `new Guid()`; UUIDv7 for DB primary keys, not sequential/guessable IDs).
- **2-space indentation** is the default house style across languages; treat 4-space/tabs as the outlier if you see it.
- **Bump the version as part of every change**, not as an afterthought: patch per change, minor for new features, major for breaking changes.
- **Deterministic tests.** Prefer seeded/mock time and randomness (a fake clock abstraction, an in-memory filesystem, fixed PRNG seeds) over real sleeps or nondeterministic inputs, so tests are reproducible and fast.
  ```csharp
  var fakeClock = A.Fake<IClock>();
  A.CallTo(() => fakeClock.UtcNow()).Returns(new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero));
  ```
- **Doc comments explain *why*, not *what*** — invariants, non-obvious constraints, workarounds for a specific bug. Skip comments that just restate the code.
- **Write project docs (`README.md`, architecture notes, ADRs, etc.) for a fresh reader with zero prior context** — someone who wasn't part of whatever discussion, PR review, or chat session led to the change. Never write "as discussed," "per your request," "we decided," "the user asked for," or similar — state the resulting fact, decision, or rationale directly, as if it had always been true. If the *why* behind a decision matters, capture the durable reason (a constraint, a past incident, a tradeoff) rather than who asked for it or where it came from.
  ```md
  <!-- Bad: assumes the reader was in the room -->
  We decided to switch to keyset pagination like you asked.

  <!-- Good: states the fact and its durable rationale -->
  Pagination uses the keyset pattern — offset pagination degrades badly past large page counts.
  ```

---

## Backend

### General backend guidelines
- Layer strictly: handlers/controllers never touch data access directly; a repository/data-access layer sits in between.
- Return structured, stable error codes/mnemonics from the API boundary; never leak raw exception details or stack traces to clients. Map error codes centrally on the consuming (frontend) side rather than ad hoc per call site.
- Every mutation of important state should be auditable — either an audit-log table, structured logging, or both — especially for anything with compliance implications. See `observability` for the actor-centric pattern this actually looks like in practice.
- Prefer reversible actions (deactivate/soft-delete) over hard deletes where the domain allows it.
- Async all the way — no sync-over-async blocking; wrap unit-of-work/connections in `using`/RAII so they're always released.
- **Don't default to REST out of habit.** gRPC and a custom WebSocket/RPC transport (a single message-envelope-based channel) are legitimate choices, not exotic fallbacks — pick based on the actual need: simple resource CRUD consumed by a browser fits REST-ish conventions fine; high-throughput bidirectional or streaming workloads are often better served by gRPC or a WebSocket RPC channel instead.
- **HTTP status codes describe transport-level outcomes only — never the business/endpoint result.** A status code answers "did a valid route get reached, did the connection/transfer itself succeed" — nothing about what the endpoint's own logic decided. `404` means *no such endpoint exists at this path*, not "the endpoint ran and didn't find the widget you asked for" (that's a successfully-executed request with a not-found *result* — respond `200` with that result in the payload). `500` means the server process itself failed to produce a response (an unhandled crash), not "the request failed validation" or "the domain operation was rejected." Every endpoint response carries its actual outcome — success or error alike — in the payload via one consistent discriminated result envelope (ties to the structured error-mnemonics bullet above), so client code branches on payload contents, never on `response.status`.
  ```ts
  type ApiResult<T> =
    | { ok: true; data: T }
    | { ok: false; code: string; message: string }; // e.g. "WIDGET_NOT_FOUND", "VALIDATION_FAILED"

  // Both "found" and "not found" are successful invocations of the endpoint —
  // both get HTTP 200, and the caller branches on `result.ok`, not on status.
  const result: ApiResult<WidgetType> = await widgetService.find(widgetId);
  ```
  Using `400`/`401`/`403`/`422` to encode validation failures, auth decisions, or domain rejections is a common but incorrect pattern this guidance deliberately rejects — those are business results too, and belong in the payload alongside everything else. Status codes stay legitimately relevant only for things actually happening at the network/transport layer: redirects, content negotiation, rate limiting or circuit-breaking enforced by a gateway/load balancer in front of the app, and genuine upstream transport failures (`502`/`503`/`504`) — none of which is the endpoint's own business logic speaking.
- **Never emit explicit `null`s in a JSON API response — model optionality more explicitly** (an absent key, a discriminated `{ present: false }`-style shape, or a separate endpoint/field for the optional data). This is a general JSON payload convention, not a C#-specific one — see `backend-csharp` for the language-specific reminder.
- **Encode any integer that could exceed `2^53 - 1` (JavaScript's safe-integer limit, `Number.MAX_SAFE_INTEGER`) as a JSON string, never a JSON number.** JSON itself has no separate integer type — numbers are parsed as IEEE-754 doubles by JavaScript (and by many other JSON consumers), which can only represent integers exactly up to `2^53 - 1`. Any 64-bit integer, snowflake-style ID, large sequence number, or monetary amount in minor units at scale can silently exceed that and lose precision on the wire with no error raised — the response parses fine, the number is just quietly wrong. `int32`-range values are always safe and don't need this; the risk starts well before a true 64-bit range, so encode as a string whenever a field's range isn't provably bounded well under `2^53`.
  ```ts
  type WidgetResponse = {
    widgetId: string;      // UUIDv7 primary key — already a string, no issue
    sequenceNumber: string; // a bigint-backed counter — encoded as a string, not a number
    quantity: number;       // small, provably bounded — a plain number is fine
  };
  ```

### API idempotency & versioning

- **Idempotency keys for any retried mutating request.** A client that never gets a response (timeout, dropped connection) can't tell whether the request succeeded server-side or not — a naive retry risks double-creating, double-charging, or double-sending. The client generates one key per logical operation (not per HTTP attempt) and sends it with the request; the server durably records the key before performing the operation, and on a repeat with the same key, returns the original recorded result instead of repeating the effect. This is the same durable-intent/completion pattern as the externality handling in `systems/background-jobs.md` — same underlying problem (a retried call, no way to tell if it already happened), same fix.
  ```sql
  create table idempotent_request (
    idempotency_key text primary key,
    request_hash text not null,   -- hash of the request body, to catch a key reused for a *different* request
    response_body jsonb,          -- null until the operation actually completes
    created_at timestamptz not null default clock_timestamp()
  );
  ```
  Insert the key first (`on conflict do nothing`); if a row already exists with a `response_body` set, return that instead of re-running the operation; if `request_hash` doesn't match what's stored, that's a client bug (reusing a key for a genuinely different request) — reject it rather than silently doing something with it. Reserve this for operations where a duplicate would actually cause harm (create, charge, send) — a read is already naturally safe to retry and doesn't need it.
- **A deliberate versioning and deprecation policy for endpoints and payload shapes.** Pick one axis to version on (a URL/header-based API version, e.g. `/v2/widgets`, or payload-shape backward-compatibility rules) and apply it consistently — don't mix both informally. Additive changes (a new optional field) don't need a version bump; anything that changes what an existing field means, removes a field, or changes its required-ness is breaking and does. Deprecation needs an actual lifecycle — mark it, publish a real sunset date in advance, keep serving the old version until then — never silently break or remove a version clients are still calling. The same discipline already applies to the audit-event mnemonics in `observability` (`_v1`/`_v2` as a new type, never silently repurposing what an existing mnemonic means) — apply it here too, including to error codes inside the discriminated result envelope from the section above.

### Security patterns

- **Never trust client input, for data or for authorization.** Frontend validation and route guards are UX conveniences, not security controls — a client can always bypass them (a raw HTTP call, a modified request, a different client entirely). This generalizes the "never let a client-supplied value control a security decision" principle in `core-principles` to every input, not just explicit security-toggle fields.
- **Re-check authorization for the specific resource on every request, not just "is this route behind a login gate."** Being authenticated proves who the caller is; it doesn't prove they're allowed to act on the particular resource ID in *this* request. Skipping the per-resource check is how one user ends up reading or mutating another user's data just by changing an ID (IDOR) — the fix is checking ownership/permission against the resource actually being touched, every time, not trusting that reaching the handler at all implies authorization.
- **Validate and constrain every input at the API boundary** — type, range, length, format — before it reaches business logic, regardless of what the client-side form already checked.
- **Never bind a request body directly onto a domain/DB model (mass assignment).** Map only the specific fields a given operation is allowed to set; otherwise a client can set fields it was never meant to control (`isAdmin`, `accountBalance`, `role`) just by including extra keys in the JSON body.
  ```ts
  // Anti-pattern: whatever the client sent becomes the update
  await widgetRepo.update(widgetId, req.body);
  // Explicit allowlist instead: only these fields can ever be set this way
  await widgetRepo.update(widgetId, { name: req.body.name, description: req.body.description });
  ```
- **Encode output at the point of rendering, for the context it's rendered into** (HTML-escape for HTML, JS-string-escape for inline scripts, etc.) — sanitizing on the way in is not a substitute, since the same stored value may end up rendered in more than one context with different escaping needs.
- **CSRF protection on every state-changing endpoint reachable via a browser session cookie** — `SameSite` cookies plus an anti-forgery token for cookie-authenticated requests. Not needed for a pure bearer-token API the browser never attaches automatically, but don't assume that's what you have without checking.
- **Set security response headers centrally** (a CSP, `X-Content-Type-Options: nosniff`, HSTS, etc.), via shared middleware — not decided ad hoc per endpoint, where it's easy for a new endpoint to simply be forgotten.
- **Least-privilege service/DB credentials.** The application's own database role or service account should hold only the permissions it actually exercises, not a superuser/admin connection reused everywhere out of convenience — a compromised app process shouldn't automatically mean a compromised database.
- **Dependency/supply-chain integrity**: commit the lockfile, audit dependencies for known vulnerabilities as part of CI (e.g. `npm audit`, per the equivalent bullet in `frontend-angular`), and track any necessary exception in an explicit allowlist rather than silently ignoring a finding.
- **Rate-limit and abuse-protect any expensive or sensitive endpoint, not only login.** `systems/login.md` documents the detailed pattern (per-account + per-source keying, capped backoff, CAPTCHA escalation) — the same shape applies to any endpoint that's costly to run or attractive to abuse (password reset requests, expensive search/export operations, anything that sends email/SMS on the caller's behalf).

### Rust

- Prefer many small, single-purpose crates over one monolith; put shared logic in a thin library crate at the bottom of the dependency graph.
  ```
  workspace/
    rt/          # thin shared lib: error alias, small helpers — no dependents
    widget_fetch/
    widget_normalize/
    widget_export/
  ```
- **`anyhow::Result<T>` + `bail!()`/`.context()` everywhere** — this is the only accepted error-handling style now. Do not introduce a hand-rolled error enum or a project-wide `Rt<T> = Result<T, Box<dyn Error>>` alias for new code; treat any existing `Rt<T>`-style alias as legacy to migrate away from when touched.
  ```rust
  use anyhow::{Result, Context, bail};

  fn parse_widget(bytes: &[u8]) -> Result<Widget> {
      if bytes.first() != Some(&b'W') {
          bail!("invalid widget header");
      }
      let body = std::str::from_utf8(bytes).context("widget body was not valid utf8")?;
      Ok(Widget::from_str(body)?)
  }
  ```
- `rustfmt`/`cargo fmt --check` and `cargo clippy --all-targets` (often `-D warnings`) enforced in CI; 2-space indent via `rustfmt.toml` is the deliberate house style, not default rustfmt (`tab_spaces = 2`, `newline_style = "Unix"`).
- `#![deny(warnings)]` at the crate root for services where that's appropriate.
- **Avoid `unsafe` — reach for it only when there's genuinely no safe alternative** (an FFI boundary, a documented perf-critical hot path where the safe version was actually measured to be too slow). When it's unavoidable, every `unsafe` block gets a `// SAFETY:` comment immediately above it explaining *why* it's needed and *how* the invariant the compiler can't check is actually upheld — not just restating that it's unsafe.
  ```rust
  // SAFETY: `ptr` was just returned by `Box::into_raw` above and hasn't been
  // freed or aliased since, so reconstructing a Box from it here is sound.
  let widget = unsafe { Box::from_raw(ptr) };
  ```
- Config via `serde` + RON for human-edited files, not JSON/YAML.
  ```rust
  #[derive(serde::Deserialize)]
  struct WidgetConfig { batch_size: u32, endpoint: String }

  impl Default for WidgetConfig {
      fn default() -> Self { ron::from_str(DEFAULT_CONFIG_RON).unwrap() }
  }
  ```
- Time/randomness abstracted behind a trait (e.g. a `ClockSource`) so it can be faked in tests instead of sleeping or relying on real entropy.
  ```rust
  trait ClockSource { fn now(&self) -> Instant; }
  struct RealClock;
  impl ClockSource for RealClock { fn now(&self) -> Instant { Instant::now() } }
  struct TestClock(Cell<Instant>); // advance() manually in tests, never sleep
  ```
- Doc comments (`//!` module docs, `///` on public fns) spell out invariants, panics, and algorithmic guarantees — treated as load-bearing documentation, not boilerplate.
  ```rust
  /// Accumulates stats without a lock; fields update independently, so
  /// cross-field consistency is only guaranteed once writes have quiesced.
  pub struct StreamingStats { ... }
  ```
- Known-answer/round-trip tests as `const` arrays or deterministic hand-rolled PRNGs, preferred over pulling in a fuzzing dependency for small closed-form problems.
  ```rust
  // Verified against the spec, not against the implementation.
  const VECTORS: &[(&[u8], &str)] = &[(b"", ""), (b"f", "Zg=="), (b"fo", "Zm8=")];
  #[test]
  fn encodes_known_vectors() {
      for (input, expected) in VECTORS { assert_eq!(encode(input), *expected); }
  }
  ```
- `#[ignore]`-tag tests that need an external service so `cargo test` stays fast and hermetic by default.
  ```rust
  #[test]
  #[ignore] // requires a running postgres instance
  fn round_trips_through_postgres() { ... }
  ```

---

## Testing

A unifying philosophy across the per-language testing conventions already documented elsewhere in this guideline set (NUnit + FakeItEasy in `backend-csharp`, Vitest in `frontend-react`, GUT in `game-godot`, `ClockSource`/`Dep` fakes in `backend-rust`/`backend-csharp`) — this doesn't replace those, it's the shape that should hold across all of them.

### Test pyramid shape

- **Most coverage comes from fast, in-process unit tests** exercising business logic with fakes for anything non-deterministic (clock, randomness, filesystem) — the `Dep`/`ClockSource`-style seams already documented per language exist specifically to make this cheap.
- **A second, smaller layer of integration tests verifies the actual data-access/SQL layer against a real database** — see below; this is not optional given this repo's no-ORM stance.
- **A thin top layer of end-to-end/UI tests** exercises real user flows through the full stack. Keep this the smallest layer on purpose — it's the slowest to run and the most brittle to unrelated changes, so reserve it for the handful of flows that actually need full-stack confidence, not as a substitute for the layers below it.

### Integration tests hit a real database — never a mocked connection

**Given this repo's no-ORM stance — SQL is hand-written and reviewed directly against the target engine, not generated — a mocked DB connection in a test proves the calling code invoked *some* function, and nothing about whether the SQL itself is correct.** A join written wrong, a constraint that doesn't fire, a query that returns the wrong shape — none of that is caught by a fake that just returns canned data regardless of what query was sent. Integration tests for repository/data-access code should run against a real instance of the same engine used in production:

- **SQLite/DuckDB**: cheap — an in-process `:memory:` instantiation per test, the same "ephemeral, rebuildable" case already documented in `database-sqlite`/`database-duckdb`. No separate service needed, and there's no excuse not to do this given how little it costs.
- **Postgres**: a real disposable instance (a test container, or a locally running dev instance reset between runs) — there's no meaningful in-memory substitute that actually exercises Postgres-specific behavior (GiST exclusion constraints, `unnest`, `ON CONFLICT`, partial indexes) faithfully enough to trust.

**What's still fine to fake**: the clock, randomness, the filesystem, and any third-party network dependency — none of those are the thing this repo has a strong, deliberate opinion about writing directly, unlike SQL. Don't extend "test against the real thing" to those; the existing per-language fake-seam patterns are the right call for them.

### An accepted floor even without deep coverage

A trivial "constructs/instantiates without throwing" smoke test is an accepted floor for any class/component that otherwise lacks deeper tests — already the convention in `backend-csharp`'s coverage-gate discussion and `frontend-angular`'s `TestBed` "should create" test, stated here once as a general expectation rather than restated per language.

### Tests requiring a live external dependency

Tag and exclude these from the default fast test run (`backend-rust`'s `#[ignore]`, or the equivalent convention in whatever language is in play) rather than deleting them or letting them flake the default suite — the default test command should stay fast and hermetic by default, while the coverage stays available to run deliberately (e.g. in a scheduled or pre-release CI job).

### Migrations

Already covered in `database` — migrations get tested against realistic, already-populated data, not empty tables, which is what actually surfaces real migration bugs. Not restated here.

---

## Working with AI Coding Assistants

- Review AI-generated code carefully, especially data structures and constraints — don't trust it silently, particularly around anything touching persisted data.
- Be explicit about unstated requirements (e.g. data-preservation rules) — the assistant won't infer them.
- Test AI-suggested migrations/changes against realistic, populated data, not empty tables.
- Keep a persistent, explicit "rules" document that the assistant is pointed at every session, so conventions survive across sessions instead of having to be re-explained (this file is meant to be exactly that).
