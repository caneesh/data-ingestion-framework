# Configuration Generator

An interactive CLI wizard (`ingestion-config-gen` module) that builds feed
configurations for JDBC, file and Kafka sources — asking only the questions
that apply, validating every answer as it is typed, and dry-run-validating the
result through the framework's **own parsers** (`JdbcSourceConfig.parse`,
`SchemaContract.parse`). A feed that generates cleanly is a feed the pipeline
will accept.

## Running it

The generator ships inside the application assembly jar:

```bash
java -cp ingestion-app/target/ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  com.hcsc.generic.ingest.confgen.ConfigGeneratorMain \
  --source-type jdbc --output-dir conf/feeds --formats hocon,yaml,json
```

| Flag | Meaning |
|------|---------|
| `--source-type jdbc\|file\|kafka` | Skip the source-type prompt |
| `--output-dir DIR` | Where generated files land (default `generated-configs`) |
| `--formats hocon,yaml,json` | Any combination; default `hocon` |
| `--draft FILE` | Auto-save every answer here; resume from it if it exists |
| `--answers FILE` | Non-interactive: pre-load answers, apply defaults, never prompt |
| `--connect-test` | After generation, probe the database via `JdbcHealthCheck` (JDBC) |
| `--no-color` | Disable ANSI colors (also honors `NO_COLOR` / dumb terminals) |

Exit codes: `0` generated (or draft saved), `1` dry-run validation failed,
`2` bad usage / incomplete answers file.

At any prompt:

- `?` — show the question's help text
- `:save` — save the draft and exit (resume later with `--draft`)
- Enter on a `[default]` — accept the default

The wizard walks nine question groups with a progress indicator —
General, Source, Authentication, Extraction, Watermark, Retry, Audit,
Performance, Destination — and shows only the questions your earlier answers
make relevant (e.g. watermark questions appear only for `INCREMENTAL`
extraction; CyberArk fields only when you pick the `cyberark` secret
provider).

## Large collections

Wherever a list is asked (columns, primary keys, watermark columns, partition
predicates, contract columns), three input forms are accepted:

```
claim_id, state, modified_ts            # inline CSV
["claim_id", "state"]                   # inline JSON array
@/path/to/columns.txt                   # file: one item per line (# = comment)
@/path/to/columns.json                  # file: JSON array
```

File references are checked for existence, parsed immediately and previewed
(`-> file columns.txt (42 items): claim_id, state, ...`) so problems surface
while you are still in the wizard. For schema-contract columns, JSON objects
carry the full contract (`name/type/nullable/required/aliases/position`);
plain CSV names become minimal `string` columns you can refine later.

## Split output: the mapping document gets its own file

For HOCON output, a feed with a schema contract is written as **two files**
so a wide mapping (hundreds of columns) does not bury the rest of the
configuration:

```
generated-configs/
  claims.conf           # feed: source, watermark, audit, raw, curated ...
  claims-schema.conf    # the schema contract (source-to-target mapping)
```

The main file pulls the contract in with a standard HOCON include:

```hocon
feeds.claims {
  include required("claims-schema.conf")

  source { ... }
  raw { ... }
  curated { ... }
}
```

`include required(...)` is resolved natively by `ConfigFactory.parseFile`
relative to the including file, so **the runtime pipeline needs no changes**
— `IngestMain --conf claims.conf` sees one logical configuration. `required`
means a missing/renamed schema file fails the parse loudly instead of
silently running without a contract. The generator re-parses the written
pair and verifies it matches the validated configuration exactly before
reporting success.

Sections in the main file are emitted in reading order (identity, source,
schema include, rejects/idempotency, audit, raw, curated) rather than
alphabetically. JSON/YAML outputs stay single-file (those formats have no
include mechanism); they carry the schema inline.

The schema-contract questions are available in both the **file** and
**JDBC** flows (opt-in for JDBC; strongly recommended for wide or
long-lived feeds). For a wide table you have two ways to produce the
mapping:

- **Introspect it** (JDBC): answer `y` to *"Generate the column mapping by
  introspecting the source table?"* — the generator connects with the
  feed's own URL/auth/TLS settings and emits one contract column per
  source column (name, Spark type, nullability, 0-based position) via
  `DatabaseMetaData`/INFORMATION_SCHEMA. SQL Server types map to what the
  Spark JDBC reader produces (`decimal(p,s)` preserved, `datetimeoffset`
  → `string`, unknown engine types → `string`), so `on_type_change`
  validation stays green. An explicitly supplied `schema.columns` answer
  always wins over introspection.
- **Supply it**: maintain the mapping as a JSON array in its own file and
  answer `@/path/columns.json`.

Either way the mapping lands in `<entity>-schema.conf`, which is the file
you review and maintain going forward (add aliases, defaults, validation
rules there).

## Secrets

Credentials are captured as **references**, never values (unless you
explicitly choose `inline`, which is flagged as testing-only): `env`,
`sysprop`, `file`, `cyberark` (CCP url/app-id/safe/object), `conjur`
(base url/account/host id/variable, with the host API key referenced from an
environment variable) and `azure_keyvault` (vault url/secret name). Dry-run validation substitutes
placeholders for secret references so a config generated on a workstation
validates without production credentials; whether the reference resolves in
*this* environment is reported as a warning, and `--connect-test` performs the
real probe. The printed summary and all logs mask secret material
structurally (`password`, `*secret*`, `token`, `jaas` keys and inline
`value` fields become `********`; provider/key metadata stays reviewable).

## SQL preview (JDBC)

After validation the generator prints the extraction SQL exactly as
`QueryBuilder` will emit it, with the first incremental window rendered from
`initial_value`:

```
SELECT claim_id, state, modified_ts FROM d_claims
WHERE (state = 'IL') AND "claim_id" > 0
-- first window shown from initial_value; later runs bound by the stored watermark
```

## Example: minimal non-interactive run

`answers.json` (same format the wizard auto-saves as a draft):

```json
{
  "source_type": "jdbc",
  "answers": [
    {"id": "entity", "scalar": "orders_feed"},
    {"id": "source.url", "scalar": "jdbc:sqlserver://srv:1433;databaseName=Orders"},
    {"id": "source.auth.type", "scalar": "MANAGED_IDENTITY"},
    {"id": "source.mode", "scalar": "FULL_TABLE"},
    {"id": "source.table", "scalar": "dbo.orders"},
    {"id": "_perf.partitioning", "scalar": "NONE"},
    {"id": "_audit.enabled", "scalar": "false"},
    {"id": "raw.database", "scalar": "orders_raw"},
    {"id": "raw.path", "scalar": "hdfs:///data/warehouse/orders/raw/orders"},
    {"id": "curated.enabled", "scalar": "false"}
  ]
}
```

```bash
... ConfigGeneratorMain --answers answers.json --formats hocon,yaml
```

Unanswered questions take their documented defaults (retry 3×2000 ms,
fetchsize 1000, `raw.table` = entity, `ingest_dt` run-date partition, ...).

## Migration guide

Existing hand-written feeds need **no changes** — the generator emits the
same `feeds.<entity>` structure `application.conf` uses today. To migrate a
legacy feed to generated form: run the wizard, answer from the old file, and
diff the output; legacy per-source options the wizard does not ask for
(`header_aliases`, plain `columns`) keep working but should move to a schema
contract, which the wizard builds for you.

## Backward compatibility report

- New module `ingestion-config-gen`; **zero changes** to existing modules
  beyond registering the module in the parent pom and bundling it in the app
  assembly.
- No new runtime dependencies: Typesafe Config only (YAML output uses a
  built-in emitter; the framework consumes the HOCON/JSON forms).
- Validation is delegated to the existing parsers, so generator and pipeline
  can never disagree; when framework validation evolves, the generator
  follows automatically.
- The wizard runs on a plain JVM. Only schema-contract deep validation needs
  Spark classes; without them it degrades to an explicit warning instead of
  failing.

## Extending

New source types plug in like every other framework extension point:
implement `SourceQuestionFlow` (a `sourceType` plus an ordered `Seq[Question]`
with `appliesWhen` conditions) and register it via
`QuestionFlowRegistry.register(...)`; the wizard, drafts, renderers and
summary handling pick it up unchanged.
