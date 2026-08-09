# Running the Configuration Generator — Step by Step

A practical walkthrough for generating a feed configuration with the
interactive wizard and putting it to work. For the full feature reference
(question groups, input forms, secret handling, extension points) see
[CONFIG_GENERATOR.md](CONFIG_GENERATOR.md).

## 1. Prerequisites

- Java 11+
- The application assembly jar. Build it once from the repo root:

```bash
mvn clean package -DskipTests
```

This produces:

```
ingestion-app/target/ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

The generator is bundled inside that jar — no extra installation. It runs on
a plain JVM; Spark is **not** required to generate configurations (only the
optional schema-contract deep validation uses Spark classes and downgrades to
a warning without them).

Tip: a shell alias keeps the commands short.

```bash
alias confgen='java -cp ingestion-app/target/ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  com.hcsc.generic.ingest.confgen.ConfigGeneratorMain'
```

## 2. Generate a feed interactively

```bash
confgen --source-type jdbc --output-dir conf/feeds
```

What happens:

1. The wizard walks you through up to nine question groups with a progress
   header (`[3/9] Authentication`). Only questions relevant to your earlier
   answers are asked.
2. Every answer is validated immediately; invalid input shows the problem and
   re-asks.
3. At the end it dry-run-validates the feed through the framework's own
   parsers, prints the extraction SQL preview (JDBC), writes the file(s), and
   prints a summary with secrets masked.

Useful keystrokes at any prompt:

| Input | Effect |
|-------|--------|
| `Enter` | Accept the `[default]` shown in the prompt |
| `?` | Show help for the current question |
| `:save` | Save a draft and exit (see §4) |
| `@/path/to/file` | Answer a list question from a file (CSV/JSON/text) |

Omit `--source-type` to be prompted for `jdbc` / `file` / `kafka`.

## 3. Choose output formats and location

```bash
confgen --source-type file --output-dir conf/feeds --formats hocon,yaml,json
```

- Files are named `<entity>.conf` / `<entity>.yaml` / `<entity>.json`.
- The framework consumes the HOCON (`.conf`) or JSON form; YAML is for teams
  whose tooling standardizes on YAML.
- Default is `--formats hocon` and `--output-dir generated-configs`.

## 4. Save a draft and resume later

Long questionnaire, meeting in five minutes? Auto-save everything:

```bash
confgen --source-type jdbc --draft ~/drafts/claims_feed.json
```

- With `--draft`, every answer is saved as you go; `:save` exits cleanly.
- Re-running the **same command** resumes exactly where you stopped —
  answered questions are not asked again.
- The draft is plain JSON; you can read or hand-edit it before resuming.

## 5. Non-interactive generation (CI / scripted)

Provide all answers up front; nothing is prompted:

```bash
confgen --answers answers.json --output-dir conf/feeds --formats hocon
```

- `answers.json` uses the same format as a saved draft
  (`{"source_type": "jdbc", "answers": [{"id": "...", "scalar": "..."}, ...]}`).
- Unanswered questions take their documented defaults (retry 3×2000 ms,
  fetchsize 1000, `raw.table` = entity, `ingest_dt` partition, ...).
- Missing required answers exit with code `2` and list the gaps — safe to
  wire into a pipeline.

Exit codes: `0` success (or draft saved) · `1` dry-run validation failed ·
`2` bad usage / incomplete answers.

## 6. Optional: test database connectivity (JDBC)

```bash
confgen --source-type jdbc --connect-test
```

After generation this resolves the real credentials and runs the same health
check the pipeline uses (`JdbcHealthCheck`). Run it where the database and
secret store are reachable; on a workstation without them the generation
still succeeds and the probe result is reported as a warning.

## 7. Use the generated configuration

The output is a standard `feeds.<entity>` block. Either reference the file
directly:

```bash
spark-submit \
  --class com.hcsc.generic.ingest.app.IngestMain \
  ingestion-app/target/ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity claims_feed --mode INCR --conf-path conf/feeds/claims_feed.conf
```

or merge the block into your existing `application.conf` under `feeds { }`.

Recommended first run for a new feed:

```bash
... IngestMain --entity claims_feed --mode FULL \
  --conf-path conf/feeds/claims_feed.conf --validate-only --explain-mapping
```

`--validate-only` exercises header/contract/connection validation without
writing any data.

## 8. Check the configuration into git

Generated configs are meant to be version-controlled. Two rules:

1. **Never commit inline secrets.** Choose `env`, `sysprop`, `file`,
   `cyberark`, `conjur` or `azure_keyvault` as the secret provider; the config then
   contains only references. The `inline` provider is for local testing and
   is flagged as such by the wizard.
2. Review the printed masked summary, then:

```bash
git add conf/feeds/claims_feed.conf
git commit -m "Add claims_feed ingestion configuration"
git push
```

Drafts (`--draft` files) may contain inline secret values if you chose the
`inline` provider — keep them out of git (add your drafts directory to
`.gitignore`).

## 9. Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `JDBC_003 Cannot infer dialect from url ...` | The URL prefix isn't a known dialect (e.g. H2). Answer the dialect question explicitly (`generic`, `sqlserver`, ...). |
| `auth.password (env) did not resolve in this environment` (warning) | Expected on a workstation — the env var/vault only exists at deploy time. Verify with `--connect-test` where the secret store is reachable. |
| `schema contract validation skipped: Spark classes not on the classpath` (warning) | Contract deep validation needs Spark types. Re-run validation via `--validate-only` on the cluster, or ignore — structure was still checked. |
| `STRING watermark not approved` keeps re-asking | STRING watermarks compare lexicographically and require an explicit `y`. If that's not what you want, `:save`, edit the draft's `watermark_type`, and resume. |
| Exit code `2` with a list of question ids | Your `--answers` file is missing those required answers; add them and re-run. |
| Colors garbled in logs/CI | Pass `--no-color` (also honored automatically when `NO_COLOR` is set or the terminal is dumb). |
