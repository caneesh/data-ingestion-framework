# SQL Server Authentication Audit

Audit date: 2026-07-27 · Repo state: commit `17e159f` · Scope: complete
codebase and all deployment configuration present in this repository.

This document answers: **how does the application authenticate to SQL
Server, and where do the username/password come from — in particular, is
CyberArk/Conjur actually used?** All findings cite exact files, classes,
methods and line numbers. Line numbers are accurate as of the commit above.

---

## A. Final conclusion

- **SQL Server authentication mechanism:** Configuration-driven per feed via
  `feeds.<entity>.source.auth.type`. Supported mechanisms:
  - SQL username/password — `SQL_PASSWORD` (**the default** when no
    `auth.type` is configured)
  - Azure AD — `MANAGED_IDENTITY`, `ENTRA_SERVICE_PRINCIPAL`,
    `ENTRA_PASSWORD`, `ENTRA_DEFAULT`, `ACCESS_TOKEN`
  - Windows/Kerberos integrated — `ENTRA_INTEGRATED`
    (driver property `authentication=ActiveDirectoryIntegrated`)

  **In the repository as shipped, no active feed connects to SQL Server at
  all** — the only JDBC feed in `application.conf` is entirely commented out.

- **Username source (SQL_PASSWORD):** `source.auth.user` — a bare string or a
  secret reference resolved through the provider registry
  (`SqlPasswordAuth.resolve`, `JdbcAuthenticationProvider.scala:58-59`).
  No default, no hardcoded value.

- **Password source (SQL_PASSWORD):** `source.auth.password` (legacy fallback
  `source.password`), always resolved through the secret-provider registry:
  `env` | `sysprop` | `file` | `inline` | `cyberark` (CCP) | `conjur` |
  `azure_keyvault` (`JdbcAuthenticationProvider.scala:60-61` →
  `SecretProvider.scala:83-109`). No default, no hardcoded value.

- **Is CyberArk used for SQL Server credentials?** **Cannot confirm — wired
  into the live connection path, but not exercised by any shipped
  configuration.** The CyberArk/Conjur providers are *not* dead utility code:
  they execute whenever a feed's `auth.user`/`auth.password` object carries
  `provider = "cyberark"` or `"conjur"`. But the only configuration in the
  repo that references CyberArk is inside the commented-out `claims_data`
  example (`application.conf:202-289`). Whether CyberArk is used in
  production depends entirely on the runtime `--conf-path` / `-Dconfig.file`
  feed file, which is not in this repository.

- **Confidence level:** High for the code-path analysis (every connection
  site and resolution branch traced). Low for actual production behavior,
  because the deciding artifact — the deployed feed configuration — is
  external (see section D).

---

## B. Evidence

| # | Finding | File | Class/Method | Configuration key or code | Explanation |
|---|---------|------|--------------|---------------------------|-------------|
| 1 | Spark read connection (driver + executors) | `ingestion-jdbc/.../JdbcSource.scala:117-137` | `JdbcSource.buildReader` | `.format("jdbc")`, `.option("user"/"password", ...)` (136-137); `Properties` path for PREDICATES (121-124) | The main extraction connections. Credentials come exclusively from `cfg.user`/`cfg.password` on the parsed config. |
| 2 | Driver-side health-check connection | `JdbcHealthCheck.scala:22` | `JdbcHealthCheck.check` | `DriverManager.getConnection(cfg.url, props)` | Pre-read probe; same `cfg.user`/`cfg.password` (lines 18-19). Enabled by default (`health_check.enabled`, `JdbcSourceConfig.scala:226-227`). |
| 3 | Driver-side watermark/bounds queries | `DriverQueries.scala:25` | `DriverQueries.firstRow` | `DriverManager.getConnection(cfg.url, props)` | Upper-watermark capture and MIN/MAX bound discovery; same credential source (lines 22-23). |
| 4 | Optional executor probe | `JdbcHealthCheck.scala:59-71` | `JdbcHealthCheck.executorProbe` | `java.sql.DriverManager.getConnection(url, p)` (65) | Only when `health_check.executor_probe = true`. Captures user/password in the Spark closure (lines 54-56). |
| 5 | JDBC URL and driver | `JdbcSourceConfig.scala:99,109`; `JdbcDialect.scala:91-99` | `JdbcSourceConfig.parse`; `SqlServerDialect` | `source.url` (required); `source.driver` else `defaultDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"` | Driver jar is **not** bundled — test-scope only in `ingestion-jdbc/pom.xml` ("Production JDBC drivers supplied at deploy time"). |
| 6 | Auth mechanism selection | `JdbcSourceConfig.scala:251-260` | `resolveAuth` | `auth.type`, default `AuthType.SqlPassword` (253) | Delegates to `JdbcAuthenticationProviders.resolve`; result becomes `cfg.user`/`cfg.password`/`connectionProperties`. |
| 7 | SQL username/password resolution | `JdbcAuthenticationProvider.scala:51-63` | `SqlPasswordAuth.resolve` | `auth.user` (58, provider-resolvable), `auth.password` (60), legacy `source.password` (61) | Both credentials go through `SecretProviders.resolveAt`. |
| 8 | Azure AD modes | `JdbcAuthenticationProvider.scala:66-136` | `ManagedIdentityAuth` … `AccessTokenAuth` | driver props `authentication=ActiveDirectoryMSI/...ServicePrincipal/...Password/...Default/...Integrated`, `accessToken` | Token flows performed by the Microsoft driver on driver *and* executors; no Azure SDK involved. |
| 9 | Secret provider registry (the fork point) | `SecretProvider.scala:57-109` | `SecretProviders.resolveRef/resolveAt` | `{ provider = "env"\|"file"\|"sysprop"\|"inline"\|"cyberark"\|"conjur"\|"azure_keyvault", ... }` | Registered at lines 66-80 (defensive). Bare strings = inline with warning (99-102). |
| 10 | CyberArk CCP provider — live code | `CyberArkSecretProvider.scala:59-76,107-128` | `CyberArkSecretProvider.resolve/fetch` | `url/app_id/safe/object/attribute` | Reached from #7 only when a credential ref says `provider = "cyberark"`. |
| 11 | Conjur provider — live code | `ConjurSecretProvider.scala:41-76`; `ConjurClient.scala:96-160` | `resolve` → `ConjurClient.authenticate/getSecret` | `url/account/host_id/api_key/variable`; `POST /authn/...`, `GET /secrets/...` | Same: reached only via `provider = "conjur"`. |
| 12 | **No active JDBC feed in shipped config** | `ingestion-app/src/main/resources/application.conf:201-289` | — | `# claims_data { ... type = "jdbc" ... cyberark ... }` | The entire JDBC + CyberArk example is commented. Stripping comments and grepping for `type = "jdbc"`, `cyberark` or `conjur` returns nothing. The active feed `health_sherpa_member` is `type = "file"` (lines 89-90). |
| 13 | Config loading & precedence | `IngestMain.scala:24-29` | `IngestMain.main` | `--conf-path` else `ConfigFactory.load()`; feed = `feeds.<entity>` | `--conf-path` (explicit file) beats `-Dconfig.file` beats classpath `application.conf` (Typesafe Config standard). |
| 14 | Deployment script | `scripts/run_health_sherpa.sh` | — | `--files "$CONF_FILE"`, `-Dconfig.file=...` on driver and executors, `--entity health_sherpa_member` | The only deployment artifact in the repo; runs the **file** feed. No credentials on the command line. |
| 15 | TLS posture | `JdbcDialect.scala:94-98` | `SqlServerDialect.defaultConnectionProperties` | `encrypt=true`, `trustServerCertificate=false`, `loginTimeout=30` | Never disabled in code. ⚠️ Overridable: `connectionProperties = dialect defaults ++ authProps ++ source.connection_properties` (`JdbcSourceConfig.scala:220-222`) — a feed config *could* set `trustServerCertificate=true`. |
| 16 | Secret logging | `JdbcSource.scala:236-245`; `JdbcHealthCheck.scala:75-76`; `CyberArkSecretProvider.scala:108`; `ConjurSecretProvider.scala:66` | `logQuery`, `sanitized` | SHA-256 query hash; `password=***` URL masking; vault logs name object/variable only | No plaintext credential logging found anywhere in main code. |
| 17 | ⚠️ Secret-into-SQL diagnostic edge | `QueryModel.scala:72-73`; `JdbcSource.scala:243-244` | `QueryParameterDef.resolve` (`SECRET_PROVIDER` source); `logQuery` | `diagnostics.log_sql = true` | A query parameter sourced from `SECRET_PROVIDER` is rendered into the SQL text; the opt-in `log_sql` diagnostic would print it. |
| 18 | No hardcoded credentials | sweep of `ingestion-*/src/main` | — | regex `password\s*=\s*"..."` | Only constant *names* (`"SQL_PASSWORD"`). Inline values exist only in tests and the `inline` provider (flagged dev-only by a warning). |

Ruled out from code alone: WebSphere/JNDI DataSources (no usage anywhere in
the codebase), command-line credential arguments (`Cli.scala:19-88` defines
no credential flags), Kubernetes/Databricks secret mounts (no manifests in
the repo).

---

## C. Runtime credential flow

1. **Entry:** `spark-submit --class com.hcsc.generic.ingest.app.IngestMain`
   (`scripts/run_health_sherpa.sh`) → `IngestMain.main` parses CLI
   (`--entity`, `--mode`, optional `--conf-path`) — no credential flags exist
   (`Cli.scala:19-88`).
2. **Config load** (`IngestMain.scala:24-29`): `--conf-path` file, else
   `ConfigFactory.load()` (honors `-Dconfig.file`, else classpath
   `application.conf`). Feed block = `feeds.<entity>`.
3. **Pipeline → source** (`IngestPipeline.scala:222-229`): injects
   `entity`/`run_id` into the source config, resolves `source.type`
   (`"jdbc"`) from `SourceRegistry`, calls `JdbcSource.read`.
4. **Parse + auth resolution** (`JdbcSource.scala:44` →
   `JdbcSourceConfig.parse:98` → `resolveAuth:251`): `auth.type` (default
   `SQL_PASSWORD`) selects the provider; `SqlPasswordAuth.resolve`
   (`JdbcAuthenticationProvider.scala:54-63`) resolves `auth.user` and
   `auth.password` via `SecretProviders.resolveAt`.
5. **Secret retrieval** (`SecretProvider.scala:83-109`): bare string →
   inline (warned); object → named provider. **Only here would
   CyberArk/Conjur execute** — `CyberArkSecretProvider.fetch` (HTTPS GET to
   CCP, `CyberArkSecretProvider.scala:107-128`) or
   `ConjurClient.authenticate` + `getSecret` (`ConjurClient.scala:96-160`).
   The result lands in `cfg.user`/`cfg.password` in driver memory; blank
   secrets are rejected (`JDBC_002`).
6. **Connections:** health check (`JdbcHealthCheck.check:22`) → driver
   queries for watermark/bounds (`DriverQueries.firstRow:25`) → Spark reader
   (`JdbcSource.buildReader:117-137`), where user/password become JDBC
   options serialized to executors, which open the actual extraction
   connections. Optional executor probe (`JdbcHealthCheck.executorProbe:59-71`).
7. **As shipped**, step 3 never reaches `JdbcSource`: the only active feed is
   the file feed, so steps 4-6 do not execute with any configuration present
   in this repository.

---

## D. Missing information

To reach a definitive conclusion about production, the following external
artifacts are required:

1. **The production feed configuration** — the actual file passed via
   `--conf-path` or `-Dconfig.file` at deploy time. This single artifact
   determines `auth.type` and the password `provider`.
2. **Deploy-time environment** — values/presence of referenced environment
   variables and system properties; CyberArk CCP reachability and
   `AppID/Safe/Object` grants; Conjur account/host identity and how the host
   API key is injected.
3. **Scheduler/CI wrappers** — no Control-M, Jenkins, Kubernetes or
   Databricks artifacts exist in this repo; only
   `scripts/run_health_sherpa.sh`. Any real spark-submit wrapper for a JDBC
   feed is external.
4. **JDBC driver provisioning** — `mssql-jdbc` is test-scope only; how the
   production driver jar reaches the cluster (`--jars`, cluster classpath)
   is undocumented here.
5. **JVM TLS keystore settings** for CCP client-certificate authentication
   (`spark.driver.extraJavaOptions`), referenced in
   `CyberArkSecretProvider.scala:44-47` but configured externally.

---

## E. Recommended verification

Run against the *real* deployed configuration file (`$CONF`). None of these
print usernames, passwords, API keys or secret values.

```bash
# 1. Is there an active JDBC feed at all?
grep -vE '^\s*#' "$CONF" | grep -n 'type\s*=\s*"jdbc"'

# 2. Which auth mechanism and secret providers does it use? (names only)
grep -vE '^\s*#' "$CONF" | grep -nE '(auth\s*\{|type\s*=|provider\s*=)'

# 3. Confirm at runtime with a no-write validation run; the health check
#    performs full credential resolution and a real connection without
#    ingesting any data:
spark-submit --class com.hcsc.generic.ingest.app.IngestMain <jar> \
  --entity <jdbc_entity> --mode FULL --conf-path "$CONF" --validate-only
```

Then check the driver logs for these value-free markers:

| Log line | Source | Meaning |
|----------|--------|---------|
| `[CyberArk] Fetching credential https://...Object=...` | `CyberArkSecretProvider.scala:108` | CyberArk CCP **is** in the credential path |
| `[Conjur] Fetching variable '<id>' (account=..., https://...)` | `ConjurSecretProvider.scala:66` | Conjur **is** in the credential path |
| `[SecretProviders] ... inline plaintext secret` | `SecretProvider.scala:100-102` | Credential is inline in the config file |
| `[JdbcSource] Connection healthy (sqlserver, jdbc:...password=***)` | `JdbcHealthCheck.scala:28` | Connection succeeded; URL masked |

Presence/absence of the vault lines is the definitive answer.

Safe temporary debug (logs mechanism and provider *names* only), inserted in
`JdbcSourceConfig.resolveAuth` after line 259:

```scala
logger.info(s"[JdbcAuth] entity auth resolved: type=${provider.authenticationType} " +
  s"userProviderRef=${auth.exists(_.hasPath("user.provider"))} " +
  s"passwordProvider=${auth.flatMap(a => ConfigUtils.optString(a, "password.provider")).getOrElse("<bare/none>")}")
```

---

## Follow-up hardening candidates

From evidence rows 15 and 17 (not yet implemented):

1. Reject `trustServerCertificate=true` in `source.connection_properties`
   for the sqlserver dialect unless explicitly approved by a dedicated
   opt-in flag, so feed configuration cannot silently weaken the dialect's
   TLS defaults.
2. Suppress (or refuse) `diagnostics.log_sql = true` when any query
   parameter uses the `SECRET_PROVIDER` source, so a secret rendered into
   the SQL text can never reach the logs.
