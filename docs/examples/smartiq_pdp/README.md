# SmartIQ PDP example — layout

Mirrors the deployed site layout so the mapping from repo to server is
one-to-one:

| Repo | Deploys to |
|---|---|
| `params/` — feed, schema, override example | `<base>/params/membership/smartiq_pdp/` |
| `ddl/` — table DDLs + the one-time rename migration | run once via beeline; not deployed |
| `lower-env/params/`, `lower-env/ddl/` | the e2e twin of the above |
| `CONTROL_PLANE_ONBOARDING.md`, `PROD_PROMOTION.md` | procedures, not artifacts |

**Scripts are deliberately NOT here.** The launchers live in `/scripts`
at the repo root and deploy to `<base>/src/scripts/membership/smartiq_pdp/`,
because they are shared framework machinery, not feed artifacts:
`run_smartiq.sh` requires `run_ingest.sh` and `ingest_submit_common.sh`
beside it (it refuses to start otherwise), the same siblings serve every
other feed's launcher, and `build_bundle.sh` ships the whole set. A copy
here would be a second version to drift.

`smartiq.env` is created on the server from `/scripts/smartiq.env.example`
and never lives in git — it holds site values.
