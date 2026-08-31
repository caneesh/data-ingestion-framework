# Documentation

## Start here — four documents cover almost everything

| Read this | When |
|---|---|
| [operations/OPERATIONS_RUNBOOK.md](operations/OPERATIONS_RUNBOOK.md) | **operating**: error catalog, recovery, watermarks, locks, Control-M design and build guide |
| [operations/DEPLOYMENT.md](operations/DEPLOYMENT.md) | **installing**: build, site layout, spark-submit, secret providers, promotion checklists |
| [architecture/CONFIGURATION_MODEL.md](architecture/CONFIGURATION_MODEL.md) | **configuring**: the full feed-config surface, every `CFG_*`/`CUR_*` rule, overrides, reconcile |
| [development/DEVELOPER_GUIDE.md](development/DEVELOPER_GUIDE.md) | **onboarding a feed**: schema contracts, sources, RAW/CURATED, local runs |

Plus the worked example: [examples/smartiq_pdp/](examples/smartiq_pdp/) — a
production feed's configs, DDLs, test plan and promotion procedure, laid
out exactly as deployed ([its README](examples/smartiq_pdp/README.md) maps
repo → server).

Everything below is reference for specific situations — nothing in it is
required to install, configure, run or debug the framework day to day.

## Deep dives (architecture/)

Read when the specific subsystem matters to what you're doing:

| Document | Covers |
|---|---|
| [ARCHITECTURE.md](architecture/ARCHITECTURE.md) | module map, data flow, extension points |
| [CURATED_PARTITIONING.md](architecture/CURATED_PARTITIONING.md) | curated write strategies, PARTITION_OVERWRITE |
| [DECOUPLING_DESIGN.md](architecture/DECOUPLING_DESIGN.md) | independent raw/curated jobs, batch checkpointing, replay selectors |
| [INPUT_MODES.md](architecture/INPUT_MODES.md) | batch/Kafka/CDC into the shared curated writer, honest limits |
| [CONTROL_PLANE_DESIGN.md](architecture/CONTROL_PLANE_DESIGN.md) | **future work, partially prototyped** — embedding the framework in an application |

## Tooling (development/)

| Document | Covers |
|---|---|
| [CONFIG_GENERATOR.md](development/CONFIG_GENERATOR.md) | the feed-configuration wizard: what it produces and how to run it |

## History (reports/) — point in time, not living documentation

**True when written; the code has moved since.** Useful for tracing *why*
something is the way it is — never needed to operate. Where a report and a
living document disagree, the living document wins; where either disagrees
with the code, the code wins.

| Report | Records |
|---|---|
| [REQUIREMENTS_GAP_ANALYSIS.md](reports/REQUIREMENTS_GAP_ANALYSIS.md) | gaps found against the SQL Server raw/curated requirements |
| [REQUIREMENTS_COMPLIANCE_STATUS.md](reports/REQUIREMENTS_COMPLIANCE_STATUS.md) | compliance position at review time |
| [REMEDIATION_PLAN.md](reports/REMEDIATION_PLAN.md) | the plan that closed those gaps |
| [SQL_SERVER_RAW_CURATED_IMPLEMENTATION_PLAN.md](reports/SQL_SERVER_RAW_CURATED_IMPLEMENTATION_PLAN.md) | implementation sequence |
| [SQL_SERVER_RAW_CURATED_ACCEPTANCE_REPORT.md](reports/SQL_SERVER_RAW_CURATED_ACCEPTANCE_REPORT.md) | acceptance traceability |
| [SQL_SERVER_AUTH_AUDIT.md](reports/SQL_SERVER_AUTH_AUDIT.md) | the credential-chain security audit (line citations age with the code) |
| [INGESTION_PATTERN_GAP_ANALYSIS.md](reports/INGESTION_PATTERN_GAP_ANALYSIS.md) | which extraction/curated strategies exist, are partial, or are missing |
| [PERFORMANCE_REVIEW.md](reports/PERFORMANCE_REVIEW.md) | static performance analysis; phase 1 implemented |
| [PRODUCT_ROADMAP.md](reports/PRODUCT_ROADMAP.md) | candidate future work at time of writing |
| [CONTROL_PLANE_IMPLEMENTATION_PLAN.md](reports/CONTROL_PLANE_IMPLEMENTATION_PLAN.md) | the phased plan behind CONTROL_PLANE_DESIGN.md |
