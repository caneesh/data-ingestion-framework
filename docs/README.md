# Documentation

Four folders, split by **who needs the document and when** — not by subject.
If you are unsure where something belongs, the test is which of these
questions it answers.

| Folder | Answers | Read it when |
|---|---|---|
| [operations/](operations/) | *How do I run this in production?* | deploying, scheduling, or something failed at 3am |
| [development/](development/) | *How do I build on it or onboard a feed?* | writing a feed config, extending the framework |
| [architecture/](architecture/) | *What does it do and how is it put together?* | evaluating capability, reviewing a design decision |
| [reports/](reports/) | *What did we find, decide, or promise?* | tracing why something is the way it is |
| [examples/](examples/) | working feed configurations, DDL and test plans | onboarding a pipeline like an existing one |

---

## operations/ — required to deploy and run

The minimum set for taking this to production.

| Document | Covers |
|---|---|
| [DEPLOYMENT.md](operations/DEPLOYMENT.md) | build and assembly, `spark-submit`, where configuration lives, control-table placement, JDBC drivers, secret providers, pre-production checklist |
| [OPERATIONS_RUNBOOK.md](operations/OPERATIONS_RUNBOOK.md) | error catalog, watermark and quarantine recovery, restart/resume, Control-M recipes |
| [SQL_SERVER_AUTH_AUDIT.md](operations/SQL_SERVER_AUTH_AUDIT.md) | credential resolution chain, security posture, verification commands |

## development/ — building on the framework

| Document | Covers |
|---|---|
| [DEVELOPER_GUIDE.md](development/DEVELOPER_GUIDE.md) | the main guide: build, feed definition, schema contracts, sources, RAW/CURATED, validation, local runs |
| [CONFIG_GENERATOR.md](development/CONFIG_GENERATOR.md) | the feed-configuration wizard and what it produces |
| [RUNNING_CONFIG_GENERATOR.md](development/RUNNING_CONFIG_GENERATOR.md) | running the generator step by step |

## architecture/ — capability and design

| Document | Covers |
|---|---|
| [ARCHITECTURE.md](architecture/ARCHITECTURE.md) | module map, data flow, extension points, enforced invariants |
| [CONFIGURATION_MODEL.md](architecture/CONFIGURATION_MODEL.md) | the full feed-config surface and the incompatibility matrix (`CFG_*`) |
| [CURATED_PARTITIONING.md](architecture/CURATED_PARTITIONING.md) | curated write strategies and partition handling |
| [DECOUPLING_DESIGN.md](architecture/DECOUPLING_DESIGN.md) | independent raw and curated jobs, batch checkpointing |
| [INPUT_MODES.md](architecture/INPUT_MODES.md) | batch, streaming and CDC into the shared curated writer |

## reports/ — point in time, not living documentation

**These were true when written and the code has moved since.** They record
what was assessed, decided or planned — useful for tracing *why*, not for
learning *what is*. Where a report and a document above disagree, the
document above wins; where either disagrees with the code, the code wins.

| Document | Records |
|---|---|
| [REQUIREMENTS_GAP_ANALYSIS.md](reports/REQUIREMENTS_GAP_ANALYSIS.md) | gaps found against the SQL Server raw/curated requirements |
| [REQUIREMENTS_COMPLIANCE_STATUS.md](reports/REQUIREMENTS_COMPLIANCE_STATUS.md) | compliance position at the time of review |
| [REMEDIATION_PLAN.md](reports/REMEDIATION_PLAN.md) | the plan that closed those gaps |
| [SQL_SERVER_RAW_CURATED_IMPLEMENTATION_PLAN.md](reports/SQL_SERVER_RAW_CURATED_IMPLEMENTATION_PLAN.md) | implementation sequence |
| [SQL_SERVER_RAW_CURATED_ACCEPTANCE_REPORT.md](reports/SQL_SERVER_RAW_CURATED_ACCEPTANCE_REPORT.md) | acceptance traceability |
| [INGESTION_PATTERN_GAP_ANALYSIS.md](reports/INGESTION_PATTERN_GAP_ANALYSIS.md) | which extraction/curated strategies exist, are partial, or are missing (SCD2) |
| [PERFORMANCE_REVIEW.md](reports/PERFORMANCE_REVIEW.md) | static performance analysis; phase 1 items are implemented, later phases are not |
| [PRODUCT_ROADMAP.md](reports/PRODUCT_ROADMAP.md) | positioning and planned direction |

---

## Where to start

- **Deploying it:** [DEPLOYMENT.md](operations/DEPLOYMENT.md), then
  [OPERATIONS_RUNBOOK.md](operations/OPERATIONS_RUNBOOK.md).
- **Onboarding a feed:** [DEVELOPER_GUIDE.md](development/DEVELOPER_GUIDE.md) and a
  worked example under [examples/](examples/).
- **Evaluating it:** [ARCHITECTURE.md](architecture/ARCHITECTURE.md) and
  [CONFIGURATION_MODEL.md](architecture/CONFIGURATION_MODEL.md).
- **Something is broken:** [OPERATIONS_RUNBOOK.md](operations/OPERATIONS_RUNBOOK.md),
  and the troubleshooting section of the relevant example.
