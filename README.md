# Data Ingestion Framework

A configuration-driven, multi-source data ingestion framework built with Scala 2.11 and Spark 2.3.

## Architecture

This is a multi-module Maven project designed for extensibility:

```
data-ingestion-framework/
├── ingestion-core/      # Core abstractions, utilities, and shared components
├── ingestion-file/      # File-based source connector (CSV, JSON, Parquet, etc.)
├── ingestion-jdbc/      # JDBC source connector (SQL Server, DB2, Oracle, etc.)
├── ingestion-kafka/     # Kafka streaming source connector
└── ingestion-app/       # Main application entry point
```

### Modules

| Module | Description |
|--------|-------------|
| `ingestion-core` | Source/Sink traits, config utilities, partitioning, transforms, stage runners |
| `ingestion-file` | File source: CSV, JSON, Parquet with header handling, aliases, trailer removal |
| `ingestion-jdbc` | JDBC source: SQL Server, DB2, Oracle, PostgreSQL with partitioned reads |
| `ingestion-kafka` | Kafka source: batch reads with JSON/string parsing |
| `ingestion-app` | Application entry point bundling all connectors |

## Features

### Source Connectors
- **File**: CSV, JSON, Parquet, ORC with configurable delimiters, headers, multiline
- **JDBC**: Any JDBC-compliant database with parallel partition reads
- **Kafka**: Batch consumption with JSON/string value parsing

### Core Capabilities
- Configuration-driven pipeline definition (Typesafe Config / HOCON)
- Header alias mapping for vendor file format changes
- Positional column assignment for headerless or unstable files
- Trailer row removal by marker text or position
- RAW metadata enrichment: `source_file`, `row_idx`, `load_timestamp`, `file_type`, `file_id`
- Dynamic partition column derivation
- Curated layer with type casting, derived columns, and audit fields
- FULL overwrite and INCR upsert-union-dedup merge strategies

## Configuration

All pipelines are defined in `application.conf`:

```hocon
feeds {
  my_feed {
    source {
      type = "file"          # file | jdbc | kafka
      path = "hdfs:///data/input/*.csv"
      # ... source-specific options
    }
    
    raw {
      database = "raw_db"
      table = "my_table"
      path = "hdfs:///warehouse/raw/my_table"
      partitioning {
        keys = ["ingest_dt"]
        derive { ingest_dt = "date_format(current_timestamp(), 'yyyy-MM-dd')" }
      }
    }
    
    curated {
      enabled = true
      database = "curated_db"
      table = "my_table"
      merge { keys = ["business_key"] }
    }
  }
}
```

## Build

```bash
mvn clean package
```

This produces:
- `ingestion-app/target/ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar`

## Run

```bash
spark-submit \
  --class com.hcsc.generic.ingest.app.IngestMain \
  --master yarn \
  ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity my_feed \
  --mode FULL \
  --conf-path /path/to/application.conf
```

### CLI Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--entity` | Yes | Feed name from config |
| `--mode` | Yes | `FULL` or `INCR` |
| `--conf-path` | No | Path to config file (defaults to classpath) |
| `--stage` | No | `all`, `raw`, `curated` |
| `--raw-flag` | No | Override file_type partition value |
| `--resume-ingest-dt` | No | Resume curated from specific RAW partition |

## Adding a New Source Connector

1. Create a new module (e.g., `ingestion-s3`)
2. Implement the `Source` trait:

```scala
object S3Source extends Source {
  override def sourceType: String = "s3"
  
  override def read(spark: SparkSession, sourceConf: Config): DataFrame = {
    // Implementation
  }
  
  def register(): Unit = SourceRegistry.register(this)
}
```

3. Register in `IngestMain.registerSources()`
4. Add module dependency to `ingestion-app`

## Validation

Test in your target HDP/Spark environment before production deployment. Verify:
- Hive metastore connectivity
- HDFS paths and permissions
- JDBC driver availability (for JDBC sources)
- Kafka client compatibility (for Kafka sources)
