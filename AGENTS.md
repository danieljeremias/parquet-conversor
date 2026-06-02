# AGENTS.md

## Purpose
- This is a Quarkus CLI migrator that rewrites Parquet files in S3 to fix schema compatibility for `transaction_id`.
- Core behavior: convert Avro field `transaction_id` from `int` to nullable `string` while preserving other fields.

## Big Picture Architecture
- Entry point is `src/main/java/com/tasy/finops/parquet/conversor/Application.java` (`@QuarkusMain`): starts Quarkus, runs one migration pass, then exits via `Quarkus.asyncExit()`.
- Orchestration is in `src/main/java/com/tasy/finops/parquet/conversor/S3ParquetMigrator.java`:
  - reads `source.bucket` and `source.prefix`
  - lists objects with `ListObjectsV2`
  - filters keys ending with `.parquet`
  - calls `ParquetTransformer.transform(bucket, key)` per object.
- Transformation is in `src/main/java/com/tasy/finops/parquet/conversor/ParquetTransformer.java`:
  - downloads S3 object to temp file
  - reads first record to obtain schema
  - rebuilds schema by string replacement for `transaction_id`
  - rewrites all records to a new Parquet file
  - uploads transformed file to `usage-fixed/` path.
- S3 access is centralized in `src/main/java/com/tasy/finops/parquet/conversor/S3Helper.java` (singleton `S3Client.create()`).

## Data/Schema Rules You Must Preserve
- `transaction_id` conversion is implemented in two places and both matter:
  - `rebuildSchema(...)`: replaces `"type":"int"` with `["null","string"]` for that exact field snippet.
  - `processRecord(...)`: converts non-null `transaction_id` values with `toString()` before writing.
- Output key is currently derived with `key.replace("usage/", "usage-fixed/")` (hardcoded prefixes), not `target.prefix` config.

## Configuration and Runtime Assumptions
- Config lives in `src/main/resources/application.properties`:
  - `source.bucket`, `source.prefix`, `target.bucket`, `target.prefix`.
- AWS credentials/region are expected from the standard AWS SDK v2 environment/provider chain.
- The same bucket is used for source/target in default config (`emr-finops-metrics-db`).

## Developer Workflows (Verified from repo files)
- Dev mode: `./gradlew quarkusDev`
- Build jar: `./gradlew build`
- Run packaged app: `java -jar build/quarkus-app/quarkus-run.jar`
- Native build: `./gradlew build -Dquarkus.native.enabled=true`

## Project-Specific Conventions
- Uses CDI (`@ApplicationScoped`, `@Inject`) instead of manual wiring.
- Fail-fast style: `transform(...)` wraps checked exceptions and throws `RuntimeException`.
- Logging is currently `System.out.println("Processing: ...")`, not a logging facade.
- Schema mutation is intentionally minimal and string-based; avoid broad schema rewrites unless required.

## Dependencies and Integration Points
- Quarkus 3.x + Java 21 (`build.gradle`).
- Libraries: AWS S3 SDK (via `quarkus-amazon-s3`), Avro `1.12.1`, Parquet Avro `1.17.1`.
- No tests currently exist in `src/test` (directory is empty); changes should be validated with targeted local runs against representative Parquet samples.

## Existing AI Guidance Files
- Repository scan found no existing `.github/copilot-instructions.md`, `AGENT.md`, `AGENTS.md`, `CLAUDE.md`, cursor/windsurf/cline rule files.

