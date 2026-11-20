# Redshift table archive utility

Archive one table, a comma-separated list, or every regular local table in a schema through an existing StrongDM resource. The script never deletes, renames, or truncates source tables. Views, materialized views, and external tables are not selected.

## Install

Python 3.10+ and the StrongDM CLI are required.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```



## Preview the selection

### Sign in AWS with sdm name which has write acess

```
sdm connect aws-is-dev-us-east-1-write
```

```bash
sdm aws --name "aws-is-dev-us-east-1-write" run python archive_redshift_tables.py \
  --sdm-name redshift-is-dev-admin \
  --database dev \
  --schema bronze \
  --tables br_labor_master \
  --s3-prefix <REPLACE_WITH_S3_BUCKET_PATH> \
  --iam-role <REPLACE_WITH_ROLE> \
  --dry-run
```
```
Select S3 path consists of PRFIX ex s3://BUCKET_NAME/SOME_PREFIX_NAME
```

Dry run connects to StrongDM and reads catalog metadata. It does not run UNLOAD, scan/count table data, or write to S3. It prints destinations for a preview run ID; the real run gets a different ID.

## Archive tables

Run the same command without `--dry-run` to archive.

- One table: `--tables br_labor_master`
- Multiple tables: `--tables br_labor_master,br_other_table`
- Entire schema: omit `--tables` or pass `--tables ""`.
- For an identifier containing a comma, CSV-quote it inside the argument: `--tables '"table,one",table_two'`.
- Names are case-sensitive identifier values. Do not supply `schema.table`; specify the schema with `--schema`.
- StrongDM resource names and endpoints are discovered using `sdm status`; disconnected resources are connected automatically. The installed CLI must expose the usual DATASOURCE / STATUS / ADDRESS / TYPE columns. No connection is automatically disconnected afterward.
- The local database user defaults to `sdm`; override with `--db-user`. If needed, set `REDSHIFT_PASSWORD` through your normal secret handling, never as a CLI argument.

## S3 layout

```text
s3://BUCKET/PREFIX/DATABASE/SCHEMA/TABLE/UTC_RUN_ID/
  ddl.sql
  metadata.json
  restore.sql
  data/
    manifest
    <Parquet files>
```

The run ID includes UTC time and a random suffix. Names are percent-encoded in object keys. Each run uses a new prefix. No ALLOWOVERWRITE or CLEANPATH is used. A conditional initial metadata write prevents reusing an existing archive location.

`metadata.json` progresses from `in_progress` to `completed` or `failed`. Only `completed` archives passed the checks. Partial files are retained on failure for diagnosis. An interrupt can leave `in_progress` metadata; it must not be treated as a completed archive.

## What is verified

For each table, the script:

1. Verifies the current table ID against the discovered table.
2. Saves SHOW TABLE output and its SHA-256 digest.
3. Saves ownership and the visible grants from `information_schema.table_privileges`.
4. Runs an exact COUNT and UNLOAD inside the same explicit transaction, using the database's configured snapshot/serializable isolation. The COUNT is an additional full-table scan. A socket/statement timeout can be raised for large tables.
5. Checks for DDL changes and commits the export transaction.
6. Reads the verbose manifest, compares its per-file record counts with the source count, and verifies every listed object's location and content length using S3 HEAD requests.
7. Writes restore instructions and a completed metadata record.

A failed table is reported and subsequent tables are attempted. Exit code 0 means all selected tables completed (or a successful dry run); 1 means a failure; 130 means interrupted. A local JSON run summary is written after normal archive processing. Use `--summary-file PATH` to choose its location.

Empty tables still receive DDL, metadata, and restore instructions. A zero-row manifest with no files is valid for an empty table. Recreate its DDL without COPY if there are no rows to restore.

## Permissions and limitations

- Redshift needs SELECT on source tables and authorization to use the supplied IAM role for UNLOAD. That role needs S3 permissions for the export. It is separate from the Python process's AWS identity.
- The Python AWS identity needs S3 PutObject/GetObject/HeadObject access for the archive. Encrypted buckets may also need the relevant KMS permissions. Bucket policies and default encryption remain in effect; the script does not change them.
- For Parquet UNLOAD, keep the Redshift cluster and S3 bucket in the same AWS region. `--aws-region` configures boto3, not cross-region Redshift export.
- Grants visible to the connected user are saved for reference, not automatically reconstructed. Role membership, default/schema permissions, masking/RLS policies, dependencies, and every aspect of external governance are not captured. SHOW TABLE is the table definition, not a full database backup.
- All-table discovery uses the catalog visible at the beginning of the run. Tables created afterward are not included. Snapshot consistency is per table, not across an entire schema. Coordinate concurrent DDL and ongoing ingestion for archival/deletion workflows.
- Row counts and file sizes are checked, not full row-by-row content checksums. Perform a restore test, business validation, and ownership/dependency review before a separate deletion process. This utility never declares a table safe to delete.
- Date ranges and business totals require table-specific validation; add those checks to your review process.
- The destination in the example is a sample. Use an approved retained archive prefix and appropriate S3 lifecycle/retention settings for real archival.
- This script is not an unused-table detector. It exports exactly the selected/discovered tables regardless of usage.

## Restore

Review and run `ddl.sql`. For a restore test, edit the destination table/schema in both DDL and `restore.sql`. Then run `restore.sql`, which uses the exact `data/manifest` object and `FORMAT AS PARQUET MANIFEST`. Validate source row counts, business metrics, ownership, permissions, and downstream access separately.

## AWS references

- [UNLOAD and verbose manifests](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html)
- [SHOW TABLE](https://docs.aws.amazon.com/redshift/latest/dg/r_SHOW_TABLE.html)
- [Transaction isolation](https://docs.aws.amazon.com/redshift/latest/dg/c_serial_isolation.html)

## Restore archived tables

The S3 root must be the prefix **before** `DATABASE/SCHEMA/TABLE/`. This version reads the flat table archive layout with `ddl.sql`, `metadata.json`, and `data/manifest` directly under each table folder; it does not discover older timestamp/run-ID subfolders.

Preview restoring one table into a test schema:

```bash
sdm connect aws-is-dev-us-east-1-write-cli
sdm aws --name aws-is-dev-us-east-1-write-cli run python restore_redshift_tables.py \
  --sdm-name redshift-is-dev-admin \
  --database dev \
  --schema bronze \
  --table br_labor_master \
  --s3-prefix <REPLACE_WITH_S3_BUCKET_PATH> \
  --iam-role <REPLACE_WITH_ROLE> \
  --test --dry-run
```

- Remove `--dry-run` to perform the restore. Dry run reads and validates S3 artifacts, connects to the database and checks for existing destination objects; it creates no schemas/tables and runs no COPY. It writes no local summary unless `--summary-file` is explicitly supplied.
- Omit `--table` to process every immediate table archive folder in the selected schema, with pagination.
- `--test` uses a persistent schema named `<schema>_tmp`, such as `bronze_tmp`, keeping original table names. It is not a session-local temporary schema and remains available for validation afterward.
- Omit `--test` to restore to the original schema. Existing destination objects are errors, never overwritten, dropped, truncated, or appended to.
- Each archive must have completed metadata, a matching DDL checksum, and a valid verbose manifest. Manifest row counts must match the archived source/export counts, and each listed object's size/location is checked.
- The script changes only the CREATE TABLE destination in the saved DDL and removes `IF NOT EXISTS`. DDL names must match the selected archive; multiple SQL statements are rejected. Other DDL content, including foreign-key references and defaults, stays as archived. Referenced tables/schemas may need to exist before restoration; this is not a dependency-ordered database restore.
- For each table, CREATE SCHEMA (if needed), CREATE TABLE, COPY, and COUNT run in one explicit transaction. COUNT must equal the manifest total before COMMIT. A failed table is rolled back and remaining tables continue. Empty archives without data files create only the table and verify zero rows.
- If connection is lost during COMMIT, outcome can be uncertain: inspect the destination before retrying. A completed table is not rolled back because another table fails.
- By default, a local `redshift_restore_<timestamp>.json` records results, expected/restored counts, and errors. Use `--summary-file PATH` to choose its path. Exit codes: 0 all succeeded/ready, 1 any failure, 130 interrupted.
- Restored ownership is the connected user; saved ownership and permissions are not automatically reapplied. Row count agreement does not replace business validation.
- Python needs S3 list/read access (and any required KMS decrypt access). Redshift needs CREATE/USAGE privileges for the destination and authority to use the COPY IAM role. The role needs access to the archived data. The bucket and destination Redshift cluster must meet Parquet COPY region requirements.

Restoration has been verified with mocked S3/Redshift tests only; no live restore was executed while developing the tool.
