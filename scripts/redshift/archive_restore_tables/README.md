# Redshift table archive/Restore utility

Archive one table, a comma-separated list, or every regular local table in a schema through an existing StrongDM resource. The script never deletes, renames, or truncates source tables. Views, materialized views, and external tables are not selected.

## Install

Python 3.10+ and the StrongDM CLI are required.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

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
- StrongDM resource names and endpoints are discovered using `sdm status`; disconnected resources are connected automatically. The installed CLI must expose the usual DATASOURCE / STATUS / ADDRESS / TYPE columns. No connection is automatically disconnected afterward.
- The local database user defaults to `sdm`; override with `--db-user`. If needed, set `REDSHIFT_PASSWORD` through your normal secret handling, never as a CLI argument.

## S3 layout

```text
s3://BUCKET/PREFIX/DATABASE/SCHEMA/TABLE/
  ddl.sql
  metadata.json
  data/
    manifest
    <Parquet files>
```
`metadata.json` Created by our Python script. Records the source table, owner, permissions, archive status, row counts, and DDL checksum.

`data/manifest`	Created by Redshift UNLOAD. Lists the exported Parquet files, their sizes, and row counts. COPY uses it to locate the files to restore.

`ddl.sql`	Contains the CREATE TABLE statement used to recreate the table structure.

## What is verified

For each table, the script:

1. Saves SHOW TABLE output and its SHA-256 digest.
2. Saves ownership, legacy visible grants, and a typed explicit-table grant snapshot from `svv_relation_privileges`, including users/groups/roles/PUBLIC and grant options. A superuser connection is required for complete grant visibility.
3. Runs an exact COUNT and UNLOAD inside the same explicit transaction, using the database's configured snapshot/serializable isolation.
4. Reads the verbose manifest, compares its per-file record counts with the source count, and verifies every listed object's location and content length using S3 HEAD requests.
5. Writes a completed metadata record; no restore file is generated.

A failed table is reported and subsequent tables are attempted. Exit code 0 means all selected tables completed (or a successful dry run); 1 means a failure; 130 means interrupted. A local JSON run summary is written after normal archive processing. Use `--summary-file PATH` to choose its location.

Empty tables still receive DDL and metadata. A zero-row manifest with no files is valid for an empty table. Recreate its DDL without COPY if there are no rows to restore.

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

- Remove `--dry-run` to perform the restore. Dry run reads and validates S3 artifacts only; it never connects to Redshift or executes SQL and does not check destination existence. It writes no local summary unless `--summary-file` is explicitly supplied.
- Omit `--table` to process table from archive folder in the selected schema.
- `--test` uses a persistent schema named `<schema>_tmp`, such as `bronze_tmp`, keeping original table names. It is not a session-local temporary schema and remains available for validation afterward.
- Omit `--test` to restore to the original schema, it for Testing, to create _tmp schema
- Each archive must have completed metadata, a matching DDL checksum, and a valid verbose manifest. Manifest row counts must match the archived source/export counts, and each listed object's size/location is checked.
- The script changes only the CREATE TABLE destination in the saved DDL and removes `IF NOT EXISTS`. DDL names must match the selected archive; multiple SQL statements are rejected. Other DDL content, including foreign-key references and defaults, stays as archived. Referenced tables/schemas may need to exist before restoration; this is not a dependency-ordered database restore.
- For each table, CREATE SCHEMA (if needed), CREATE TABLE, COPY, and COUNT run in one explicit transaction. COUNT must equal the manifest total before COMMIT. A failed table is rolled back and remaining tables continue. Empty archives without data files create only the table and verify zero rows.
- If connection is lost during COMMIT, outcome can be uncertain: inspect the destination before retrying. A completed table is not rolled back because another table fails.
- By default, a local `redshift_restore_<timestamp>.json` records results, expected/restored counts, and errors. Use `--summary-file PATH` to choose its path. Exit codes: 0 all succeeded/ready, 1 any failure, 130 interrupted.
- The original table owner saved in metadata.json is restored and verified before COMMIT. Supported explicit table grants saved in the typed snapshot are reapplied and verified before COMMIT. Row count agreement does not replace business validation.

### Ownership restoration and grant inspection

Restore now requires a nonempty `owner` in the archive metadata and checks that the user exists before creating the destination table. After COPY and row-count validation, it runs `ALTER TABLE ... OWNER TO ...`, verifies the owner in the catalog, and only then commits. The same behavior applies with `--test`; only the table owner is transferred, not schema ownership. Missing owners, insufficient ownership-transfer privileges, or verification mismatches fail and roll back that table. The output records `expected_owner` and, after verification, `restored_owner`. Dry run reports `planned_owner` without connecting to Redshift.

Run `check_restore_permissions.sql` in the destination database to compare source/restored owners, explicit table grants (with identity type and grant option), and schema/scoped grants. Adjust the schema and table filters. Use an administrative account for full visibility. Supported explicit table grants are replayed; inherited roles, column grants, schema/scoped grants, default permissions, and policies need separate review. Existing-table protection still applies, so this change does not update owners of tables that were already restored.

### Typed table grants and older archives

Both scripts now require a Redshift superuser connection to capture or verify a complete view of explicit table permissions. New archives use metadata format version 2 with a `permissions` snapshot: recipient name, recipient type (`user`, `group`, `role`, or `public`), privilege, and grant option. SELECT, INSERT, UPDATE, DELETE, REFERENCES, DROP, ALTER, and TRUNCATE are replayed. RULE/TRIGGER catalog entries are saved for reference but not replayed as unsupported GRANT keywords.

After count validation, restore applies the supported grants, restores ownership, verifies owner and expected grants, and commits. Missing recipients, failed GRANT commands, or missing expected grants roll back that table; other tables continue. Extra destination/default grants are retained and reported in `extra_table_grants`. This validates that expected grants are present; it does not assert that destination access is exactly identical. Original grantor attribution is not reproduced. Schema USAGE is neither granted nor changed automatically; check `check_restore_permissions.sql` separately. No roles/users/groups or their memberships are created.

Older archives without a typed snapshot fail validation by default rather than silently restoring incomplete permissions. If the original table still exists, append `--grants-from-source` to the restore command. This explicitly uses the **current** original table grants, not grants reconstructed from the historical archive; `permissions_source` records `live_source_table`. It reads the original table's permissions only. The original table must be in the connected database under `--schema`; archived ownership still comes from metadata. This flag cannot be combined with `--dry-run`, which remains strictly S3-only.

Example addition for an older archive: `--test --grants-from-source`. New archives require neither a flag nor the original table to restore grants. Existing destination tables still stop the job before restoration begins: this update does not modify grants on tables already restored. For another test restore, use a fresh destination after separately reviewing existing test objects; the script never removes them.
