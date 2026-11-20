#!/usr/bin/env python3
"""Restore archived Redshift tables; existing destination tables are never changed."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import unquote

from archive_redshift_tables import (
    ArchiveError, archive_prefix, connect_sdm, database_connection,
    get_manifest, identifier, literal, s3_location, utc_now, verify_manifest,
    capture_table_permissions, normalize_table_grants, read_table_grants,
)

# Recognize quoted identifiers, string literals, and comments before considering
# semicolons. Never replace schema names globally inside saved DDL.
TOKEN = re.compile(r'''\s+|--[^\n]*(?:\n|$)|/\*.*?\*/|"(?:[^"]|"")*"|'(?:[^'\\]|\\.|'')*'|[A-Za-z_][A-Za-z_0-9$]*|[^\s]''', re.S)


def target_ddl(ddl, source_schema, table, target_schema):
    tokens = [(m.group(), m.start(), m.end()) for m in TOKEN.finditer(ddl)
              if not m.group().isspace() and not m.group().startswith(('--', '/*'))]
    if len(tokens) < 6 or [t[0].upper() for t in tokens[:2]] != ['CREATE', 'TABLE']:
        raise ArchiveError('DDL must contain one regular CREATE TABLE statement')
    i = 2
    if [t[0].upper() for t in tokens[i:i+3]] == ['IF', 'NOT', 'EXISTS']:
        i += 3
    def decoded(value):
        return value[1:-1].replace('""', '"') if value.startswith('"') else value.lower()
    if (len(tokens) <= i+3 or tokens[i+1][0] != '.' or tokens[i+3][0] != '('
            or decoded(tokens[i][0]) != source_schema or decoded(tokens[i+2][0]) != table):
        raise ArchiveError('DDL CREATE TABLE name does not match the selected archive')
    if any(t[0] == ';' for t in tokens[:-1]):
        raise ArchiveError('DDL must contain exactly one statement')
    # Remove IF NOT EXISTS, ensuring even a concurrent existing table causes an
    # error rather than allowing COPY to append to it.
    return ('CREATE TABLE ' + identifier(target_schema) + '.' + identifier(table)
            + ' ' + ddl[tokens[i+3][1]:])


def quote_ddl_columns(ddl):
    """Quote column declarations and column lists, without touching SQL values/types."""
    tokens = [(m.group(), m.start(), m.end()) for m in TOKEN.finditer(ddl)
              if not m.group().isspace() and not m.group().startswith(('--', '/*'))]
    edits = {}
    def quote_token(index):
        value, start, end = tokens[index]
        if re.fullmatch(r'[A-Za-z_][A-Za-z_0-9$]*', value):
            # Unquoted SQL identifiers fold to lowercase. Already quoted names
            # retain their original spelling and escaping.
            edits[start] = (end, identifier(value.lower()))
    depth = 0
    definition_start = False
    opened = False
    constraint_heads = {'CONSTRAINT', 'PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'LIKE'}
    for i, (value, _, _) in enumerate(tokens):
        if value == '(':
            if not opened:
                opened = True
                definition_start = True
            depth += 1
        elif value == ')':
            depth -= 1
        elif value == ',' and depth == 1:
            definition_start = True
        elif definition_start and depth == 1:
            if value.upper() not in constraint_heads:
                quote_token(i)
            definition_start = False
    # Quote column references in keys/constraints too. These lists contain names,
    # not type parameters; commas in DECIMAL, IDENTITY, or defaults are untouched.
    for i, (value, _, _) in enumerate(tokens[:-1]):
        if value.upper() == 'REFERENCES':
            opening = i+2
            if opening < len(tokens) and tokens[opening][0] == '.':
                opening += 2
            if opening >= len(tokens) or tokens[opening][0] != '(':
                continue
        elif value.upper() in {'KEY', 'UNIQUE', 'SORTKEY', 'DISTKEY'} and tokens[i+1][0] == '(':
            opening = i+1
        else:
            continue
        j = opening+1
        while j < len(tokens) and tokens[j][0] != ')':
            if tokens[j][0] != ',':
                quote_token(j)
            j += 1
    for start, (end, replacement) in sorted(edits.items(), reverse=True):
        ddl = ddl[:start]+replacement+ddl[end:]
    notes = ['Quoted column identifiers and key column lists for reserved-word compatibility.'] if edits else []
    return ddl, notes


def normalize_distribution(ddl):
    """Honor explicit AUTO/EVEN/ALL rather than conflicting saved DISTKEY hints."""
    tokens = [(m.group(), m.start(), m.end()) for m in TOKEN.finditer(ddl)
              if not m.group().isspace() and not m.group().startswith(('--', '/*'))]
    depths = []
    depth = 0
    for value, _, _ in tokens:
        depths.append(depth)
        if value == '(':
            depth += 1
        elif value == ')':
            depth -= 1
    styles = [tokens[i+1][0].upper() for i, t in enumerate(tokens[:-1])
              if t[0].upper() == 'DISTSTYLE' and depths[i] == 0]
    if len(styles) != 1 or styles[0] not in {'AUTO', 'EVEN', 'ALL'}:
        return ddl, []
    edits = []
    for i, (value, start, end) in enumerate(tokens):
        if value.upper() != 'DISTKEY':
            continue
        if depths[i] == 1 and i > 0 and tokens[i-1][0] not in {'(', ','}:
            # Column attribute, excluding quoted identifiers/strings and nested expressions.
            edits.append((start, end))
        elif depths[i] == 0:
            if i+3 >= len(tokens) or tokens[i+1][0] != '(' or tokens[i+3][0] != ')':
                raise ArchiveError('Unsupported table DISTKEY clause')
            edits.append((start, tokens[i+3][2]))
    for start, end in reversed(edits):
        ddl = ddl[:start]+ddl[end:]
    notes = [f'Removed explicit DISTKEY to preserve DISTSTYLE {styles[0]} from archived DDL.'] if edits else []
    return ddl, notes


def read_text(s3, bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    try:
        return response['Body'].read().decode('utf-8')
    finally:
        response['Body'].close()


def discover_archives(s3, bucket, base, database, schema, table=None):
    if table is not None:
        return [table]
    prefix = archive_prefix(base, database, schema, '')
    # archive_prefix adds the table separator; select the schema folder itself.
    prefix = prefix[:-1]
    names = set()
    for page in s3.get_paginator('list_objects_v2').paginate(
            Bucket=bucket, Prefix=prefix, Delimiter='/'):
        for item in page.get('CommonPrefixes', []):
            encoded = item['Prefix'][len(prefix):].rstrip('/')
            if encoded and '/' not in encoded:
                name = unquote(encoded)
                if archive_prefix(base, database, schema, name) != item['Prefix']:
                    raise ArchiveError('Archive folder uses an unexpected name encoding')
                names.add(name)
    return sorted(names)


def prepare_archive(args, s3, bucket, base, table):
    prefix = archive_prefix(base, args.database, args.schema, table)
    metadata = json.loads(read_text(s3, bucket, prefix+'metadata.json'))
    if metadata.get('status') != 'completed':
        raise ArchiveError('Archive metadata is not completed')
    for key, value in [('database', args.database), ('schema', args.schema), ('table', table)]:
        if metadata.get(key) != value:
            raise ArchiveError(f'Archive metadata {key} does not match the selection')
    permissions = None if args.grants_from_source else archived_permissions(metadata)
    owner = metadata.get('owner')
    if not isinstance(owner, str) or not owner.strip():
        raise ArchiveError('Archive metadata is missing the original table owner')
    identifier(owner)
    ddl = read_text(s3, bucket, prefix+'ddl.sql')
    if metadata.get('ddl_sha256') != hashlib.sha256(ddl.encode()).hexdigest():
        raise ArchiveError('DDL checksum does not match archive metadata')
    expected = metadata.get('source_row_count')
    if type(expected) is not int or expected < 0:
        raise ArchiveError('Archive source row count is missing or invalid')
    manifest = get_manifest(s3, bucket, prefix+'data/manifest')
    verified = verify_manifest(s3, bucket, prefix+'data/', manifest, expected)
    if metadata.get('exported_rows') != expected:
        raise ArchiveError('Saved exported count differs from source and manifest counts')
    target_schema = args.schema + '_tmp' if args.test else args.schema
    rewritten = target_ddl(ddl, args.schema, table, target_schema)
    rewritten, column_adjustments = quote_ddl_columns(rewritten)
    rewritten, adjustments = normalize_distribution(rewritten)
    adjustments = column_adjustments + adjustments
    return {'ddl': rewritten, 'ddl_adjustments': adjustments, 'owner': owner,
            'permissions': permissions,
            'target_schema': target_schema, 'expected_rows': expected,
            'manifest_uri': f's3://{bucket}/{prefix}data/manifest',
            'archive_uri': f's3://{bucket}/{prefix}', **verified}


def archived_permissions(metadata):
    permissions = metadata.get('permissions')
    if not isinstance(permissions, dict) or permissions.get('version') != 1:
        raise ArchiveError('Archive has no typed grant snapshot. Re-archive, or use --grants-from-source if the original table still exists.')
    if permissions.get('visibility') != 'superuser' or permissions.get('scope') != 'explicit_table_grants_only':
        raise ArchiveError('Grant snapshot does not confirm full explicit-table visibility')
    grants, ignored = normalize_table_grants(permissions.get('table_grants'))
    return {**permissions, 'table_grants': grants,
            'catalog_only_grants': permissions.get('catalog_only_grants', []) + ignored}


def grant_sql(schema, table, grant):
    # Validate all interpolated privilege/identity-type tokens against allowlists.
    validated, _ = normalize_table_grants([grant])
    if not validated:
        raise ArchiveError('Catalog-only privileges cannot be replayed')
    g = validated[0]
    recipient = 'PUBLIC' if g['identity_type'] == 'public' else (
        {'user': '', 'group': 'GROUP ', 'role': 'ROLE '}[g['identity_type']] + identifier(g['identity_name']))
    return ('GRANT '+g['privilege_type']+' ON TABLE '+identifier(schema)+'.'+identifier(table)
            +' TO '+recipient+(' WITH GRANT OPTION' if g['admin_option'] else '')+';')


def verify_table_grants(conn, schema, table, expected):
    actual, _ = read_table_grants(conn, schema, table)
    def key(g):
        return g['identity_type'], g['identity_name'], g['privilege_type']
    available = {key(g): g['admin_option'] for g in actual}
    missing = [g for g in expected if key(g) not in available or (g['admin_option'] and not available[key(g)])]
    if missing:
        raise ArchiveError('Restored table is missing archived grants: '+json.dumps(missing))
    wanted = {key(g): g['admin_option'] for g in expected}
    extra = [g for g in actual if key(g) not in wanted or (g['admin_option'] and not wanted[key(g)])]
    return extra


def database_error_fields(exc):
    # pg8000 DatabaseError carries PostgreSQL protocol fields in args[0].
    # Report only SQLSTATE and the primary message, never statement/detail/context.
    fields = exc.args[0] if exc.args and isinstance(exc.args[0], dict) else {}
    output = {}
    code = fields.get('C')
    if isinstance(code, str) and re.fullmatch(r'[A-Z0-9]{5}', code):
        output['sqlstate'] = code
    message = fields.get('M')
    if isinstance(message, str):
        for key, value in os.environ.items():
            if value and any(word in key.upper() for word in ('PASSWORD', 'SECRET', 'TOKEN', 'ACCESS_KEY')):
                message = message.replace(value, '[REDACTED]')
        message = re.sub(r'(?i)(aws_access_key_id|aws_secret_access_key|aws_session_token|password|secret_access_key|session_token)\s*[=:]\s*[^\s;]+',
                         r'\1=[REDACTED]', message)
        message = re.sub(r'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b', '[REDACTED]', message)
        message = re.sub(r'https?://[^\s<>]+', '[URL REDACTED]', message)
        output['redshift_message'] = message[:2000]
    return output


def restore_table(args, table, prepared, endpoint, connection_factory=database_connection):
    result = {'table': table, 'destination_schema': prepared['target_schema'],
              'expected_rows': prepared['expected_rows'], 'restored_rows': None,
              'archive_uri': prepared['archive_uri'], 'status': 'failed'}
    result['ddl_adjustments'] = prepared.get('ddl_adjustments', [])
    result['expected_owner'] = prepared.get('owner')
    conn = None
    committed = False
    stage = 'connect'
    try:
        conn = connection_factory(args, endpoint)
        stage = 'begin'
        conn.run('BEGIN')
        schema = prepared['target_schema']
        stage = 'check_destination'
        existing = conn.run('''SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE CAST(n.nspname AS VARCHAR(128))=:schema
            AND CAST(c.relname AS VARCHAR(128))=:table''', schema=schema, table=table)
        if existing:
            raise ArchiveError('Destination object already exists; it will not be changed')
        stage = 'check_permissions'
        # Full visibility is needed to verify grants, including extra defaults.
        admin = conn.run('SELECT usesuper FROM pg_user WHERE usename = CURRENT_USER')
        if not admin or admin[0][0] is not True:
            raise ArchiveError('A Redshift superuser connection is required to restore and verify permissions')
        if args.grants_from_source:
            source = conn.run('''SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE CAST(n.nspname AS VARCHAR(128))=:schema
                AND CAST(c.relname AS VARCHAR(128))=:table AND c.relkind='r' ''', schema=args.schema, table=table)
            if not source:
                raise ArchiveError('Original table is missing; cannot read live source grants')
            permissions = capture_table_permissions(conn, args.schema, table)
            result['permissions_source'] = 'live_source_table'
        else:
            permissions = archived_permissions({'permissions': prepared.get('permissions')})
            result['permissions_source'] = 'archive_metadata'
        grants = permissions['table_grants']
        result['catalog_only_grants_not_replayed'] = permissions.get('catalog_only_grants', [])
        stage = 'check_owner'
        owner = prepared.get('owner')
        if not isinstance(owner, str) or not owner.strip():
            raise ArchiveError('Archive metadata is missing the original table owner')
        identifier(owner)
        if not conn.run('SELECT 1 FROM pg_user WHERE CAST(usename AS VARCHAR(128))=:owner', owner=owner):
            raise ArchiveError(f'Archived owner {owner!r} does not exist or is not visible in the destination')
        qualified = identifier(schema)+'.'+identifier(table)
        stage = 'create_schema'
        conn.run('CREATE SCHEMA IF NOT EXISTS '+identifier(schema))
        stage = 'create_table'
        conn.run(prepared['ddl'])
        if prepared['data_files']:
            stage = 'copy'
            conn.run(f'COPY {qualified}\nFROM {literal(prepared["manifest_uri"])}\n'
                     f'IAM_ROLE {literal(args.iam_role)}\nFORMAT AS PARQUET\nMANIFEST;')
        stage = 'validate_count'
        restored = int(conn.run('SELECT COUNT(*) FROM '+qualified)[0][0])
        result['restored_rows'] = restored
        if restored != prepared['expected_rows']:
            raise ArchiveError(f'Row count mismatch: manifest={prepared["expected_rows"]}, restored={restored}')
        stage = 'restore_grants'
        for grant in grants:
            conn.run(grant_sql(schema, table, grant))
        stage = 'restore_owner'
        conn.run('ALTER TABLE '+qualified+' OWNER TO '+identifier(owner))
        stage = 'validate_owner'
        owners = conn.run('''SELECT CAST(u.usename AS VARCHAR(128))
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            JOIN pg_user u ON u.usesysid=c.relowner
            WHERE CAST(n.nspname AS VARCHAR(128))=:schema
            AND CAST(c.relname AS VARCHAR(128))=:table''', schema=schema, table=table)
        if not owners or owners[0][0] != owner:
            raise ArchiveError('Restored table owner does not match the archived owner')
        result['restored_owner'] = owners[0][0]
        stage = 'validate_grants'
        result['extra_table_grants'] = verify_table_grants(conn, schema, table, grants)
        result['table_grants_restored'] = len(grants)
        result['permissions_validation'] = 'expected_supported_grants_present'
        result['schema_usage_validation'] = 'not_checked; review schema/scoped grants separately'
        stage = 'commit'
        conn.run('COMMIT')
        committed = True
        result.update(status='completed', completed_at=utc_now())
    except Exception as exc:
        if conn is not None:
            try:
                conn.run('ROLLBACK')
            except Exception:
                result['rollback_unconfirmed'] = True
        result.update(error_type=type(exc).__name__, failed_step=stage,
                      error=str(exc) if isinstance(exc, ArchiveError) else
                      'Database operation failed; see redshift_message/sqlstate when available, or inspect Redshift logs.')
        result.update(database_error_fields(exc))
        if stage == 'commit':
            result['commit_outcome_unconfirmed'] = True
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                if not committed:
                    result['connection_close_failed'] = True
    return result


def existing_destinations(args, tables, endpoint):
    """Read all destination names before any restore work begins."""
    schema = args.schema + '_tmp' if args.test else args.schema
    conn = database_connection(args, endpoint)
    try:
        rows = conn.run('''SELECT CAST(c.relname AS VARCHAR(128))
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE CAST(n.nspname AS VARCHAR(128))=:schema''', schema=schema)
        selected = set(tables)
        return sorted({row[0] for row in rows if row[0] in selected})
    finally:
        conn.close()


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--sdm-name', required=True)
    p.add_argument('--database', required=True, help='Destination database and archived database folder')
    p.add_argument('--schema', required=True, help='Archived schema; destination is the same unless --test')
    p.add_argument('--table', help='One table; omit to restore every table folder in the schema')
    p.add_argument('--s3-prefix', required=True, help='Root before DATABASE/SCHEMA/TABLE folders')
    p.add_argument('--iam-role', required=True, help='Redshift COPY IAM role ARN')
    p.add_argument('--grants-from-source', action='store_true',
                   help='Read current explicit grants from the original table, for legacy archives; requires the original table and a superuser')
    p.add_argument('--test', action='store_true', help='Restore into SCHEMA_tmp, keeping original table names')
    p.add_argument('--dry-run', action='store_true', help='Read/validate S3 archives only; no Redshift connection or SQL execution')
    p.add_argument('--aws-profile', help='Omit when running through sdm aws run')
    p.add_argument('--aws-region')
    p.add_argument('--db-user', default='sdm')
    p.add_argument('--sdm-bin', default='sdm')
    p.add_argument('--timeout', type=int, default=3600)
    p.add_argument('--statement-timeout-ms', type=int, default=3600000)
    p.add_argument('--summary-file', type=Path)
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    if args.dry_run and args.grants_from_source:
        raise ArchiveError('--grants-from-source requires a database connection; omit --dry-run to use it. Dry run remains S3-only.')
    for name in [args.database, args.schema, args.schema+'_tmp' if args.test else args.schema]:
        identifier(name)
        if len(name.encode()) > 127:
            raise ArchiveError('Database/schema identifier exceeds 127 bytes')
    if args.table is not None:
        identifier(args.table)
    if args.timeout <= 0 or args.statement_timeout_ms <= 0:
        raise ArchiveError('Timeouts must be positive')
    if not re.fullmatch(r'arn:(aws|aws-us-gov|aws-cn):iam::\d{12}:role/[A-Za-z0-9+=,.@_/-]+', args.iam_role):
        raise ArchiveError('--iam-role must be an IAM role ARN')
    import boto3
    bucket, base = s3_location(args.s3_prefix)
    s3 = boto3.Session(profile_name=args.aws_profile, region_name=args.aws_region).client('s3')
    endpoint = None if args.dry_run else connect_sdm(args.sdm_name, args.sdm_bin)
    tables = discover_archives(s3, bucket, base, args.database, args.schema, args.table)
    if not tables:
        raise ArchiveError('No table archive folders found')
    print(json.dumps({'mode': 'dry_run' if args.dry_run else 'restore', 'database': args.database,
                      'destination_schema': args.schema+'_tmp' if args.test else args.schema,
                      'tables': tables}), flush=True)
    if not args.dry_run:
        existing = existing_destinations(args, tables, endpoint)
        if existing:
            schema = args.schema + '_tmp' if args.test else args.schema
            print(json.dumps({'status': 'failed', 'failed_step': 'check_destinations',
                              'destination_schema': schema, 'existing_tables': existing,
                              'error': 'Destination objects already exist; job aborted before restoring any tables.'}), flush=True)
            return 1
    results = []
    for index, table in enumerate(tables):
        try:
            prepared = prepare_archive(args, s3, bucket, base, table)
            if args.dry_run:
                result = {'table': table, 'status': 'archive_validated',
                          'planned_destination_schema': prepared['target_schema'],
                          'database_connected': False, 'destination_existence_checked': False,
                          'ddl_adjustments': prepared.get('ddl_adjustments', []),
                          'planned_owner': prepared['owner'],
                          'planned_table_grants': prepared['permissions']['table_grants'],
                          'expected_rows': prepared['expected_rows'], 'archive_uri': prepared['archive_uri']}
            else:
                result = restore_table(args, table, prepared, endpoint)
        except Exception as exc:
            result = {'table': table, 'status': 'failed', 'error_type': type(exc).__name__,
                      'error': str(exc) if isinstance(exc, ArchiveError) else 'Archive validation/access failed; check S3 access and archive files.'}
        results.append(result)
        print(json.dumps(result), flush=True)
        # Also stop if a destination appears after the initial check. CREATE TABLE
        # has no IF NOT EXISTS, so a concurrent object cannot receive appended data.
        if (result.get('failed_step') == 'check_destination' or result.get('sqlstate') == '42P07'):
            for pending in tables[index+1:]:
                skipped = {'table': pending, 'status': 'failed', 'failed_step': 'job_aborted',
                           'error': 'Not attempted because a destination object already exists.'}
                results.append(skipped)
                print(json.dumps(skipped), flush=True)
            break
    failed = sum(r['status']=='failed' for r in results)
    summary = {'completed_at': utc_now(), 'database': args.database, 'test': args.test,
               'dry_run': args.dry_run, 'succeeded': len(results)-failed, 'failed': failed, 'tables': results}
    if not args.dry_run or args.summary_file:
        path = args.summary_file or Path('redshift_restore_'+re.sub(r'[^0-9]', '', utc_now())+'.json')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, indent=2)+'\n')
        print(json.dumps({'summary_file': str(path), 'succeeded': len(results)-failed, 'failed': failed}))
    return 1 if failed else 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print('Interrupted. Check destination tables before retrying.', file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(str(exc) if isinstance(exc, ArchiveError) else
              f'Restore failed ({type(exc).__name__}); check StrongDM, dependencies, and AWS access.', file=sys.stderr)
        sys.exit(1)
