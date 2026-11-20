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
    return {'ddl': rewritten, 'ddl_adjustments': adjustments,
            'target_schema': target_schema, 'expected_rows': expected,
            'manifest_uri': f's3://{bucket}/{prefix}data/manifest',
            'archive_uri': f's3://{bucket}/{prefix}', **verified}


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


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--sdm-name', required=True)
    p.add_argument('--database', required=True, help='Destination database and archived database folder')
    p.add_argument('--schema', required=True, help='Archived schema; destination is the same unless --test')
    p.add_argument('--table', help='One table; omit to restore every table folder in the schema')
    p.add_argument('--s3-prefix', required=True, help='Root before DATABASE/SCHEMA/TABLE folders')
    p.add_argument('--iam-role', required=True, help='Redshift COPY IAM role ARN')
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
    results = []
    for table in tables:
        try:
            prepared = prepare_archive(args, s3, bucket, base, table)
            if args.dry_run:
                result = {'table': table, 'status': 'archive_validated',
                          'planned_destination_schema': prepared['target_schema'],
                          'database_connected': False, 'destination_existence_checked': False,
                          'ddl_adjustments': prepared.get('ddl_adjustments', []),
                          'expected_rows': prepared['expected_rows'], 'archive_uri': prepared['archive_uri']}
            else:
                result = restore_table(args, table, prepared, endpoint)
        except Exception as exc:
            result = {'table': table, 'status': 'failed', 'error_type': type(exc).__name__,
                      'error': str(exc) if isinstance(exc, ArchiveError) else 'Archive validation/access failed; check S3 access and archive files.'}
        results.append(result)
        print(json.dumps(result), flush=True)
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
