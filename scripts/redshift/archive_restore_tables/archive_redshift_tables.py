#!/usr/bin/env python3
"""Archive Redshift local tables through StrongDM. Never drops or renames tables."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlsplit


class ArchiveError(RuntimeError):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def identifier(value):
    if not value or '\x00' in value:
        raise ArchiveError('An identifier is empty or contains a null character')
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    # Backslashes must be escaped as well as quotes in Redshift SQL strings.
    return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"


def parse_tables(value):
    if value is None or not value.strip():
        return None
    names = next(csv.reader([value], skipinitialspace=True))
    names = [name.strip() for name in names]
    if not names or any(not name for name in names):
        raise ArchiveError('Empty table name in --tables; omit the option to select the entire schema')
    for name in names:
        identifier(name)
    return list(dict.fromkeys(names))


def s3_location(uri):
    parts = urlsplit(uri)
    if parts.scheme != 's3' or not parts.netloc or parts.query or parts.fragment:
        raise ArchiveError('Use an S3 URI without query or fragment: s3://bucket/prefix/')
    return parts.netloc, parts.path.lstrip('/').rstrip('/')


def archive_prefix(base, database, schema, table):
    components = [quote(x, safe='') for x in [database, schema, table]]
    return '/'.join(([base] if base else []) + components) + '/'


def parse_sdm_status(output, resource):
    for line in output.splitlines():
        columns = re.split(r'\s{2,}', line.strip())
        if len(columns) < 4 or columns[0] != resource:
            continue
        if columns[3].lower() != 'redshift':
            raise ArchiveError(f'StrongDM resource {resource!r} is not a Redshift resource')
        address = columns[2]
        match = re.fullmatch(r'(127\.0\.0\.1|localhost|\[::1\]):(\d+)', address)
        if not match:
            raise ArchiveError(f'Cannot resolve a local StrongDM endpoint from {address!r}')
        port = int(match[2])
        if not 1 <= port <= 65535:
            raise ArchiveError('Invalid StrongDM port')
        return {'connected': columns[1].lower() == 'connected',
                'host': match[1].strip('[]'), 'port': port}
    raise ArchiveError(f'Resource {resource!r} not found in sdm status; check access and sdm login')


def cli_output(args):
    try:
        # sdm aws run injects an AWS HTTPS proxy. StrongDM control commands
        # must reach their own service directly; keep the parent AWS environment
        # intact so boto3 continues to use the StrongDM AWS proxy and credentials.
        control_env = {key: value for key, value in os.environ.items()
                       if key.lower() not in {'http_proxy', 'https_proxy', 'all_proxy'}}
        result = subprocess.run(args, capture_output=True, text=True, timeout=90,
                                check=False, env=control_env)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ArchiveError(f'Cannot run {args[0]}: {type(exc).__name__}') from exc
    if result.returncode:
        raise ArchiveError(f'{args[0]} {args[1]} failed; check StrongDM login/access in your terminal')
    return result.stdout


def connect_sdm(resource, executable='sdm'):
    if resource.startswith('-'):
        raise ArchiveError('StrongDM resource name cannot start with a hyphen')
    state = parse_sdm_status(cli_output([executable, 'status']), resource)
    if not state['connected']:
        cli_output([executable, 'connect', resource])
        state = parse_sdm_status(cli_output([executable, 'status']), resource)
    if not state['connected']:
        raise ArchiveError('StrongDM did not report the resource as connected')
    return state


def database_connection(args, endpoint):
    try:
        import pg8000.native
    except ImportError as exc:
        raise ArchiveError('Install dependencies: python -m pip install -r requirements.txt') from exc
    conn = pg8000.native.Connection(
        user=args.db_user, password=os.environ.get('REDSHIFT_PASSWORD'),
        host=endpoint['host'], port=endpoint['port'], database=args.database,
        timeout=args.timeout,
    )
    try:
        conn.run('SET TIMEZONE TO \'UTC\'')
        conn.run('SET enable_case_sensitive_identifier TO true')
        conn.run(f'SET statement_timeout TO {args.statement_timeout_ms}')
        actual = str(conn.run('SELECT CURRENT_DATABASE()')[0][0]).strip()
        if actual != args.database:
            raise ArchiveError(f'Connected database {actual!r} differs from requested database {args.database!r}')
        return conn
    except Exception:
        conn.close()
        raise


def discover_tables(conn, schema, requested):
    exists = conn.run('SELECT 1 FROM pg_namespace WHERE CAST(nspname AS VARCHAR(128))=:schema', schema=schema)
    if not exists:
        raise ArchiveError(f'Schema {schema!r} does not exist or is not visible')
    rows = conn.run('''SELECT c.oid::BIGINT, CAST(c.relname AS VARCHAR(128)),
        CAST(u.usename AS VARCHAR(128))
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_user u ON u.usesysid=c.relowner
        WHERE c.relkind='r' AND CAST(n.nspname AS VARCHAR(128))=:schema
        ORDER BY 2''', schema=schema)
    tables = {r[1]: {'table_id': int(r[0]), 'name': r[1], 'owner': r[2]} for r in rows}
    if requested is not None:
        missing = [name for name in requested if name not in tables]
        if missing:
            raise ArchiveError('Not visible as regular local tables: ' + ', '.join(missing))
        return [tables[name] for name in requested]
    return list(tables.values())


def unload_sql(schema, table, destination, iam_role):
    query = f'SELECT * FROM {identifier(schema)}.{identifier(table)}'
    return (f'UNLOAD ({literal(query)})\nTO {literal(destination)}\n'
            f'IAM_ROLE {literal(iam_role)}\nFORMAT AS PARQUET\nMANIFEST VERBOSE;')


def put_json(s3, bucket, key, value, new=False):
    kwargs = {'Bucket': bucket, 'Key': key,
              'Body': json.dumps(value, indent=2, default=str).encode(),
              'ContentType': 'application/json'}
    if new:
        kwargs['IfNoneMatch'] = '*'
    s3.put_object(**kwargs)


def get_manifest(s3, bucket, key):
    result = s3.get_object(Bucket=bucket, Key=key)
    try:
        return json.loads(result['Body'].read())
    finally:
        result['Body'].close()


def verify_manifest(s3, bucket, data_prefix, manifest, expected_rows):
    if not isinstance(manifest, dict) or not isinstance(manifest.get('entries'), list):
        raise ArchiveError('Invalid UNLOAD manifest: entries array is missing')
    entries = manifest['entries']
    if not entries and expected_rows:
        raise ArchiveError('Nonempty source table has no exported files')
    seen, rows, size = set(), 0, 0
    for entry in entries:
        if not isinstance(entry, dict):
            raise ArchiveError('Invalid manifest entry')
        part = urlsplit(entry.get('url', ''))
        key = part.path.lstrip('/')
        # UNLOAD produces S3 URLs; only allow this archive's bucket and prefix.
        if part.scheme != 's3' or part.netloc != bucket or not key.startswith(data_prefix):
            raise ArchiveError('Manifest references an object outside this table archive')
        if key in seen or key == data_prefix + 'manifest':
            raise ArchiveError('Duplicate or invalid data entry in manifest')
        seen.add(key)
        meta = entry.get('meta', {})
        row_count, content_length = meta.get('record_count'), meta.get('content_length')
        if type(row_count) is not int or row_count < 0 or type(content_length) is not int or content_length < 0:
            raise ArchiveError('Verbose manifest entry lacks valid record_count/content_length')
        head = s3.head_object(Bucket=bucket, Key=key)
        if head['ContentLength'] != content_length:
            raise ArchiveError('S3 object size does not match its manifest')
        rows += row_count
        size += content_length
    if rows != expected_rows:
        raise ArchiveError(f'Row count mismatch: source={expected_rows}, manifest={rows}')
    totals = manifest.get('meta', {})
    if totals and (totals.get('record_count') != rows or totals.get('content_length') != size):
        raise ArchiveError('Manifest summary does not match entry row counts or file sizes')
    return {'exported_rows': rows, 'data_files': len(entries), 'data_bytes': size}


TABLE_PRIVILEGES = {'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'REFERENCES', 'DROP', 'ALTER', 'TRUNCATE'}
CATALOG_ONLY_PRIVILEGES = {'RULE', 'TRIGGER'}


def normalize_table_grants(grants):
    if not isinstance(grants, list):
        raise ArchiveError('Typed table grant list is missing or invalid')
    normalized, ignored = {}, []
    for grant in grants:
        if not isinstance(grant, dict):
            raise ArchiveError('Invalid table grant entry')
        kind = grant.get('identity_type')
        name = grant.get('identity_name')
        privilege = grant.get('privilege_type')
        option = grant.get('admin_option')
        if kind not in {'user', 'group', 'role', 'public'} or not isinstance(name, str) or not name:
            raise ArchiveError('Grant recipient name/type is missing or unsupported')
        identifier(name)
        if type(option) is not bool:
            raise ArchiveError('Grant option must be a boolean')
        if privilege in CATALOG_ONLY_PRIVILEGES:
            ignored.append(grant)
            continue
        if privilege not in TABLE_PRIVILEGES:
            raise ArchiveError(f'Unsupported table privilege: {privilege!r}')
        if option and kind != 'user':
            raise ArchiveError('WITH GRANT OPTION is supported here only for database users')
        key = (kind, name, privilege)
        normalized[key] = normalized.get(key, False) or option
    result = [dict(zip(['identity_type', 'identity_name', 'privilege_type', 'admin_option'], (*key, option)))
              for key, option in sorted(normalized.items())]
    return result, ignored


def read_table_grants(conn, schema, table):
    rows = conn.run("""SELECT identity_type, identity_name, privilege_type, admin_option
        FROM svv_relation_privileges
        WHERE namespace_name=:schema AND relation_name=:table""", schema=schema, table=table)
    return normalize_table_grants([dict(zip(
        ['identity_type', 'identity_name', 'privilege_type', 'admin_option'], row)) for row in rows])


def capture_table_permissions(conn, schema, table):
    # Limited catalog visibility can look like an empty/missing grant list.
    # Require a superuser for a reliable permissions snapshot and verification.
    admin = conn.run('SELECT usesuper FROM pg_user WHERE usename = CURRENT_USER')
    if not admin or admin[0][0] is not True:
        raise ArchiveError('A Redshift superuser connection is required to capture/verify all explicit table grants')
    grants, ignored = read_table_grants(conn, schema, table)
    return {'version': 1, 'source': 'svv_relation_privileges', 'visibility': 'superuser',
            'table_grants': grants, 'catalog_only_grants': ignored,
            'scope': 'explicit_table_grants_only'}


def archive_table(args, table, run_id, endpoint, s3, bucket, base, connection_factory=database_connection):
    prefix = archive_prefix(base, args.database, args.schema, table['name'])
    data_prefix = prefix + 'data/'
    uri = f's3://{bucket}/{prefix}'
    metadata = {'format_version': 2, 'status': 'in_progress', 'run_id': run_id,
                'started_at': utc_now(), 'sdm_resource': args.sdm_name,
                'database': args.database, 'schema': args.schema, 'table': table['name'],
                'table_id': table['table_id'], 'owner': table['owner'],
                'archive_uri': uri, 'manifest_uri': uri + 'data/manifest',
                'validation': 'not_complete',
                'notes': ['Table archive only; no source tables are deleted or renamed.',
                          'Visible table grants are metadata, not a complete permissions backup.',
                          'A verified export does not replace a test restoration before deletion.']}
    # Claim the table archive location before doing any export.
    # Existing metadata blocks another run from replacing this archive.
    put_json(s3, bucket, prefix + 'metadata.json', metadata, new=True)
    conn = None
    try:
        conn = connection_factory(args, endpoint)
        conn.run('BEGIN')
        qualified = f'{identifier(args.schema)}.{identifier(table["name"])}'
        # Check that the table was not replaced between discovery and export.
        identity = conn.run('''SELECT c.oid::BIGINT FROM pg_class c
            JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE CAST(n.nspname AS VARCHAR(128))=:schema
            AND CAST(c.relname AS VARCHAR(128))=:table AND c.relkind='r' ''',
            schema=args.schema, table=table['name'])
        if not identity or int(identity[0][0]) != table['table_id']:
            raise ArchiveError('Table identity changed after discovery')
        ddl_rows = conn.run(f'SHOW TABLE {qualified}')
        ddl = '\n'.join(str(r[0]) for r in ddl_rows if r and r[0])
        if not ddl.strip():
            raise ArchiveError('SHOW TABLE returned no DDL')
        ddl = ddl.rstrip().rstrip(';') + ';\n'
        metadata['ddl_sha256'] = hashlib.sha256(ddl.encode()).hexdigest()
        s3.put_object(Bucket=bucket, Key=prefix+'ddl.sql', Body=ddl.encode(), ContentType='text/plain')
        grants = conn.run('''SELECT grantor, grantee, privilege_type, is_grantable
            FROM information_schema.table_privileges
            WHERE CAST(table_catalog AS VARCHAR(128))=:database
            AND CAST(table_schema AS VARCHAR(128))=:schema AND CAST(table_name AS VARCHAR(128))=:table''',
            database=args.database, schema=args.schema, table=table['name'])
        metadata['visible_table_grants'] = [dict(zip(['grantor','grantee','privilege_type','is_grantable'], r)) for r in grants]
        metadata['permissions'] = capture_table_permissions(conn, args.schema, table['name'])
        metadata['snapshot_started_at'] = utc_now()
        # COUNT and UNLOAD run in the same explicit transaction snapshot.
        source_rows = int(conn.run(f'SELECT COUNT(*) FROM {qualified}')[0][0])
        metadata['source_row_count'] = source_rows
        conn.run(unload_sql(args.schema, table['name'], f's3://{bucket}/{data_prefix}', args.iam_role))
        after = '\n'.join(str(r[0]) for r in conn.run(f'SHOW TABLE {qualified}') if r and r[0])
        if after.rstrip().rstrip(';')+';\n' != ddl:
            raise ArchiveError('Table DDL changed during export')
        conn.run('COMMIT')
        manifest = get_manifest(s3, bucket, data_prefix+'manifest')
        metadata.update(verify_manifest(s3, bucket, data_prefix, manifest, source_rows))
        metadata.update(status='completed', validation='source_count_and_manifest_files_verified',
                        completed_at=utc_now(), empty_table=source_rows == 0)
        put_json(s3, bucket, prefix+'metadata.json', metadata)
        return metadata
    except Exception as exc:
        if conn is not None:
            try:
                conn.run('ROLLBACK')
            except Exception:
                pass
        # Do not place raw server exceptions, SQL, or credentials into log files.
        metadata.update(status='failed', validation='not_complete', failed_at=utc_now(),
                        error_type=type(exc).__name__,
                        error=str(exc) if isinstance(exc, ArchiveError) else 'Operation failed; check database/S3 access. For another archive, use a new --s3-prefix.')
        try:
            put_json(s3, bucket, prefix+'metadata.json', metadata)
        except Exception:
            metadata['failure_metadata_upload_failed'] = True
        return metadata
    finally:
        if conn is not None:
            conn.close()


def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('--sdm-name', required=True, help='Exact StrongDM Redshift resource name')
    p.add_argument('--database', required=True, help='Redshift source database')
    p.add_argument('--schema', required=True, help='Source schema; case-sensitive identifier')
    p.add_argument('--tables', help='One table or comma-separated table names; omit or pass empty for all local regular tables')
    p.add_argument('--s3-prefix', required=True, help='Archive root, e.g. s3://bucket/redshift-archive/')
    p.add_argument('--iam-role', required=True, help='Role ARN used by Redshift UNLOAD/COPY')
    p.add_argument('--aws-profile', help='AWS profile for Python S3 operations; otherwise use standard AWS credential chain')
    p.add_argument('--aws-region', help='AWS SDK region; Parquet exports require source cluster and bucket in the same region')
    p.add_argument('--db-user', default='sdm', help='StrongDM local database user; optional password from REDSHIFT_PASSWORD')
    p.add_argument('--sdm-bin', default='sdm', help='StrongDM CLI executable')
    p.add_argument('--timeout', type=int, default=3600, help='Database socket timeout in seconds')
    p.add_argument('--statement-timeout-ms', type=int, default=3600000, help='Per-statement timeout in milliseconds')
    p.add_argument('--summary-file', type=Path, help='Local JSON run summary path')
    p.add_argument('--dry-run', action='store_true', help='Connect and list selected tables and destinations; no S3 writes or UNLOAD')
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    if args.timeout <= 0 or args.statement_timeout_ms <= 0:
        raise ArchiveError('Timeout values must be positive')
    for value in [args.database, args.schema]:
        identifier(value)
    if not re.fullmatch(r'arn:(aws|aws-us-gov|aws-cn):iam::\d{12}:role/[A-Za-z0-9+=,.@_/-]+', args.iam_role):
        raise ArchiveError('--iam-role must be an IAM role ARN')
    requested = parse_tables(args.tables)
    bucket, base = s3_location(args.s3_prefix)
    endpoint = connect_sdm(args.sdm_name, args.sdm_bin)
    conn = database_connection(args, endpoint)
    try:
        tables = discover_tables(conn, args.schema, requested)
    finally:
        conn.close()
    if not tables:
        raise ArchiveError('No regular local tables found; nothing was archived')
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '_' + uuid.uuid4().hex[:8]
    selected = [{'table': t['name'], 'table_id': t['table_id'],
                 'archive_uri': f's3://{bucket}/'+archive_prefix(base, args.database, args.schema, t['name'])} for t in tables]
    print(json.dumps({'mode': 'dry_run' if args.dry_run else 'archive', 'run_id':run_id,
                      'database': args.database, 'schema': args.schema,
                      'selected_table_count': len(tables), 'selected_tables': selected}, indent=2), flush=True)
    if args.dry_run:
        return 0
    try:
        import boto3
    except ImportError as exc:
        raise ArchiveError('Install dependencies: python -m pip install -r requirements.txt') from exc
    session = boto3.Session(profile_name=args.aws_profile, region_name=args.aws_region)
    s3 = session.client('s3')
    results = []
    for table in tables:
        try:
            result = archive_table(args, table, run_id, endpoint, s3, bucket, base)
        except Exception as exc:
            # Includes inability to claim metadata.json before an export starts.
            result = {'table':table['name'], 'status':'failed', 'error_type':type(exc).__name__}
        results.append(result)
        print(json.dumps({'table':table['name'], 'status':result['status'],
                          'exported_rows':result.get('exported_rows'),
                          'archive_uri':result.get('archive_uri'),
                          'error_type':result.get('error_type')}, default=str), flush=True)
    summary = {'run_id':run_id, 'database':args.database, 'schema':args.schema,
               'completed_at':utc_now(), 'succeeded':sum(r['status']=='completed' for r in results),
               'failed':sum(r['status']!='completed' for r in results), 'tables':results}
    destination = args.summary_file or Path(f'redshift_archive_{run_id}.json')
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(summary, indent=2, default=str)+'\n')
    print(json.dumps({'summary_file':str(destination), 'succeeded':summary['succeeded'], 'failed':summary['failed']}))
    return 1 if summary['failed'] else 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print('Interrupted; partial S3 files may remain. Do not treat in_progress archives as completed.', file=sys.stderr)
        sys.exit(130)
    except Exception as error:
        print(str(error) if isinstance(error, ArchiveError) else
              f'Archive failed ({type(error).__name__}); check StrongDM, database, and AWS credentials.', file=sys.stderr)
        sys.exit(1)
