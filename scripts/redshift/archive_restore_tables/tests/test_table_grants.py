import hashlib
import json
import unittest
from unittest.mock import patch
from test_restore_existing_destinations import r
import test_restore_owner as owner_tests
from test_restore_owner import OwnerConnection
import archive_redshift_tables as archive
from archive_redshift_tables import capture_table_permissions, normalize_table_grants


def grant(kind='user', name='dbt', privilege='SELECT', option=False):
    return dict(identity_type=kind, identity_name=name, privilege_type=privilege, admin_option=option)


class GrantConnection(OwnerConnection):
    def __init__(self, grants=None, missing=False, denied=False, admin=True, source=True):
        super().__init__();self.grants=grants or [];self.missing=missing;self.grant_denied=denied;self.admin=admin;self.source=source
    def run(self, sql, **kwargs):
        if sql.startswith('SELECT usesuper'):
            self.sql.append(sql);return [[self.admin]]
        if 'FROM svv_relation_privileges' in sql:
            self.sql.append(sql)
            rows=[] if self.missing and kwargs['schema']=='bronze_tmp' else self.grants
            return [[g[k] for k in ['identity_type','identity_name','privilege_type','admin_option']] for g in rows]
        if "AND c.relkind='r'" in sql:
            self.sql.append(sql);return [[1]] if self.source else []
        if sql.startswith('GRANT') and self.grant_denied:
            self.sql.append(sql);raise RuntimeError('denied')
        return super().run(sql,**kwargs)


class TableGrantTests(unittest.TestCase):
    def fixture(self, grants):
        base=owner_tests.RestoreOwnerTests()
        prepared=base.prepared();prepared['permissions']['table_grants']=grants
        return base.args(),prepared
    def test_capture_typed_grants_filters_catalog_only(self):
        rows=[grant(),grant('group','read_only_users'),grant('role','reporting'),grant('public','public'),
              grant(privilege='DROP'),grant(privilege='TRUNCATE'),grant(privilege='ALTER'),grant(privilege='RULE'),grant(privilege='TRIGGER')]
        snapshot=capture_table_permissions(GrantConnection(rows),'bronze','labor')
        self.assertEqual(len(snapshot['table_grants']),7);self.assertEqual(len(snapshot['catalog_only_grants']),2)
        self.assertEqual(snapshot['visibility'],'superuser')
        with self.assertRaises(r.ArchiveError):capture_table_permissions(GrantConnection(admin=False),'bronze','labor')
    def test_archive_persists_typed_permissions(self):
        args, _ = self.fixture([])
        class Conn(GrantConnection):
            def run(self,sql,**kwargs):
                if sql.startswith('SELECT c.oid'):return [[7]]
                if sql.startswith('SHOW TABLE'):return [['CREATE TABLE bronze.labor (id BIGINT);']]
                if 'information_schema.table_privileges' in sql:return [['owner','dbt','SELECT','NO']]
                return super().run(sql,**kwargs)
        class S3:
            def __init__(self):self.objects={}
            def put_object(self,**kwargs):self.objects[kwargs['Key']]=kwargs['Body']
        s3=S3();conn=Conn([grant('group','readers')])
        with patch.object(archive,'get_manifest',return_value={}), \
             patch.object(archive,'verify_manifest',return_value={'exported_rows':2,'data_files':1,'data_bytes':20}):
            result=archive.archive_table(args,{'name':'labor','table_id':7,'owner':'original_owner'},'run',{},s3,'bucket','archive',lambda *_:conn)
        self.assertEqual(result['status'],'completed')
        saved=json.loads(s3.objects['archive/dev/bronze/labor/metadata.json'])
        self.assertEqual(saved['format_version'],2)
        self.assertEqual(saved['permissions']['table_grants'],[grant('group','readers')])
    def test_grant_sql_recipient_types_options_and_quoting(self):
        for kind,name,expected in [('user','odd"name','TO "odd""name"'),('group','readers','TO GROUP "readers"'),
                                   ('role','reports','TO ROLE "reports"'),('public','public','TO PUBLIC')]:
            self.assertIn(expected,r.grant_sql('schema','table',grant(kind,name)))
        self.assertTrue(r.grant_sql('schema','table',grant(option=True)).endswith('WITH GRANT OPTION;'))
        for bad in [grant(privilege='SELECT; DROP TABLE x'),grant('group',option=True),grant(privilege='RULE')]:
            with self.assertRaises(r.ArchiveError):r.grant_sql('schema','table',bad)
    def test_restore_applies_before_owner_and_verifies(self):
        grants=[grant(),grant('group','readers'),grant('role','reports'),grant(privilege='UPDATE',option=True)]
        args,prepared=self.fixture(grants);conn=GrantConnection(grants)
        result=r.restore_table(args,'labor',prepared,{},lambda *_:conn)
        self.assertEqual(result['status'],'completed');self.assertEqual(result['table_grants_restored'],4)
        self.assertEqual(result['extra_table_grants'],[])
        self.assertLess(next(i for i,s in enumerate(conn.sql) if s.startswith('GRANT')),
                        next(i for i,s in enumerate(conn.sql) if s.startswith('ALTER TABLE')))
    def test_missing_grant_rolls_back(self):
        args,prepared=self.fixture([grant()]);conn=GrantConnection(missing=True)
        result=r.restore_table(args,'labor',prepared,{},lambda *_:conn)
        self.assertEqual(result['failed_step'],'validate_grants');self.assertEqual(conn.sql[-1],'ROLLBACK')
    def test_grant_denied_rolls_back(self):
        args,prepared=self.fixture([grant()]);conn=GrantConnection(denied=True)
        result=r.restore_table(args,'labor',prepared,{},lambda *_:conn)
        self.assertEqual(result['failed_step'],'restore_grants');self.assertEqual(conn.sql[-1],'ROLLBACK')
    def test_extra_defaults_reported_not_revoked(self):
        args,prepared=self.fixture([grant()]);extra=grant('group','default_readers')
        conn=GrantConnection([grant(),extra])
        result=r.restore_table(args,'labor',prepared,{},lambda *_:conn)
        self.assertEqual(result['status'],'completed');self.assertEqual(result['extra_table_grants'],[extra])
        self.assertFalse(any(s.startswith('REVOKE') for s in conn.sql))
    def test_legacy_requires_explicit_source(self):
        with self.assertRaisesRegex(r.ArchiveError,'grants-from-source'):r.archived_permissions({})
        args,prepared=self.fixture([]);args.grants_from_source=True;prepared['permissions']=None
        conn=GrantConnection([grant('group','readers')])
        result=r.restore_table(args,'labor',prepared,{},lambda *_:conn)
        self.assertEqual(result['status'],'completed');self.assertEqual(result['permissions_source'],'live_source_table')
        conn=GrantConnection(source=False)
        result=r.restore_table(args,'labor',prepared,{},lambda *_:conn)
        self.assertEqual(result['failed_step'],'check_permissions')
        self.assertFalse(any(s.startswith('CREATE') for s in conn.sql))
    def test_legacy_prepare_source_flag_and_new_snapshot(self):
        args,_=self.fixture([])
        ddl='CREATE TABLE bronze.labor (id BIGINT);'
        metadata={'status':'completed','database':'dev','schema':'bronze','table':'labor','owner':'original_owner',
                  'ddl_sha256':hashlib.sha256(ddl.encode()).hexdigest(),'source_row_count':2,'exported_rows':2}
        with patch.object(r,'read_text',side_effect=lambda _,__,key: ddl if key.endswith('ddl.sql') else json.dumps(metadata)), \
             patch.object(r,'get_manifest',return_value={}),patch.object(r,'verify_manifest',return_value={'data_files':1}):
            with self.assertRaisesRegex(r.ArchiveError,'typed grant snapshot'):
                r.prepare_archive(args,object(),'bucket','archive','labor')
            args.grants_from_source=True
            self.assertIsNone(r.prepare_archive(args,object(),'bucket','archive','labor')['permissions'])
            args.grants_from_source=False
            metadata['permissions']={'version':1,'visibility':'superuser','scope':'explicit_table_grants_only','table_grants':[grant('role','reports')]}
            self.assertEqual(r.prepare_archive(args,object(),'bucket','archive','labor')['permissions']['table_grants'],[grant('role','reports')])
    def test_grant_option_verification(self):
        with self.assertRaises(r.ArchiveError):r.verify_table_grants(GrantConnection([grant()]),'bronze','labor',[grant(option=True)])
        self.assertEqual(r.verify_table_grants(GrantConnection([grant(option=True)]),'bronze','labor',[grant()]),[grant(option=True)])
    def test_dry_run_live_source_rejected_before_connections(self):
        args,_=self.fixture([])
        argv=['--sdm-name','dev','--database','dev','--schema','bronze','--s3-prefix','s3://bucket/root/',
              '--iam-role',args.iam_role,'--dry-run','--grants-from-source']
        with patch.object(r,'connect_sdm',side_effect=AssertionError('must not connect')):
            with self.assertRaises(r.ArchiveError):r.main(argv)

if __name__=='__main__':unittest.main()
