import hashlib
import json
import unittest
from unittest.mock import patch
from test_restore_existing_destinations import r, ROLE


class OwnerConnection:
    def __init__(self, owner='original_owner', exists=True, denied=False):
        self.owner=owner;self.exists=exists;self.denied=denied;self.sql=[];self.closed=False
    def run(self,sql,**kwargs):
        self.sql.append(sql)
        if sql.startswith('SELECT usesuper'):return [[True]]
        if sql.startswith('SELECT 1 FROM pg_class'):return []
        if sql.startswith('SELECT 1 FROM pg_user'):return [[1]] if self.exists else []
        if sql.startswith('SELECT COUNT'):return [[2]]
        if sql.startswith('ALTER TABLE') and self.denied:raise RuntimeError('owner transfer denied')
        if sql.startswith('SELECT CAST(u.usename'):return [[self.owner]]
        return []
    def close(self):self.closed=True


class RestoreOwnerTests(unittest.TestCase):
    def args(self):
        return r.parser().parse_args(['--sdm-name','demo','--database','dev','--schema','bronze',
              '--s3-prefix','s3://bucket/archive/','--iam-role',ROLE,'--test'])
    def prepared(self):
        return {'target_schema':'bronze_tmp','expected_rows':2,'data_files':1,'owner':'original_owner',
                'permissions':{'version':1,'visibility':'superuser','scope':'explicit_table_grants_only','table_grants':[]},
                'ddl':'CREATE TABLE "bronze_tmp"."labor" (id BIGINT);',
                'manifest_uri':'s3://bucket/archive/dev/bronze/labor/data/manifest',
                'archive_uri':'s3://bucket/archive/dev/bronze/labor/'}
    def test_owner_set_after_count_before_commit(self):
        conn=OwnerConnection();result=r.restore_table(self.args(),'labor',self.prepared(),{},lambda *_:conn)
        self.assertEqual(result['status'],'completed');self.assertEqual(result['restored_owner'],'original_owner')
        alter='ALTER TABLE "bronze_tmp"."labor" OWNER TO "original_owner"'
        self.assertIn(alter,conn.sql)
        count=next(i for i,s in enumerate(conn.sql) if s.startswith('SELECT COUNT'))
        self.assertLess(count,conn.sql.index(alter));self.assertLess(conn.sql.index(alter),conn.sql.index('COMMIT'))
        self.assertTrue(conn.closed)
    def test_missing_owner_user_blocks_creation(self):
        conn=OwnerConnection(exists=False)
        result=r.restore_table(self.args(),'labor',self.prepared(),{},lambda *_:conn)
        self.assertEqual(result['failed_step'],'check_owner')
        self.assertFalse(any(s.startswith(('CREATE','COPY')) for s in conn.sql));self.assertEqual(conn.sql[-1],'ROLLBACK')
    def test_transfer_error_rolls_back(self):
        conn=OwnerConnection(denied=True)
        result=r.restore_table(self.args(),'labor',self.prepared(),{},lambda *_:conn)
        self.assertEqual(result['failed_step'],'restore_owner');self.assertNotIn('COMMIT',conn.sql)
        self.assertEqual(conn.sql[-1],'ROLLBACK')
    def test_owner_mismatch_rolls_back(self):
        conn=OwnerConnection(owner='wrong_owner')
        result=r.restore_table(self.args(),'labor',self.prepared(),{},lambda *_:conn)
        self.assertEqual(result['failed_step'],'validate_owner');self.assertEqual(conn.sql[-1],'ROLLBACK')
    def test_owner_quoted(self):
        prepared=self.prepared();prepared['owner']='owner"name'
        conn=OwnerConnection(owner='owner"name')
        result=r.restore_table(self.args(),'labor',prepared,{},lambda *_:conn)
        self.assertEqual(result['status'],'completed')
        self.assertIn('ALTER TABLE "bronze_tmp"."labor" OWNER TO "owner""name"',conn.sql)
    def test_prepare_carries_owner_and_rejects_missing(self):
        ddl='CREATE TABLE bronze.labor (id BIGINT);'
        meta={'status':'completed','database':'dev','schema':'bronze','table':'labor','owner':'original_owner',
              'ddl_sha256':hashlib.sha256(ddl.encode()).hexdigest(),'source_row_count':2,'exported_rows':2,
              'permissions':self.prepared()['permissions']}
        with patch.object(r,'read_text',side_effect=lambda _,__,key: ddl if key.endswith('ddl.sql') else json.dumps(meta)), \
             patch.object(r,'get_manifest',return_value={}),patch.object(r,'verify_manifest',return_value={'data_files':1}):
            self.assertEqual(r.prepare_archive(self.args(),object(),'bucket','archive','labor')['owner'],'original_owner')
            del meta['owner']
            with self.assertRaises(r.ArchiveError):r.prepare_archive(self.args(),object(),'bucket','archive','labor')

if __name__=='__main__':unittest.main()
