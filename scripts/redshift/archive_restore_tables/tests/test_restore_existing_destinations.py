import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

root=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(root))
spec=importlib.util.spec_from_file_location('restore_under_test',root/'restore_redshift_tables.py')
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
ROLE='arn:aws:iam::206390103201:role/is-dev-redshift-spectrum-s3-role'

class ExistingTableTests(unittest.TestCase):
    def argv(self):
        return ['--sdm-name','demo','--database','dev','--schema','bronze','--s3-prefix','s3://bucket/archive/','--iam-role',ROLE,'--test']
    def test_preflight_checks_selected_names_and_test_schema(self):
        class Conn:
            def run(self,sql,**kwargs):
                self.schema=kwargs['schema'];return [['existing'],['unrelated']]
            def close(self):self.closed=True
        conn=Conn()
        with patch.object(r,'database_connection',return_value=conn):
            found=r.existing_destinations(r.parser().parse_args(self.argv()),['new','existing'],{})
        self.assertEqual(found,['existing']);self.assertEqual(conn.schema,'bronze_tmp');self.assertTrue(conn.closed)
    def run_main(self,existing,results,dry=False):
        module=types.ModuleType('boto3');module.Session=lambda **_:types.SimpleNamespace(client=lambda _:object())
        with tempfile.TemporaryDirectory() as temp, patch.dict(sys.modules,{'boto3':module}), \
             patch.object(r,'connect_sdm',return_value={}), \
             patch.object(r,'discover_archives',return_value=['new','existing','later']), \
             patch.object(r,'existing_destinations',return_value=existing) as check, \
             patch.object(r,'prepare_archive',return_value={'target_schema':'bronze_tmp','expected_rows':1,'archive_uri':'s3://bucket/archive/','owner':'original_owner','permissions':{'table_grants':[]}}) as prepare, \
             patch.object(r,'restore_table',side_effect=results) as restore, \
             contextlib.redirect_stdout(io.StringIO()) as output:
            argv=self.argv()+['--summary-file',temp+'/summary.json']+(['--dry-run'] if dry else [])
            code=r.main(argv)
            lines=[json.loads(line) for line in output.getvalue().splitlines()]
            calls=(check.call_count,prepare.call_count,restore.call_count)
        return code,lines,calls
    def test_existing_aborts_before_any_restore(self):
        code,lines,calls=self.run_main(['existing'],[])
        self.assertEqual(code,1);self.assertEqual(calls,(1,0,0))
        self.assertEqual(lines[-1]['existing_tables'],['existing'])
    def test_duplicate_race_stops_later_tables(self):
        code,lines,calls=self.run_main([], [{'table':'new','status':'failed','failed_step':'create_table','sqlstate':'42P07'}])
        self.assertEqual(code,1);self.assertEqual(calls,(1,1,1))
        self.assertEqual(sum(line.get('failed_step')=='job_aborted' for line in lines),2)
    def test_other_failures_continue(self):
        results=[{'table':'new','status':'failed','failed_step':'validate_count'},
                 {'table':'existing','status':'completed'},{'table':'later','status':'completed'}]
        code,lines,calls=self.run_main([],results)
        self.assertEqual(code,1);self.assertEqual(calls,(1,3,3))
        self.assertEqual(lines[-1]['succeeded'],2)
    def test_dry_run_never_checks_database(self):
        code,lines,calls=self.run_main([],[],dry=True)
        self.assertEqual(code,0);self.assertEqual(calls,(0,3,0))

if __name__=='__main__':unittest.main()
