import copy
import unittest
from unittest.mock import MagicMock

from dbdemos.conf import Conf, DemoConf
from dbdemos.job_bundler import JobBundler


PIPELINE_TEMPLATE = {
    "id": "pipeline-bike",
    "run_after_creation": False,
    "definition": {
        "name": "dbdemos_pipeline_bike_{{CATALOG}}_{{SCHEMA}}",
        "catalog": "{{CATALOG}}",
        "schema": "{{SCHEMA}}",
        "root_path": "{{DEMO_FOLDER}}",
        "libraries": [{"glob": {"include": "{{DEMO_FOLDER}}/1-sdp-sql/transformations/**"}}],
        "configuration": {"catalog": "{{CATALOG}}", "schema": "{{SCHEMA}}"},
    },
}


class TestCreateInitBundleJobKeepsTemplates(unittest.TestCase):
    """Bundling resolves pipeline templates for the build run only.

    The packager dumps demo_conf.json_conf to the bundle conf.json, so the
    templates in json_conf must survive the build untouched. Otherwise end users
    install pipelines pointing at the build catalog and the staging repo path.
    """

    def setUp(self):
        conf = Conf(username="test_user@test.com",
                    workspace_url="https://test.cloud.databricks.com",
                    org_id="1234567890",
                    pat_token="test_token",
                    repo_staging_path="/Repos/test_user@test.com",
                    repo_name="dbdemos-notebooks")
        self.bundler = JobBundler(conf)
        self.bundler.installer_pipelines = MagicMock()
        self.bundler.installer_workflow = MagicMock()
        self.bundler.installer_workflow.create_demo_init_job.return_value = {"uid": 123}

        self.json_conf = {
            "name": "pipeline-bike",
            "title": "Bike",
            "category": "data-engineering",
            "description": "description",
            "default_catalog": "main",
            "default_schema": "dbdemos_pipeline_bike",
            "pipelines": [copy.deepcopy(PIPELINE_TEMPLATE)],
            "init_job": {"settings": {"name": "init", "tasks": []}},
        }
        self.demo_conf = DemoConf("product_demos/pipeline-bike", self.json_conf)

    def test_json_conf_pipelines_keep_templates(self):
        self.bundler.create_init_bundle_job(self.demo_conf)
        self.assertEqual(self.json_conf["pipelines"], [PIPELINE_TEMPLATE])

    def test_build_run_uses_build_catalog_and_repo_path(self):
        self.bundler.create_init_bundle_job(self.demo_conf)
        resolved = self.bundler.installer_pipelines.load_demo_pipelines.call_args.args[1].pipelines[0]["definition"]
        self.assertEqual(resolved["catalog"], "main__build")
        self.assertEqual(resolved["schema"], "dbdemos_pipeline_bike")
        self.assertEqual(resolved["root_path"], "/Repos/test_user@test.com/dbdemos-notebooks/product_demos/pipeline-bike")


if __name__ == '__main__':
    unittest.main()
