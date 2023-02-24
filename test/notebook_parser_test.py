import re
import base64
import urllib.parse
import json
from dbdemos.notebook_parser import NotebookParser



def test_close_cell():
    with open("../dbdemos/template/LICENSE.html", "r") as f:
        p = NotebookParser(f.read())
        p.hide_commands_and_results()
        #print(p.get_html())
        #p.hide_command_result(0)


def test_automl():
    with open("../dbdemos/bundles/mlops-end2end/install_package/01_feature_engineering.html", "r") as f:
        p = NotebookParser(f.read())
        assert "Data exploration notebook" in p.content
        assert "Please run the notebook cells to get your AutoML links" not in p.content
        p.remove_automl_result_links()
        assert "Data exploration notebook" not in p.content
        assert "Please run the notebook cells to get your AutoML links" in p.content
        #print(p.get_html())
        #p.hide_command_result(0)

def test_parser_contains():
    with open("../dbdemos/bundles/mlops-end2end/install_package/_resources/00-setup.html", "r") as f:
        p = NotebookParser(f.read())
        assert p.contains("00-global-setup")
        p.replace_in_notebook('00-global-setup', './00-global-setup-test', True)
        assert p.contains("./00-global-setup-test")
        #print(p.get_html())
        #p.hide_command_result(0)

def test_parser_notebook():

    with open("../dbdemos/bundles/lakehouse-retail-c360/install_package/01-Data-ingestion/01.1-DLT-churn-SQL.html", "r") as f:
        p = NotebookParser(f.read())
        assert p.contains("""<a dbdemos-pipeline-id=\\"dlt-churn\\" href=\\"#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d\\" target=\\"_blank\\">""")
        p.replace_dynamic_links_pipeline([{"id": "dlt-churn", "uid": "uuuiduuu"}])
        assert p.contains("""<a dbdemos-pipeline-id=\\"dlt-churn\\" href=\\"#joblist/pipelines/uuuiduuu\\" target=\\"_blank\\">""")

    with open("../dbdemos/bundles/dlt-cdc/install_package/01-Retail_DLT_CDC_SQL.html", "r") as f:
        p = NotebookParser(f.read())
        assert p.contains("""<a dbdemos-pipeline-id=\\"dlt-cdc\\" href=\\"/#joblist/pipelines/c1ccc647-74e6-4754-9c61-6f2691456a73\\">""")
        p.replace_dynamic_links_pipeline([{"id": "dlt-cdc", "uid": "uuuiduuu"}])
        assert p.contains("""<a dbdemos-pipeline-id=\\"dlt-cdc\\" href=\\"/#joblist/pipelines/uuuiduuu\\">""")

    with open("../dbdemos/bundles/dbt-on-databricks/install_package/00-DBT-on-databricks.html", "r") as f:
        p = NotebookParser(f.read())
        p.replace_dynamic_links_pipeline([{"id": "dlt-test", "uid": "uuuiduuu"}])
        #assert """<a dbdemos-pipeline-id="dlt-test" href="https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/uuuiduuu">Delta Live Table Pipeline for unit-test demo</""" in c
        assert p.contains("""<a dbdemos-workflow-id=\\"dbt\\" href=\\"/#job/104444623965854\\">""")
        p.replace_dynamic_links_workflow([{'uid': 450396635732004, 'run_id': 3426479, 'id': 'dbt'}])
        assert p.contains("""<a dbdemos-workflow-id=\\"dbt\\" href=\\"/#job/450396635732004\\">""")

        assert p.contains("""<a dbdemos-repo-id=\\"dbt-databricks-c360\\" href=\\"/#workspace/PLACEHOLDER_CHANGED_AT_INSTALL_TIME/README.md\\">""")
        p.replace_dynamic_links_repo([{'uid': '/Repos/quentin.ambard@databricks.com/dbdemos-dbt-databricks-c360', 'id': 'dbt-databricks-c360', 'repo_id': 3891038073826409}])
        assert p.contains("""<a dbdemos-repo-id=\\"dbt-databricks-c360\\" href=\\"/#workspace/Repos/quentin.ambard@databricks.com/dbdemos-dbt-databricks-c360/README.md\\">""")



test_close_cell()
test_automl()
test_parser_contains()
test_parser_notebook()