import json
from dbdemos.conf import Conf
from dbdemos.job_bundler import JobBundler
from dbdemos.packager import Packager


def load_conf(conf_path):
    with open(conf_path, "r") as r:
        c = json.loads(r.read())

    with open("./dbdemos/resources/default_cluster_config.json", "r") as cc:
        default_cluster_template = cc.read()
    with open("./dbdemos/resources/default_test_job_conf.json", "r") as cc:
        default_cluster_job_template = cc.read()
    return Conf(c['username'], c['url'], c['org_id'], c['pat_token'],
                default_cluster_template, default_cluster_job_template,
                c['repo_staging_path'], c['repo_name'], c['repo_url'], c['branch'])

def bundle(conf, demo_path_in_repo):
    bundler = JobBundler(conf)
    # the bundler will use a stating repo dir in the workspace to analyze & run content.
    bundler.reset_staging_repo(skip_pull=False)

    bundler.add_bundle(demo_path_in_repo)
    # Run the jobs (only if there is a new commit since the last time, or failure, or force execution)
    bundler.start_and_wait_bundle_jobs(force_execution = False, skip_execution=True)
    packager = Packager(conf, bundler)
    packager.package_all()

#Loads conf (your workspace url & token) : local_conf.json.
# Conf file example. This is what will be used as repo content to build the package. You can use the repo you're working on.
""" {
  "pat_token": "xxx",
  "username": "xx.xx@databricks.com",
  "url": "https://e2-demo-field-eng.cloud.databricks.com/",
  "org_id": "1444828305810485",
  "repo_staging_path": "/Repos/xx.xxx@databricks.com",
  "repo_name": "field-demos,
  "repo_url": "<CHANGE WITH YOUR https://github.com/databricks-demos/dbdemos-notebook-demo.git FORK>",
  "branch": "master"
}
"""
conf = load_conf("local_conf_azure.json")

#This will create the bundle and save it in the local ./bundles and ./minisite folder.
#change the path with your demo path in the https://github.com/databricks/field-demo repo (your fork)
try:
    bundle(conf, "product_demos/Data-Science/mlops-end2end")
except Exception as e:
    print(f"Failure building the job: {e}")
    raise e

# Now that your demo is packaged, we can install it & test.
# We recommend testing in a new workspace so that you have a fresh install
# Load the conf for the workspace where you want to install the demo:
conf = load_conf("local_conf_azure.json")

import dbdemos
try:
    #Install your demo in a given folder:
    dbdemos.install("mlops-end2end", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, conf.username,
                    conf.pat_token, conf.workspace_url, cloud="AZURE", start_cluster = False)
    #Check if the init job is successful:
    dbdemos.check_status("sql-ai-functions", conf.username, conf.pat_token, conf.workspace_url, cloud="AWS")
    print("looking good! Ready to send your PR with your new demo!")
except Exception as e:
    print(f"Failure  installing the demo: {e}")
    raise e
