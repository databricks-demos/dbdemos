import json
from dbdemos.conf import Conf, DemoConf
from dbdemos.installer import Installer
from dbdemos.job_bundler import JobBundler
from dbdemos.packager import Packager
import traceback

with open("./local_conf_E2TOOL.json", "r") as r:
    c = json.loads(r.read())
with open("./dbdemos/resources/default_cluster_config.json", "r") as cc:
    default_cluster_template = cc.read()
with open("./dbdemos/resources/default_test_job_conf.json", "r") as cc:
    default_cluster_job_template = cc.read()

conf = Conf(c['username'], c['url'], c['org_id'], c['pat_token'],
            default_cluster_template, default_cluster_job_template,
            c['repo_staging_path'], c['repo_name'], c['repo_url'], c['branch'], github_token=c['github_token'])


def bundle():
    bundler = JobBundler(conf)
    # the bundler will use a stating repo dir in the workspace to analyze & run content.
    bundler.reset_staging_repo(skip_pull=False, )
    bundler.load_bundles_conf()
    
    # Or manually add bundle to run faster:
    """
    bundler.add_bundle("aibi/aibi-sales-pipeline-review")
    bundler.add_bundle("product_demos/Unity-Catalog/05-Upgrade-to-UC")
    bundler.add_bundle("product_demos/Unity-Catalog/02-External-location")
    #bundler.load_bundles_conf()
    """

    # Run the jobs (only if there is a new commit since the last time, or failure, or force execution)
    bundler.start_and_wait_bundle_jobs(force_execution = False, skip_execution=False, recreate_jobs=False)

    packager = Packager(conf, bundler)
    packager.package_all()

bundle()

#Loads conf to install on cse2.
with open("local_conf_E2FE.json", "r") as r:
    c = json.loads(r.read())

from dbdemos.installer import Installer
import dbdemos


dbdemos.list_demos(pat_token=c['pat_token'])
#dbdemos.install_all("/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS", start_cluster = False, skip_dashboards=False, catalog='main_test_quentin')
#dbdemos.check_status_all()
dbdemos.install("lakehouse-iot-platform", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False, skip_dashboards=False, serverless=True)
#dbdemos.install("lakehouse-fsi-fraud", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False, skip_dashboards=False)
#dbdemos.install("lakehouse-retail-c360", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False, skip_dashboards=False)
#dbdemos.install("lakehouse-fsi-smart-claims", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False, skip_dashboards=False)
#dbdemos.install("lakehouse-fsi-credit", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False, skip_dashboards=False)
#dbdemos.install("computer-vision-pcb", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False, skip_dashboards=False)

#dbdemos.install("uc-01-acl", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], start_cluster = False,  schema='test_quentin_acl', catalog='dbdemos')
#dbdemos.install("uc-02-external-location", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS")
#dbdemos.install("uc-03-data-lineage", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS")
#dbdemos.install("uc-05-upgrade", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'])

    
#dbdemos.install("dlt-cdc", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False, debug=True)
#dbdemos.install("dlt-loans", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False, debug=True)
#dbdemos.install("dlt-unit-test", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False, debug=True)

#dbdemos.install("llm-tools-functions", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS", use_current_cluster=False, current_cluster_id=c["current_cluster_id"], schema='test_quentin_rag', catalog='dbdemos', debug=True)

#dbdemos.install("cdc-pipeline", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False, debug=True)
#dbdemos.install("auto-loader", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False, debug=True)
"""
"""

#dbdemos.install("llm-fine-tuning", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test', cloud="AWS", start_cluster = False)



#dbdemos.install_all("/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS", start_cluster = False, skip_dashboards = True)
#dbdemos.check_status_all(c['username'], c['pat_token'], c['url'], cloud="AWS")
#dbdemos.install("delta-lake", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS", start_cluster = True, skip_dashboards=False)
#dbdemos.install("delta-lake", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS", start_cluster = True, skip_dashboards=False)
#installer = Installer()
#for d in installer.get_demos_available():
#    dbdemos.install(d, "/Users/quentin.ambard@databricks.com/test_dbdemos", True, c['username'], c['pat_token'], c['url'], cloud="AWS")


#dbdemos.list_demos(None)

#dbdemos.install("llm-rag-chatbot", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test_rag', cloud="AWS", start_cluster = False,  skip_dashboards=True, use_current_cluster=True, debug = True)
#dbdemos.install("lakehouse-monitoring", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test_lhm', cloud="AWS", start_cluster = False,  skip_dashboards=False, use_current_cluster=True, debug = True)
#dbdemos.install("uc-04-system-tables", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test_sys', cloud="AWS", start_cluster = False, debug = True, serverless=True)

#dbdemos.install("lakehouse-retail-churn", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS", schema='test_quentin', catalog='dbdemos')
#dbdemos.install("lakehouse-fsi-credit", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS", use_current_cluster=False, skip_dashboards=True)

#dbdemos.install("feature-store", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS", use_current_cluster=False, current_cluster_id=c["current_cluster_id"])
#dbdemos.install("alakehouse-iot-platform", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="Azure", use_current_cluster=False, current_cluster_id=c["current_cluster_id"])
#dbdemos.install("delta-lake", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="GCP")
#dbdemos.install("delta-lake", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="Azure", use_current_cluster=True, current_cluster_id=c["current_cluster_id"])
#dbdemos.install("lakehouse-iot-platform", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS")
#dbdemos.install("lakehouse-retail-churn", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS")
#dbdemos.install("mlops-end2end", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS", skip_dashboards=True, schema='test_quentin_rag', catalog='dbdemos')
#dbdemos.install("pandas-on-spark", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], cloud="AWS")
#dbdemos.install("delta-sharing-airlines", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'])
#dbdemos.install("dlt-cdc", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'])
#dbdemos.install("dlt-loans", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'])
#dbdemos.install("dlt-unit-test", "/Users/quentin.ambard@databricks.com/test_install", True, c['username'], c['pat_token'], c['url'])
#dbdemos.create_cluster("uc-05-upgrade", c['username'], c['pat_token'], c['url'], "GCP")


#dbdemos.install("aibi-marketing-campaign", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test_sys', cloud="AWS", start_cluster = False, debug = True)
#dbdemos.install("aibi-portfolio-assistant", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test_sys', cloud="AWS", start_cluster = False, debug = True)
#dbdemos.install("aibi-supply-chain-forecasting", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test_sys', cloud="AWS", start_cluster = False, debug = True)
#dbdemos.install("aibi-sales-pipeline-review", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test_sys', cloud="AWS", start_cluster = False, debug = True)
#dbdemos.install("aibi-patient-genomics", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test_sys', cloud="AWS", start_cluster = False, debug = True)
#dbdemos.install("aibi-customer-support", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test_sys', cloud="AWS", start_cluster = False, debug = True)



#dbdemos.install("dbt-on-databricks", "/Users/quentin.ambard@databricks.com/test_install_quentin", True, c['username'], c['pat_token'], c['url'], catalog='main', schema='quentin_test_sys', cloud="AWS", start_cluster = False, debug = True, serverless=True)
