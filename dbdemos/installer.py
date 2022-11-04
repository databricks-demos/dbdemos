import collections

import pkg_resources
from .conf import DBClient, DemoConf, Conf, ConfTemplate, merge_dict, DemoNotebook
from .tracker import Tracker
from .notebook_parser import NotebookParser
from pathlib import Path
import time
import json
import re
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import urllib
import threading

CSS_REPORT = """
<style>
.dbdemos_install{
                    font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji,FontAwesome;
color: #3b3b3b;
box-shadow: 0 .15rem 1.15rem 0 rgba(58,59,69,.15)!important;
padding: 10px;
margin: 10px;
}
.code {
    padding: 5px;
border: 1px solid #e4e4e4;
font-family: monospace;
background-color: #f5f5f5;
margin: 5px 0px 0px 0px;
display: inline;
}
</style>"""

class Installer:
    def __init__(self, username = None, pat_token = None, workspace_url = None, cloud = "AWS"):
        self.cloud = cloud
        self.dbutils = None
        if username is None:
            username = self.get_current_username()
        if workspace_url is None:
            workspace_url = self.get_current_url()
        if pat_token is None:
            pat_token = self.get_current_pat_token()
        conf = Conf(username, workspace_url, pat_token)
        self.tracker = Tracker(self.get_org_id(), self.get_uid())
        self.db = DBClient(conf)

    def displayHTML_available(self):
        try:
            from dbruntime.display import displayHTML
            return True
        except:
            return False

    #TODO replace with https://github.com/mlflow/mlflow/blob/master/mlflow/utils/databricks_utils.py#L64 ?
    def get_dbutils(self):
        if self.dbutils is None:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark.conf.get("spark.databricks.service.client.enabled") == "true":
                from pyspark.dbutils import DBUtils
                self.dbutils = DBUtils(spark)
            else:
                import IPython
                self.dbutils = IPython.get_ipython().user_ns["dbutils"]
        return self.dbutils

    def get_current_url(self):
        try:
            return "https://"+self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
        except:
            return "local"

    def get_org_id(self):
        try:
            return self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().tags().apply('orgId')
        except:
            return "local"

    def get_uid(self):
        try:
            return self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().tags().apply('userId')
        except:
            return "local"

    def get_current_folder(self):
        try:
            current_notebook = self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
            return current_notebook[:current_notebook.rfind("/")]
        except:
            return "local"

    def get_workspace_id(self):
        try:
            return self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().workspaceId().get()
        except:
            return "local"

    def get_current_pat_token(self):
        try:
            return self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        except:
            return "local"

    def get_current_username(self):
        try:
            return self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
        except:
            return "local"

    def get_current_cloud(self):
        try:
            hostname = self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
        except:
            print(f"WARNING: Can't get cloud from dbutils. Fallback to default local cloud {self.cloud}")
            return self.cloud
        if "gcp" in hostname:
            return "GCP"
        elif "azure" in hostname:
            return "AZURE"
        else:
            return "AWS"


    def check_demo_name(self, demo_name):
        demo_availables = self.get_demos_available()
        # TODO: where should we store the bundle, as .zip in the wheel and extract them locally?
        if demo_name not in demo_availables:
            demos = '\n  - '.join(demo_availables)
            raise Exception(f"The demo {demo_name} doesn't exist. \nDemos currently available: \n  - {demos}")

    def get_demos_available(self):
        return set(pkg_resources.resource_listdir("dbdemos", "bundles"))

    def get_demo_conf(self, demo_name:str, demo_folder: str = ""):
        demo = self.get_resource(f"bundles/{demo_name}/conf.json")
        conf_template = ConfTemplate(self.db.conf.username, demo_name, demo_folder)
        return DemoConf(demo_name, json.loads(conf_template.replace_template_key(demo)))

    def get_resource(self, path):
        return pkg_resources.resource_string("dbdemos", path).decode('UTF-8')

    def install_demo(self, demo_name, install_path, overwrite=False, update_cluster_if_exists = True, skip_dashboards = False):
        # first get the demo conf.
        if install_path is None:
            install_path = self.get_current_folder()
        elif install_path.startswith("./"):
            install_path = self.get_current_folder()+"/"+install_path[2:]
        print(f"Installing demo {demo_name} under {install_path}...")
        self.check_demo_name(demo_name)
        demo_conf = self.get_demo_conf(demo_name, install_path+"/"+demo_name)
        self.tracker.track_install(demo_conf.category, demo_name)
        self.get_current_username()
        cluster_id, cluster_name = self.load_demo_cluster(demo_name, demo_conf, update_cluster_if_exists)
        pipeline_ids = self.load_demo_pipelines(demo_name, demo_conf)
        dashboards = [] if skip_dashboards else self.install_dashboards(demo_conf, install_path)
        notebooks = self.install_notebooks(demo_name, install_path, demo_conf, cluster_name, cluster_id, pipeline_ids, dashboards, overwrite)
        job_id, run_id = self.start_demo_init_job(demo_conf)
        for pipeline in pipeline_ids:
            if pipeline["run_after_creation"]:
                self.db.post(f"2.0/pipelines/{pipeline['uid']}/updates", { "full_refresh": True })

        self.display_install_result(demo_name, demo_conf.description, demo_conf.title, install_path, notebooks, job_id, run_id, cluster_id, cluster_name, pipeline_ids, dashboards)

    def install_dashboards(self, demo_conf: DemoConf, install_path):
        result = []
        if "dashboards" in pkg_resources.resource_listdir("dbdemos", "bundles/"+demo_conf.name):
            print(f'    Installing dashsboards')
            # TODO: could parallelize that?
            for dashboard in pkg_resources.resource_listdir("dbdemos", "bundles/"+demo_conf.name+"/dashboards"):
                definition = json.loads(self.get_resource("bundles/"+demo_conf.name+"/dashboards/"+dashboard))
                id = dashboard[:dashboard.rfind(".json")]
                existing_dashboard = self.get_dashboard_id_by_name(definition['dashboard']['name'])
                #Create the folder where to save the queries
                path = f'{install_path}/dbdemos_dashboards/{demo_conf.name}'
                self.db.post("2.0/workspace/mkdirs", {"path": path})
                folders = self.db.get("2.0/workspace/list", {"path": Path(path).parent.absolute()})
                if "error_code" in folders:
                    raise Exception(f"ERROR - wrong install path: {folders}")
                parent_folder_id = None
                for f in folders["objects"]:
                    if f["object_type"] == "DIRECTORY" and f["path"] == path:
                        parent_folder_id = f["object_id"]
                data = {
                    'import_file_contents': definition,
                    'parent': f'folders/{parent_folder_id}'
                }
                endpoint_id = self.get_or_create_endpoint()
                if endpoint_id is None:
                    print("ERROR: couldn't create or get a SQL endpoint for dbdemos. Do you have permission? Trying to import the dashboard without (import will pick the first available if any)")
                else:
                    data['warehouse_id'] = endpoint_id
                if existing_dashboard is not None:
                    data['overwrite_dashboard_id'] = existing_dashboard
                    data['should_overwrite_existing_queries'] = True
                i = self.db.post(f"2.0/preview/sql/dashboards/import", data)
                if "id" in i:
                    result.append({"id": id, "name": definition['dashboard']['name'], "installed_id": i["id"]})
                    permissions = {"access_control_list": [
                        {"user_name": self.db.conf.username, "permission_level": "CAN_MANAGE"},
                        {"group_name": "users", "permission_level": "CAN_EDIT"}
                    ]}
                    permissions = self.db.post("2.0/preview/sql/permissions/dashboards/"+i["id"], permissions)
                    print(f"     Dashboard {definition['dashboard']['name']} permissions set to {permissions}")
                    self.db.post("2.0/preview/sql/dashboards/"+i["id"], {"run_as_role": "viewer"})
                else:
                    print(f"    ERROR loading dashboard {definition['dashboard']['name']}: {i}, {existing_dashboard}")
                    result.append({"id": id, "name": definition['dashboard']['name'], "error": i, "installed_id": existing_dashboard})
        return result

    def get_dashboard_id_by_name(self, name):
        def get_dashboard(page):
            ds = self.db.get("2.0/preview/sql/dashboards", params = {"page_size": 250, "page": page})
            for d in ds['results']:
                if d['name'] == name:
                    return d['id']
            if ds["count"] >= 250:
                return get_dashboard(page+1)
            return None
        return get_dashboard(1)

    def get_demo_datasource(self):
        data_sources = self.db.get("2.0/preview/sql/data_sources")
        for source in data_sources:
            if source['name'] == "dbdemos-shared-endpoint":
                return source
        #Try to fallback to an existing shared endpoint.
        for source in data_sources:
            if "shared-sql-endpoint" in source['name'].lower():
                return source
        for source in data_sources:
            if "shared" in source['name'].lower():
                return source
        return None

    def get_or_create_endpoint(self):
        ds = self.get_demo_datasource()
        if ds is not None:
            return ds["warehouse_id"]
        def get_definition(serverless):
            return {
                "name": "dbdemos-shared-endpoint",
                "cluster_size": "Small",
                "min_num_clusters": 1,
                "max_num_clusters": 1,
                "tags": {
                    "custom_tags": [{"project": "databricks-demo"}]
                },
                "spot_instance_policy": "COST_OPTIMIZED",
                "enable_photon": "true",
                "enable_serverless_compute": serverless,
                "channel": { "name": "CHANNEL_NAME_CURRENT" }
            }
        w = self.db.post("2.0/sql/warehouses", json=get_definition(True))
        if "id" in w:
            return w["id"]
        w = self.db.post("2.0/sql/warehouses", json=get_definition(False))
        if "id" in w:
            return w["id"]
        return None

    def display_install_result(self, demo_name, description, title, install_path = None, notebooks = [], job_id = None, run_id = None, cluster_id = None, cluster_name = None, pipelines_ids = [], dashboards = []):
        if self.displayHTML_available():
            self.display_install_result_html(demo_name, description, title, install_path, notebooks, job_id, run_id, cluster_id, cluster_name, pipelines_ids, dashboards)
        else:
            self.display_install_result_console(demo_name, description, title, install_path, notebooks, job_id, run_id, cluster_id, cluster_name, pipelines_ids, dashboards)

    def display_install_result_html(self, demo_name, description, title, install_path = None, notebooks = [], job_id = None, run_id = None, cluster_id = None, cluster_name = None, pipelines_ids = [], dashboards = []):
        html = f"""{CSS_REPORT}
        <div class="dbdemos_install">
            <img style="float:right; width: 100px; padding: 20px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/resources/{demo_name}.png" />
            <h1>Your demo {title} is ready!</h1>
            <i>{description}</i><br/><br/>
            """
        if cluster_id is not None:
            cluster_section = f"""
            <h2>Interactive cluster for the demo:</h2>
            <a href="{self.db.conf.workspace_url}/#setting/clusters/{cluster_id}/configuration">{cluster_name}</a>. You can refresh your demo cluster with:
            <div class="code">
                dbdemos.create_cluster('{demo_name}')
            </div>"""
            cluster_instruction = f' using the cluster <a href="{self.db.conf.workspace_url}/#setting/clusters/{cluster_id}/configuration">{cluster_name}</a>'
        else:
            cluster_section = ""
            cluster_instruction = ""
        if len(notebooks) > 0:
            first = list(filter(lambda n: "/" not in n.path, notebooks))
            first.sort(key=lambda n: n.path)
            html += f"""Start with the first notebook <a href="{self.db.conf.workspace_url}/#workspace{install_path}/{demo_name}/{first[0].path}">{demo_name}/{first[0].path}</a>{cluster_instruction}"""
            html += """<h2>Notebook installed:</h2><ul>"""
            for n in notebooks:
                if "_resources" not in n.path:
                    html += f"""<li>{n.path}: <a href="{self.db.conf.workspace_url}/#workspace{install_path}/{demo_name}/{n.path}">{n.title}</a></li>"""
            html += """</ul>"""
        if len(pipelines_ids) > 0:
            html += f"""<h2>Delta Live Table Pipelines</h2><ul>"""
            for p in pipelines_ids:
                html += f"""<li><a href="{self.db.conf.workspace_url}/#joblist/pipelines/{p['uid']}">{p['name']}</a></li>"""
            html +="</ul>"
        if len(dashboards) > 0:
            html += f"""<h2>DBSQL Dashboards</h2><ul>"""
            for d in dashboards:
                if "error" in d:
                    error_already_installed  = ""
                    if d["installed_id"] is not None:
                        error_already_installed = f""" A dashboard with the same name exists: <a href="{self.db.conf.workspace_url}/sql/dashboards/{d['installed_id']}">{d['name']}</a>"""
                    html += f"""<li>ERROR INSTALLING DASHBOARD {d['name']}: {d['error']}. The Import/Export API must be enabled.{error_already_installed}</li>"""
                else:
                    html += f"""<li><a href="{self.db.conf.workspace_url}/sql/dashboards/{d['installed_id']}">{d['name']}</a></li>"""

            html +="</ul>"
        if job_id is not None:
            html += f"""<h2>Initialization job started</h2>
                        We started a <a href="{self.db.conf.workspace_url}/#job/{job_id}/run/{run_id}">job</a> to initialize your demo data (for DBSQL Dashboards & Delta Live Table). 
                        Please wait for the job completion to be able to access the dataset & dashboards..."""
        html += cluster_section+"</div>"
        from dbruntime.display import displayHTML
        displayHTML(html)

    def display_install_result_console(self, demo_name, description, title, install_path = None, notebooks = [], job_id = None, run_id = None, cluster_id = None, cluster_name = None, pipelines_ids = [], dashboards = []):
        if len(notebooks) > 0:
            print("----------------------------------------------------")
            print("-------------- Notebook installed: -----------------")
            for n in notebooks:
                if "_resources" not in n.path:
                    print(f"   - {n.title}: {self.db.conf.workspace_url}/#workspace{install_path}/{demo_name}/{n.path}")
        if job_id is not None:
            print("----------------------------------------------------")
            print("--- Job initialization started (load demo data): ---")
            print(f"    - Job run available under: {self.db.conf.workspace_url}/#job/{job_id}/run/{run_id}")
        if cluster_id is not None:
            print("----------------------------------------------------")
            print("------------ Demo interactive cluster: -------------")
            print(f"    - {cluster_name}: {self.db.conf.workspace_url}/#setting/clusters/{cluster_id}/configuration")
            cluster_instruction = f" using the cluster {cluster_name}"
        else:
            cluster_instruction = ""
        if len(pipelines_ids) > 0:
            print("----------------------------------------------------")
            print("------------ Delta Live Table available: -----------")
            for p in pipelines_ids:
                print(f"    - {p['name']}: {self.db.conf.workspace_url}/#joblist/pipelines/{p['uid']}")
        if len(dashboards) > 0:
            print("----------------------------------------------------")
            print("------------- DBSQL Dashboard available: -----------")
            for d in dashboards:
                error_already_installed  = ""
                if d["installed_id"] is not None:
                    error_already_installed = f""" A dashboard with the same name exists: <a href="{self.db.conf.workspace_url}/sql/dashboards/{d['installed_id']}">{d['name']}</a>"""
                if "error" in d:
                    print(f"    - ERROR INSTALLING DASHBOARD {d['name']}: {d['error']}. The Import/Export API must be enabled.{error_already_installed}")
                else:
                    print(f"    - {d['name']}: {self.db.conf.workspace_url}/sql/dashboards/{d['installed_id']}")
        print("----------------------------------------------------")
        print(f"Your demo {title} is ready! ")
        if len(notebooks) > 0:
            first = list(filter(lambda n: "/" not in n.path, notebooks))
            first.sort(key=lambda n: n.path)
            print(f"Start with the first notebook {demo_name}/{first[0].path}{cluster_instruction}: {self.db.conf.workspace_url}/#workspace{install_path}/{demo_name}/{first[0].path}.")

    def install_notebooks(self, demo_name: str, install_path: str, demo_conf: DemoConf, cluster_name: str, cluster_id: str, pipeline_ids, dashboards, overwrite=False):
        assert len(demo_name) > 4, "wrong demo name. Fail to prevent potential delete errors."
        print(f'    Installing notebooks')
        install_path = install_path+"/"+demo_name
        s = self.db.get("2.0/workspace/get-status", {"path": install_path})
        if 'object_type' in s:
            if not overwrite:
                if self.displayHTML_available():
                    from dbruntime.display import displayHTML
                    displayHTML(f"""{CSS_REPORT}<div class="dbdemos_install">
                      <h1 style="color: red">Error!</h1>
                      <bold>Folder {install_path} isn't empty</bold>. Please install demo with overwrite=True to replace the existing content: 
                      <div class="code">
                              dbdemos.install('{demo_name}', overwrite=True)
                      </div>
                    </div>""")
                raise Exception(f"Folder {install_path} isn't empty. Please install demo with overwrite=True to replace the existing content")
            print(f"    Folder {install_path} already exists. Deleting the existing content...")
            d = self.db.post("2.0/workspace/delete", {"path": install_path, 'recursive': True})
            if 'error_code' in d:
                raise Exception(f"Couldn't erase folder {install_path}. Do you have permission? Error: {d}")

        folders_created = set()
        #Avoid multiple mkdirs in parallel as it's creating error.
        folders_created_lock = threading.Lock()
        def load_notebook(notebook):
            return load_notebook_path(notebook, "bundles/"+demo_name+"/install_package/"+notebook.path+".html")

        def load_notebook_path(notebook: DemoNotebook, template_path):
            parser = NotebookParser(self.get_resource(template_path))
            if notebook.add_cluster_setup_cell:
                self.add_cluster_setup_cell(parser, demo_name, cluster_name, cluster_id, self.db.conf.workspace_url)
            parser.replace_dashboard_links(dashboards)
            parser.remove_automl_result_links()
            parser.replace_dynamic_links_pipeline(pipeline_ids)
            parser.set_tracker_tag(self.get_org_id(), self.get_uid(), demo_conf.category, demo_name, notebook.path)
            content = parser.get_html()
            content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
            parent = str(Path(install_path+"/"+notebook.path).parent)
            with folders_created_lock:
                if parent not in folders_created:
                    r = self.db.post("2.0/workspace/mkdirs", {"path": parent})
                    folders_created.add(parent)
                    if 'error_code' in r:
                        if r['error_code'] == "RESOURCE_ALREADY_EXISTS":
                            print(f"ERROR: A folder already exists under {install_path}. Add the overwrite option to replace the content:")
                            print(f"dbdemos.install('{demo_name}', overwrite=True)")
                        raise Exception(f"Couldn't create folder under {install_path}. Import error: {r}")
            r = self.db.post("2.0/workspace/import", {"path": install_path+"/"+notebook.path, "content": content, "format": "HTML"})
            if 'error_code' in r:
                raise Exception(f"Couldn't install demo under {install_path}/{notebook.path}. Do you have permission?. Import error: {r}")
            return notebook

        #Always adds the licence notebooks
        with ThreadPoolExecutor(max_workers=5) as executor:
            notebooks = [
                DemoNotebook("_resources/LICENSE", "LICENSE", "Demo License"),
                DemoNotebook("_resources/NOTICE", "NOTICE", "Demo Notice"),
                DemoNotebook("_resources/README", "README", "Readme")
            ]
            def load_notebook_templet(notebook):
                load_notebook_path(notebook, f"template/{notebook.title}.html")
            collections.deque(executor.map(load_notebook_templet, notebooks))

        with ThreadPoolExecutor(max_workers=5) as executor:
            return [n for n in executor.map(load_notebook, demo_conf.notebooks)]


    #Start the init job if it exists
    def start_demo_init_job(self, demo_conf: DemoConf):
        if "settings" in demo_conf.init_job:
            print(f"    Searching for existing demo initialisation job {demo_conf.init_job['settings']['name']}")
            #We have an init jon
            job_name = demo_conf.init_job["settings"]["name"]
            #add cloud specific setup
            cloud = self.get_current_cloud()
            cluster_conf_cloud = json.loads(self.get_resource(f"resources/default_cluster_config-{cloud}.json"))
            for cluster in demo_conf.init_job["settings"]["job_clusters"]:
                if "new_cluster" in cluster:
                    merge_dict(cluster["new_cluster"], cluster_conf_cloud)
            existing_job = self.db.find_job(job_name)
            if existing_job is not None:
                job_id = existing_job["job_id"]
                self.db.post("/2.1/jobs/runs/cancel-all", {"job_id": job_id})
                self.wait_for_run_completion(job_id)
                print("    Updating existing job")
                r = self.db.post("2.1/jobs/reset", {"job_id": job_id, "new_settings": demo_conf.init_job["settings"]})
                if "error_code" in r:
                    raise Exception(f'ERROR setting up init job, do you have permission? please check job definition {r}, {demo_conf.init_job["settings"]}')
            else:
                print("    Creating a new job for demo initialization (data & table setup).")
                r_jobs = self.db.post("2.1/jobs/create", demo_conf.init_job["settings"])
                if "error_code" in r_jobs:
                    raise Exception(f'error setting up job, please check job definition {r_jobs}, {demo_conf.init_job["settings"]}')
                job_id = r_jobs["job_id"]
            j = self.db.post("2.1/jobs/run-now", {"job_id": job_id})
            print(f"    Demo data initialization job started: {self.db.conf.workspace_url}/#job/{job_id}/run/{j['run_id']}")
            return job_id, j['run_id']
        return None, None


    def wait_for_run_completion(self, job_id, max_retry=10):
        def is_still_running(job_id):
            runs = self.db.get("2.1/jobs/runs/list", {"job_id": job_id, "active_only": "true"})
            return "runs" in runs and len(runs["runs"]) > 0
        i = 0
        while i <= max_retry and is_still_running(job_id):
            print(f"      A run is still running for job {job_id}, waiting for termination...")
            time.sleep(5)

    def load_demo_pipelines(self, demo_name, demo_conf: DemoConf):
        #default cluster conf
        pipeline_ids = []
        for pipeline in demo_conf.pipelines:
            definition = pipeline["definition"]
            today = date.today().strftime("%Y-%m-%d")
            #enforce demo tagging in the cluster
            for cluster in definition["clusters"]:
                merge_dict(cluster, {"custom_tags": {"project": "databricks-demo", "demo": demo_name, "demo_install_date": today}})
            existing_pipeline = self.get_pipeline(definition["name"])
            print(f'    Installing pipeline {definition["name"]}')
            if existing_pipeline == None:
                p = self.db.post("2.0/pipelines", definition)
                id = p['pipeline_id']
            else:
                print("    Updating existing pipeline with last configuration")
                id = existing_pipeline['pipeline_id']
                self.db.put("2.0/pipelines/"+id, definition)
            pipeline_ids.append({"name": definition['name'], "uid": id, "id": pipeline["id"], "run_after_creation": pipeline["run_after_creation"] or existing_pipeline is not None})
            #Update the demo conf tags {{}} with the actual id (to be loaded as a job for example)
            demo_conf.set_pipeline_id(pipeline["id"], id)
        return pipeline_ids

    def load_demo_cluster(self, demo_name, demo_conf: DemoConf, update_cluster_if_exists):
        #default cluster conf
        conf_template = ConfTemplate(self.db.conf.username, demo_name)
        cluster_conf = self.get_resource("resources/default_cluster_config.json")
        cluster_conf = json.loads(conf_template.replace_template_key(cluster_conf))
        #add cloud specific setup
        cloud = self.get_current_cloud()
        cluster_conf_cloud = self.get_resource(f"resources/default_cluster_config-{cloud}.json")
        cluster_conf_cloud = json.loads(conf_template.replace_template_key(cluster_conf_cloud))
        merge_dict(cluster_conf, cluster_conf_cloud)
        merge_dict(cluster_conf, demo_conf.cluster)
        if "spark.databricks.cluster.profile" in cluster_conf["spark_conf"] and cluster_conf["spark_conf"]["spark.databricks.cluster.profile"] == "singleNode":
            del cluster_conf["autoscale"]
            cluster_conf["num_workers"] = 0

        existing_cluster = self.find_cluster(cluster_conf["cluster_name"])
        if existing_cluster == None:
            cluster = self.db.post("2.0/clusters/create", json = cluster_conf)
            cluster_conf["cluster_id"] = cluster["cluster_id"]
        else:
            cluster_conf["cluster_id"] = existing_cluster["cluster_id"]
            if update_cluster_if_exists:
                cluster = self.db.post("2.0/clusters/edit", json = cluster_conf)
                if "error_code" in cluster and cluster["error_code"] == "INVALID_STATE":
                    print(f"    Demo cluster {cluster_conf['cluster_name']} in invalid state. Stopping it...")
                    cluster = self.db.post("2.0/clusters/delete", json = {"cluster_id": cluster_conf["cluster_id"]})
                    i = 0
                    while i < 30:
                        i += 1
                        cluster = self.db.get("2.0/clusters/get", params = {"cluster_id": cluster_conf["cluster_id"]})
                        if cluster["state"] == "TERMINATED":
                            print("    Cluster properly stopped.")
                            break
                        time.sleep(2)
                    if cluster["state"] != "TERMINATED":
                        print(f"    WARNING: Couldn't stop the demo cluster properly. Unknown state. Please stop your cluster {cluster_conf['cluster_name']} before.")
                    self.db.post("2.0/clusters/edit", json = cluster_conf)
            self.db.post("2.0/clusters/start", json = cluster_conf)
        return cluster_conf['cluster_id'], cluster_conf['cluster_name']

    #return the cluster with the given name or none
    def find_cluster(self, cluster_name):
        clusters = self.db.get("2.0/clusters/list")
        if "clusters" in clusters:
            for c in clusters["clusters"]:
                if c["cluster_name"] == cluster_name:
                    return c
        return None

    def get_pipeline(self, name):
        def get_pipelines(token = None):
            r = self.db.get("2.0/pipelines", {"max_results": 100, "page_token": token})
            if "statuses" in r:
                for p in r["statuses"]:
                    if p["name"] == name:
                        return p
            if "next_page_token" in r:
                return get_pipelines(r["next_page_token"])
            return None
        return get_pipelines()


    def add_cluster_setup_cell(self, parser: NotebookParser, demo_name, cluster_name, cluster_id, env_url):
        content = """%md \n### A cluster has been created for this demo\nTo run this demo, just select the cluster `{{CLUSTER_NAME}}` from the dropdown menu ([open cluster configuration]({{ENV_URL}}/#setting/clusters/{{CLUSTER_ID}}/configuration)). <br />\n*Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('{{DEMO_NAME}}')` or re-install the demo: `dbdemos.install('{{DEMO_NAME}}')`*"""
        content = content.replace("{{DEMO_NAME}}", demo_name) \
            .replace("{{ENV_URL}}", env_url) \
            .replace("{{CLUSTER_NAME}}", cluster_name) \
            .replace("{{CLUSTER_ID}}", cluster_id)
        parser.add_extra_cell(content)

    def add_extra_cell(self, html, cell_content, position = 0):
        command = {
            "version": "CommandV1",
            "subtype": "command",
            "commandType": "auto",
            "position": 1,
            "command": cell_content
        }
        raw_content, content = self.get_notebook_content(html)
        content = json.loads(urllib.parse.unquote(content))
        content["commands"].insert(position, command)
        content = urllib.parse.quote(json.dumps(content), safe="()*''")
        return html.replace(raw_content, base64.b64encode(content.encode('utf-8')).decode('utf-8'))

    def get_notebook_content(self, html):
        match = re.search(r'__DATABRICKS_NOTEBOOK_MODEL = \'(.*?)\'', html)
        raw_content = match.group(1)
        return raw_content, base64.b64decode(raw_content).decode('utf-8')