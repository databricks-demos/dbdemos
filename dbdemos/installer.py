import collections

import pkg_resources
from dbsqlclone.utils.load_dashboard import DashboardWidgetException

from dbdemos.packager import Packager

from .conf import DBClient, DemoConf, Conf, ConfTemplate, merge_dict, DemoNotebook
from .exceptions.dbdemos_exception import ClusterPermissionException, ClusterCreationException, ClusterException, \
    ExistingResourceException, FolderDeletionException, DLTNotAvailableException, DLTCreationException, DLTException, \
    FolderCreationException, TokenException
from .installer_report import InstallerReport
from .tracker import Tracker
from .notebook_parser import NotebookParser
from .installer_workflows import InstallerWorkflow
from .installer_repos import InstallerRepo
from pathlib import Path
import time
import json
import re
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import urllib
import threading

class Installer:
    def __init__(self, username = None, pat_token = None, workspace_url = None, cloud = "AWS", org_id: str = None, current_cluster_id: str = None):
        self.cloud = cloud
        self.dbutils = None
        if username is None:
            username = self.get_current_username()
        if workspace_url is None:
            workspace_url = self.get_current_url()
        if pat_token is None:
            pat_token = self.get_current_pat_token()
        if org_id is None:
            org_id = self.get_org_id()
        self.current_cluster_id = current_cluster_id
        if self.current_cluster_id is None:
            self.current_cluster_id = self.get_current_cluster_id()
        conf = Conf(username, workspace_url, org_id, pat_token)
        self.tracker = Tracker(org_id, self.get_uid())
        self.db = DBClient(conf)
        self.report = InstallerReport(self.db.conf.workspace_url)
        self.installer_workflow = InstallerWorkflow(self)
        self.installer_repo = InstallerRepo(self)
        #Slows down on GCP as the dashboard API is very sensitive to back-pressure
        # 1 dashboard at a time to reduce import pression as it seems to be creating new errors.
        self.max_workers = 1 if self.get_current_cloud() == "GCP" else 1


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
            try:
                return "https://"+self.get_dbutils_tags_safe()['browserHostName']
            except:
                return "local"
    def get_dbutils_tags_safe(self):
        import json
        return json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())['attributes']

    def get_current_cluster_id(self):
        try:
            return self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().tags().apply('clusterId')
        except:
            try:
                return self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().clusterId().get()
            except:
                try:
                    return self.get_dbutils_tags_safe()['clusterId']
                except:
                    return "local"

    def get_org_id(self):
        try:
            return self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().tags().apply('orgId')
        except:
            try:
                return self.get_dbutils_tags_safe()['orgId']
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
            try:
                current_notebook = self.get_dbutils_tags_safe()['notebook_path']
                return current_notebook[:current_notebook.rfind("/")]
            except:
                return "local"
    def get_workspace_id(self):
        try:
            return self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().workspaceId().get()
        except:
            try:
                return self.get_dbutils_tags_safe()['orgId']
            except:
                return "local"
    def get_current_pat_token(self):
        try:
            token = self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        except Exception as e:
            raise TokenException("Couldn't get a PAT Token: "+str(e))
        if len(token) == 0:
            raise TokenException("Empty PAT Token.")
        return token

    def get_current_username(self):
        try:
            return self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
        except Exception as e2:
            try:
                return self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().userName().get()
            except Exception as e:
                try:
                    return self.get_dbutils_tags_safe()['user']
                except:
                    print(f"WARN: couldn't get current username. This shouldn't happen - unpredictable behavior - 2 errors: {e2} - {e} - will return 'unknown'")
                    return "unknown"

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
        demos = collections.defaultdict(lambda: [])
        #Define category order
        demos["lakehouse"] = []
        demo_availables = self.get_demos_available()
        if demo_name not in demo_availables:
            for demo in demo_availables:
                conf = self.get_demo_conf(demo)
                demos[conf.category].append(conf)
            self.report.display_demo_name_error(demo_name, demos)

    def get_demos_available(self):
        return set(pkg_resources.resource_listdir("dbdemos", "bundles"))

    def get_demo_conf(self, demo_name:str, catalog:str = None, schema:str = None, demo_folder: str = ""):
        demo = self.get_resource(f"bundles/{demo_name}/conf.json")
        raw_demo = json.loads(demo)
        catalog = catalog if catalog is not None else raw_demo.get('default_catalog', None)
        schema = schema if schema is not None else raw_demo.get('default_schema', None)
        conf_template = ConfTemplate(self.db.conf.username, demo_name, catalog, schema, demo_folder)
        return DemoConf(demo_name, json.loads(conf_template.replace_template_key(demo)), catalog, schema)

    def get_resource(self, path):
        return pkg_resources.resource_string("dbdemos", path).decode('UTF-8')

    def test_premium_pricing(self):
        try:
            w = self.db.get("2.0/sql/config/warehouses", {"limit": 1})
            if "error_code" in w and (w["error_code"] == "FEATURE_DISABLED" or w["error_code"] == "ENDPOINT_NOT_FOUND"):
                self.report.display_non_premium_warn(Exception(f"DBSQL not available, either at workspace level or user entitlement."), w)
                return False
            return True
        except Exception as e:
            print(e)
            self.report.display_non_premium_warn(Exception(f"DBSQL not available"), str(e))
            return False


    def install_demo(self, demo_name, install_path, overwrite=False, update_cluster_if_exists = True, skip_dashboards = False, start_cluster = True, use_current_cluster = False, debug = False, catalog = None, schema = None):
        # first get the demo conf.
        if install_path is None:
            install_path = self.get_current_folder()
        elif install_path.startswith("./"):
            install_path = self.get_current_folder()+"/"+install_path[2:]
        elif not install_path.startswith("/"):
            install_path = self.get_current_folder()+"/"+install_path
        if install_path.endswith("/"):
            install_path = install_path[:-1]
        self.check_demo_name(demo_name)
        demo_conf = self.get_demo_conf(demo_name, catalog, schema, install_path+"/"+demo_name)
        if (schema is not  None or catalog is not None) and not demo_conf.custom_schema_supported:
            self.report.display_custom_schema_not_supported_error(Exception('Custom schema not supported'), demo_conf)
        if (schema is not None and catalog is None) or (schema is None and catalog is not None):
            self.report.display_custom_schema_missing_error(Exception('Catalog and Schema must both be defined.'), demo_conf)

        self.report.display_install_info(demo_conf, install_path, catalog, schema)
        self.tracker.track_install(demo_conf.category, demo_name)
        use_cluster_id = self.current_cluster_id if use_current_cluster else None
        try:
            cluster_id, cluster_name = self.load_demo_cluster(demo_name, demo_conf, update_cluster_if_exists, start_cluster, use_cluster_id)
        except ClusterException as e:
            self.report.display_cluster_creation_error(e, demo_conf)
        pipeline_ids = self.load_demo_pipelines(demo_name, demo_conf, debug)
        dashboards = [] if skip_dashboards else self.install_dashboards(demo_conf, install_path, debug)
        repos = self.installer_repo.install_repos(demo_conf, debug)
        workflows = self.installer_workflow.install_workflows(demo_conf, use_cluster_id, debug)
        init_job = self.installer_workflow.create_demo_init_job(demo_conf, use_cluster_id, debug)
        all_workflows = workflows if init_job["id"] is None else workflows + [init_job]
        notebooks = self.install_notebooks(demo_name, install_path, demo_conf, cluster_name, cluster_id, pipeline_ids, dashboards, all_workflows, repos, overwrite, use_current_cluster, debug)
        self.installer_workflow.start_demo_init_job(init_job, debug)
        for pipeline in pipeline_ids:
            if "run_after_creation" in pipeline and pipeline["run_after_creation"]:
                self.db.post(f"2.0/pipelines/{pipeline['uid']}/updates", { "full_refresh": True })

        self.report.display_install_result(demo_name, demo_conf.description, demo_conf.title, install_path, notebooks, init_job['uid'], init_job['run_id'], cluster_id, cluster_name, pipeline_ids, dashboards, workflows)

    def install_dashboards(self, demo_conf: DemoConf, install_path, debug=True):
        if "dashboards" in pkg_resources.resource_listdir("dbdemos", "bundles/"+demo_conf.name):
            if debug:
                print(f'    Installing dashboards')
            def install_dash(dashboard):
                return self.install_dashboard(demo_conf, install_path, dashboard, debug)
            dashboards = pkg_resources.resource_listdir("dbdemos", "bundles/" + demo_conf.name + "/dashboards")
            #filter out new import/export api:
            dashboards = filter(lambda n: Packager.DASHBOARD_IMPORT_API not in n, dashboards)
            try:
                # Parallelize dashboard install, 3 by 3. Not on GCP as it's failing
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    return [n for n in executor.map(install_dash, dashboards)]
            except DashboardWidgetException as e:
                self.report.display_dashboard_widget_exception(e, demo_conf)
            except Exception as e:
                self.report.display_dashboard_error(e, demo_conf)
        return []

    def replace_dashboard_schema(self, demo_conf: DemoConf, definition: str):
        #main__build is used during the build process to avoid collision with default main.
        definition = definition.replace("main__build.", f"main.")
        if demo_conf.custom_schema_supported:
            return definition.replace(demo_conf.default_catalog+"."+demo_conf.default_schema, f"`{demo_conf.catalog}`.`{demo_conf.schema}`")
        return definition

    def install_dashboard(self, demo_conf, install_path, dashboard, debug=True):
        definition = self.get_resource("bundles/" + demo_conf.name + "/dashboards/" + dashboard)
        definition = self.replace_dashboard_schema(demo_conf, definition)
        definition = json.loads(definition)
        id = dashboard[:dashboard.rfind(".json")]
        dashboard_name = definition['dashboard']['name']
        dashboard_name = dashboard_name + " - " + self.db.conf.name
        definition['dashboard']['name'] = dashboard_name

        if debug:
            print(f"     Installing dashboard {dashboard_name} - {id}...")
        from dbsqlclone.utils import dump_dashboard
        from dbsqlclone.utils.client import Client
        client = Client(self.db.conf.workspace_url, self.db.conf.pat_token)
        existing_dashboard = self.get_dashboard_by_name(dashboard_name)
        if existing_dashboard is not None:
            existing_dashboard = dump_dashboard.get_dashboard_definition_by_id(client, existing_dashboard['id'])
            # If we can't change the ownership, we'll have to create a new dashboard.
            could_change_owner_changed = self.change_dashboard_ownership(existing_dashboard)
            # Can't change ownership, we won't be able to override the dashboard. Create a new one with your name.
            if not could_change_owner_changed:
                #definition['dashboard']['name'] = dashboard_name
                print(
                    f"WARNING:  Could not change dashboard ownership {dashboard_name}. Import might fail. Check dashboard {self.db.conf.workspace_url}/sql/dashboards/{existing_dashboard['id']}")
        # Create the folder where to save the queries
        path = f'{install_path}/dbdemos_dashboards/{demo_conf.name}'
        f = self.db.post("2.0/workspace/mkdirs", {"path": path})
        if "error_code" in f:
            raise Exception(f"ERROR - wrong install path, can't save dashboard here: {f}")
        folders = self.db.get("2.0/workspace/list", {"path": Path(path).parent.absolute()})
        if "error_code" in folders:
            raise Exception(f"ERROR - wrong install path, can't save dashboard here: {folders}")
        parent_folder_id = None
        for f in folders["objects"]:
            if f["object_type"] == "DIRECTORY" and f["path"] == path:
                parent_folder_id = f["object_id"]
        if parent_folder_id is None:
            raise Exception(f"ERROR: couldn't find dashboard folder {path}. Do you have permission?")

        # ------- LEGACY IMPORT/EXPORT DASHBOARD - TO BE REPLACED ONCE IMPORT/EXPORT API IS PUBLIC PREVIEW
        from dbsqlclone.utils.client import Client
        from dbsqlclone.utils import load_dashboard, clone_dashboard
        client = Client(self.db.conf.workspace_url, self.db.conf.pat_token, permissions= [
            {"user_name": self.db.conf.username, "permission_level": "CAN_MANAGE"},
            {"group_name": "users", "permission_level": "CAN_EDIT"}
        ])
        try:
            endpoint = self.get_or_create_endpoint(self.db.conf.name)
            if endpoint is None:
                print(
                    "WARN: couldn't create or get a SQL endpoint for dbdemos. Do you have permission? dbdemos will continue trying to import the dashboard without endoint (import will pick the first available if any)")
            else:
                client.data_source_id = endpoint['id']
                client.endpoint_id = endpoint['warehouse_id']
            if existing_dashboard is not None:
                load_dashboard.clone_dashboard_without_saved_state(definition, client, existing_dashboard['id'], parent=f'folders/{parent_folder_id}')
                return {"id": id, "name": dashboard_name, "installed_id": existing_dashboard['id']}
            else:
                state = load_dashboard.clone_dashboard(definition, client, {}, parent=f'folders/{parent_folder_id}')
                return {"id": id, "name": dashboard_name, "installed_id": state["new_id"]}
        except Exception as e:
            print(f"    ERROR loading dashboard {dashboard_name} - {str(e)}")
            raise e

        """
        # ------- NEW IMPORT/EXPORT API
        data = {
            'import_file_contents': definition,
            'parent': f'folders/{parent_folder_id}'
        }
        endpoint_id = self.get_or_create_endpoint()
        if endpoint_id is None:
            print(
                "ERROR: couldn't create or get a SQL endpoint for dbdemos. Do you have permission? Trying to import the dashboard without endoint (import will pick the first available if any)")
        else:
            data['warehouse_id'] = endpoint_id
        if existing_dashboard is not None:
            data['overwrite_dashboard_id'] = existing_dashboard
            data['should_overwrite_existing_queries'] = True
        
        TODO: import / export public preview delayed. Instead we'll fallback to manual import/export
        i = self.db.post(f"2.0/preview/sql/dashboards/import", data)
        if "id" in i:
            # Change definition for all users to be able to use the dashboard & the queries.
            permissions = {"access_control_list": [
                {"user_name": self.db.conf.username, "permission_level": "CAN_MANAGE"},
                {"group_name": "users", "permission_level": "CAN_EDIT"}
            ]}
            permissions = self.db.post("2.0/preview/sql/permissions/dashboards/" + i["id"], permissions)
            existing_dashboard_definition = self.db.get(f"2.0/preview/sql/dashboards/{existing_dashboard}/export")
            if "queries" in existing_dashboard_definition:
                for q in existing_dashboard_definition["queries"]:
                    self.db.post(f"2.0/preview/sql/permissions/query/{q['id']}", permissions)
            print(f"     Dashboard {definition['dashboard']['name']} installed. Permissions set to {permissions}")
            self.db.post("2.0/preview/sql/dashboards/" + i["id"], {"run_as_role": "viewer"})
            return {"id": id, "name": definition['dashboard']['name'], "installed_id": i["id"]}
        else:
            print(f"    ERROR loading dashboard {definition['dashboard']['name']}: {i}, {existing_dashboard}")
            return {"id": id, "name": definition['dashboard']['name'], "error": i, "installed_id": existing_dashboard}"""

    # Try to change ownership to be able to override the dashboard. Only admin can override ownership.
    # Return True if we've been able to change ownership, false otherwise
    def change_dashboard_ownership(self, existing_dashboard):
        owner = self.db.post(f"2.0/preview/sql/permissions/dashboard/{existing_dashboard['id']}/transfer", {"new_owner": self.db.conf.username})
        if 'error_code' in owner or ('message' in owner and (owner['message'] != 'Success' and not owner['message'].startswith("This object already belongs"))):
            print(f"       WARN: Couldn't update ownership of dashboard {existing_dashboard['id']} to current user. Will create a new one.")
            return False
        # Get existing dashboard definition and change all its query ownership
        # TODO legacy import/export
        # existing_dashboard_definition = self.db.get(f"2.0/preview/sql/dashboards/{existing_dashboard_id}/export")
        # if "message" in existing_dashboard_definition:
        #     print(f"WARN: Error getting existing dashboard details - id {existing_dashboard['id']}: {existing_dashboard_definition}. Will create a new one.")
        #    return False
        # else:
        # Try to update all individual query ownership
        for q in existing_dashboard["queries"]:
            owner = self.db.post(f"2.0/preview/sql/permissions/query/{q['id']}/transfer", {"new_owner": self.db.conf.username})
            if 'error_code' in owner:
                print(f"       WARN: Couldn't update ownership of query {q['id']} to current user. Will create a new dashboard.")
                return False
        return True

    def get_dashboard_by_name(self, name):
        def get_dashboard(page):
            page_size = 250
            ds = self.db.get("2.0/preview/sql/dashboards", params = {"page_size": page_size, "page": page})
            for d in ds['results']:
                if d['name'] == name and 'moved_to_trash_at' not in d:
                    return d
            if len(ds["results"]) >= page_size:
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

    def get_or_create_endpoint(self, username, endpoint_name = "dbdemos-shared-endpoint"):
        ds = self.get_demo_datasource()
        if ds is not None:
            return ds
        def get_definition(serverless, name):
            return {
                "name": name,
                "cluster_size": "Small",
                "min_num_clusters": 1,
                "max_num_clusters": 1,
                "tags": {
                    "project": "dbdemos"
                },
                "spot_instance_policy": "COST_OPTIMIZED",
                "warehouse_type": "PRO",
                "enable_photon": "true",
                "enable_serverless_compute": serverless,
                "channel": { "name": "CHANNEL_NAME_CURRENT" }
            }
        def try_create_endpoint(serverless):
            w = self.db.post("2.0/sql/warehouses", json=get_definition(serverless, endpoint_name))
            if "message" in w and "already exists" in w['message']:
                w = self.db.post("2.0/sql/warehouses", json=get_definition(serverless, endpoint_name+"-"+username))
            if "id" in w:
                return w
            if serverless:
                print(f"WARN: Couldn't create serverless warehouse ({endpoint_name}). Will fallback to standard SQL warehouse. Creation response: {w}")
            else:
                print(f"WARN: Couldn't create warehouse: {endpoint_name} and {endpoint_name}-{username}. Creation response: {w}. Use another warehouse to view your dashboard.")
            return None

        if try_create_endpoint(True) is None:
            #Try to fallback with classic endpoint?
            try_create_endpoint(False)
        ds = self.get_demo_datasource()
        if ds is not None:
            return ds
        print(f"ERROR: Couldn't create endpoint.")
        return None

    def install_notebooks(self, demo_name: str, install_path: str, demo_conf: DemoConf, cluster_name: str, cluster_id: str,
                          pipeline_ids, dashboards, workflows, repos, overwrite=False, use_current_cluster=False, debug=False):
        assert len(demo_name) > 4, "wrong demo name. Fail to prevent potential delete errors."
        if debug:
            print(f'    Installing notebooks')
        install_path = install_path+"/"+demo_name
        s = self.db.get("2.0/workspace/get-status", {"path": install_path})
        if 'object_type' in s:
            if not overwrite:
                self.report.display_folder_already_existing(ExistingResourceException(install_path, s), demo_conf)
            if debug:
                print(f"    Folder {install_path} already exists. Deleting the existing content...")
            assert install_path not in ['/Users', '/Repos', '/Shared'], "Demo name is missing, shouldn't happen. Fail to prevent main deletion."
            d = self.db.post("2.0/workspace/delete", {"path": install_path, 'recursive': True})
            if 'error_code' in d:
                self.report.display_folder_permission(FolderDeletionException(install_path, d), demo_conf)

        folders_created = set()
        #Avoid multiple mkdirs in parallel as it's creating error.
        folders_created_lock = threading.Lock()
        def load_notebook(notebook):
            return load_notebook_path(notebook, "bundles/"+demo_name+"/install_package/"+notebook.get_clean_path()+".html")

        def load_notebook_path(notebook: DemoNotebook, template_path):
            parser = NotebookParser(self.get_resource(template_path))
            if notebook.add_cluster_setup_cell and not use_current_cluster:
                self.add_cluster_setup_cell(parser, demo_name, cluster_name, cluster_id, self.db.conf.workspace_url)
            parser.replace_dashboard_links(dashboards)
            parser.remove_automl_result_links()
            parser.replace_schema(demo_conf)
            parser.replace_dynamic_links_pipeline(pipeline_ids)
            parser.replace_dynamic_links_repo(repos)
            parser.remove_delete_cell()
            parser.replace_dynamic_links_workflow(workflows)
            parser.set_tracker_tag(self.get_org_id(), self.get_uid(), demo_conf.category, demo_name, notebook.get_clean_path())
            content = parser.get_html()
            content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
            parent = str(Path(install_path+"/"+notebook.get_clean_path()).parent)
            with folders_created_lock:
                if parent not in folders_created:
                    r = self.db.post("2.0/workspace/mkdirs", {"path": parent})
                    folders_created.add(parent)
                    if 'error_code' in r:
                        if r['error_code'] == "RESOURCE_ALREADY_EXISTS":
                            self.report.display_folder_creation_error(FolderCreationException(install_path, r), demo_conf)
            r = self.db.post("2.0/workspace/import", {"path": install_path+"/"+notebook.get_clean_path(), "content": content, "format": "HTML"})
            if 'error_code' in r:
                self.report.display_folder_creation_error(FolderCreationException(f"{install_path}/{notebook.get_clean_path()}", r), demo_conf)
            return notebook

        #Always adds the licence notebooks
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            notebooks = [
                DemoNotebook("_resources/LICENSE", "LICENSE", "Demo License"),
                DemoNotebook("_resources/NOTICE", "NOTICE", "Demo Notice"),
                DemoNotebook("_resources/README", "README", "Readme")
            ]
            def load_notebook_template(notebook):
                load_notebook_path(notebook, f"template/{notebook.title}.html")
            collections.deque(executor.map(load_notebook_template, notebooks))
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            return [n for n in executor.map(load_notebook, demo_conf.notebooks)]

    def load_demo_pipelines(self, demo_name, demo_conf: DemoConf, debug=False):
        #default cluster conf
        pipeline_ids = []
        for pipeline in demo_conf.pipelines:
            definition = pipeline["definition"]
            today = date.today().strftime("%Y-%m-%d")
            #enforce demo tagging in the cluster
            for cluster in definition["clusters"]:
                merge_dict(cluster, {"custom_tags": {"project": "dbdemos", "demo": demo_name, "demo_install_date": today}})
                if self.db.conf.get_demo_pool() is not None:
                    cluster["instance_pool_id"] = self.db.conf.get_demo_pool()
                    if "node_type_id" in cluster: del cluster["node_type_id"]
                    if "enable_elastic_disk" in cluster: del cluster["enable_elastic_disk"]
                    if "aws_attributes" in cluster: del cluster["aws_attributes"]

            existing_pipeline = self.get_pipeline(definition["name"])
            if debug:
                print(f'    Installing pipeline {definition["name"]}')
            if existing_pipeline == None:
                p = self.db.post("2.0/pipelines", definition)
                if 'error_code' in p and p['error_code'] == 'FEATURE_DISABLED':
                    message = f'DLT pipelines are not available in this workspace. Only Premium workspaces are supported on Azure.'
                    pipeline_ids.append({"name": pipeline["definition"]["name"], "uid": "INSTALLATION_ERROR", "id": pipeline["id"], "error": True})
                    self.report.display_pipeline_error(DLTNotAvailableException(message, definition, p))
                    continue
                if 'error_code' in p:
                    pipeline_ids.append({"name": pipeline["definition"]["name"], "uid": "INSTALLATION_ERROR", "id": pipeline["id"], "error": True})
                    self.report.display_pipeline_error(DLTCreationException(f"Error creating the DLT pipeline: {p['error_code']}", definition, p))
                    continue
                id = p['pipeline_id']
            else:
                if debug:
                    print("    Updating existing pipeline with last configuration")
                id = existing_pipeline['pipeline_id']
                p = self.db.put("2.0/pipelines/"+id, definition)
                if 'error_code' in p:
                    pipeline_ids.append({"name": pipeline["definition"]["name"], "uid": "INSTALLATION_ERROR", "id": pipeline["id"], "error": True})
                    self.report.display_pipeline_error(DLTCreationException(f"Error updating the DLT pipeline {id}: {p['error_code']}", definition, p))
                    continue
            permissions = self.db.patch(f"2.0/preview/permissions/pipelines/{id}", {
                "access_control_list": [{"group_name": "users", "permission_level": "CAN_MANAGE"}]
            })
            if 'error_code' in permissions:
                print(f"WARN: Couldn't update the pipeline permission for all users to access: {permissions}. Try deleting the pipeline first?")
            pipeline_ids.append({"name": definition['name'], "uid": id, "id": pipeline["id"], "run_after_creation": pipeline["run_after_creation"]})
            #Update the demo conf tags {{}} with the actual id (to be loaded as a job for example)
            demo_conf.set_pipeline_id(pipeline["id"], id)
        return pipeline_ids

    def load_demo_cluster(self, demo_name, demo_conf: DemoConf, update_cluster_if_exists, start_cluster = True, use_cluster_id: str = None):
        if use_cluster_id is not None:
            return (use_cluster_id, "Interactive cluster you used for installation - make sure the cluster configuration matches.")
        #Do not start clusters by default in Databricks FE clusters to avoid costs as we have shared clusters for demos
        if start_cluster is None:
            start_cluster = not (self.db.conf.is_dev_env() or self.db.conf.is_fe_env())

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

        if "driver_node_type_id" in cluster_conf:
            if cloud not in cluster_conf["driver_node_type_id"] or cloud not in cluster_conf["node_type_id"]:
                raise Exception(f"""ERROR CREATING CLUSTER FOR DEMO {demo_name}. You need to speficy the cloud type for all clouds:  "node_type_id": {"AWS": "g5.4xlarge", "AZURE": "Standard_NC8as_T4_v3", "GCP": "a2-highgpu-1g"} and "driver_node_type_id" """)
            cluster_conf["node_type_id"] = cluster_conf["node_type_id"][cloud]
            cluster_conf["driver_node_type_id"] = cluster_conf["driver_node_type_id"][cloud]

        if "spark.databricks.cluster.profile" in cluster_conf["spark_conf"] and cluster_conf["spark_conf"]["spark.databricks.cluster.profile"] == "singleNode":
            del cluster_conf["autoscale"]
            cluster_conf["num_workers"] = 0

        existing_cluster = self.find_cluster(cluster_conf["cluster_name"])
        if existing_cluster is None:
            cluster = self.db.post("2.0/clusters/create", json = cluster_conf)
            if "error_code" in cluster and cluster["error_code"] == "PERMISSION_DENIED":
                raise ClusterPermissionException(f"Can't create cluster for demo {demo_name}", cluster_conf, cluster)
            if "cluster_id" not in cluster or "error_code" in cluster:
                print(f"    WARN: couldn't create the cluster for the demo: {cluster}")
                raise ClusterCreationException(f"Can't create cluster for demo {demo_name}", cluster_conf, cluster)
            else:
                cluster_conf["cluster_id"] = cluster["cluster_id"]
        else:
            cluster_conf["cluster_id"] = existing_cluster["cluster_id"]
            cluster = self.db.get("2.0/clusters/get", params = {"cluster_id": cluster_conf["cluster_id"]})
            self.wait_for_cluster_to_stop(cluster_conf, cluster)
            if update_cluster_if_exists:
                cluster = self.db.post("2.0/clusters/edit", json = cluster_conf)
                if "error_code" in cluster and cluster["error_code"] != "INVALID_STATE":
                    raise ClusterCreationException(f"couldn't edit the cluster conf for {demo_name}", cluster_conf, cluster)
                self.wait_for_cluster_to_stop(cluster_conf, cluster)

        if len(demo_conf.cluster_libraries) > 0:
            install = self.db.post("2.0/libraries/install", json = {"cluster_id": cluster_conf["cluster_id"], "libraries": demo_conf.cluster_libraries})
            if "error_code" in install:
                print(f"WARN: Couldn't install the libs: {cluster_conf}, libraries={demo_conf.cluster_libraries}")

        # Only start if the cluster already exists (it's starting by default for new cluster)
        if existing_cluster is not None and start_cluster:
            start = self.db.post("2.0/clusters/start", json = {"cluster_id": cluster_conf["cluster_id"]})
            if "error_code" in start:
                if start["error_code"] == "INVALID_STATE" and \
                        ("unexpected state Pending" in start["message"] or "unexpected state Restarting" in start["message"]):
                    print(f"INFO: looks like the cluster is already starting... full answer: {start}")
                else:
                    raise ClusterCreationException(f"Couldn't start the cluster for {demo_name}: {start['error_code']} - {start['message']}", cluster_conf, start)

        return cluster_conf['cluster_id'], cluster_conf['cluster_name']

    def wait_for_cluster_to_stop(self, cluster_conf, cluster):
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