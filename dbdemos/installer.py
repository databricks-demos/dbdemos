import collections

import pkg_resources


from .conf import DBClient, DemoConf, Conf, ConfTemplate, merge_dict, DemoNotebook
from .exceptions.dbdemos_exception import ClusterPermissionException, ClusterCreationException, ClusterException, \
    ExistingResourceException, FolderDeletionException, DLTNotAvailableException, DLTCreationException, DLTException, \
    FolderCreationException, TokenException
from .installer_report import InstallerReport
from .installer_genie import InstallerGenie
from .installer_dashboard import InstallerDashboard
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
from dbdemos.sql_query import SQLQueryExecutor
from databricks.sdk import WorkspaceClient

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
        self.tracker = Tracker(org_id, self.get_uid(), username)
        self.db = DBClient(conf)
        self.report = InstallerReport(self.db.conf.workspace_url)
        self.installer_workflow = InstallerWorkflow(self)
        self.installer_repo = InstallerRepo(self)
        self.installer_dashboard = InstallerDashboard(self)
        self.installer_genie = InstallerGenie(self)
        self.sql_query_executor = SQLQueryExecutor()
        #Slows down on GCP as the dashboard API is very sensitive to back-pressure
        # 1 dashboard at a time to reduce import pression as it seems to be creating new errors.
        self.max_workers = 1 if self.get_current_cloud() == "GCP" else 1


    def get_dbutils(self):
        if self.dbutils is None:
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
                from pyspark.dbutils import DBUtils
                self.dbutils = DBUtils(spark)
            except:
                try:
                    import IPython
                    self.dbutils = IPython.get_ipython().user_ns["dbutils"]
                except:
                    #Can't get dbutils (local run)
                    return None
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
        return json.loads(self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().safeToJson())['attributes']

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
            raise TokenException("Couldn't get a PAT Token: "+str(e)+". If you're installing it locally or from a batch, please use the pat_token='xxx' parameter instead using a secret.")
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

    def get_workspace_url(self):
        try:
            workspace_url = "https://"+self.get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
        except Exception as e:
            raise Exception("Couldn't get workspace URL: "+str(e))
        return workspace_url

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

    def get_resource(self, path, decode=True):
        resource = pkg_resources.resource_string("dbdemos", path)
        return resource.decode('UTF-8') if decode else resource
    
    def resource_isdir(self, path):
        return pkg_resources.resource_isdir("dbdemos", path)

    def test_premium_pricing(self):
        try:
            w = self.db.get("2.0/sql/config/warehouses", {"limit": 1}, print_auth_error = False)
            if "error_code" in w and (w["error_code"] == "FEATURE_DISABLED" or w["error_code"] == "ENDPOINT_NOT_FOUND"):
                self.report.display_non_premium_warn(Exception(f"DBSQL not available, either at workspace level or user entitlement."), w)
                return False
            return True
        except Exception as e:
            print(e)
            self.report.display_non_premium_warn(Exception(f"DBSQL not available"), str(e))
            return False

    def cluster_is_serverless(self):
        try:
            cluster_details = self.db.get("2.0/clusters/get", {"cluster_id": self.get_current_cluster_id()})
            return cluster_details.get("enable_serverless_compute", False)
        except Exception as e:
            print(f"Couldn't get cluster serverless status. Will consider it False. {e}")
            return False

    def create_or_check_schema(self, demo_conf: DemoConf, create_schema: bool, debug=True):
        """Create or verify schema exists based on create_schema parameter"""
        ws = WorkspaceClient(token=self.db.conf.pat_token, host=self.db.conf.workspace_url)
        try:
            catalog = ws.catalogs.get(demo_conf.catalog)
        except Exception as e:
            if create_schema:
                if debug:
                    print(f"Can't describe catalog {demo_conf.catalog}. Will now try to create it. Error: {e}")
                try:
                    print(f"Catalog {demo_conf.catalog} doesn't exist. Creating it. You can set create_schema=False to avoid catalog and schema creation, or install in another catalog with catalog=<catalog_name>.")
                    self.sql_query_executor.execute_query(ws, f"CREATE CATALOG IF NOT EXISTS {demo_conf.catalog}")
                    #note: ws.catalogs.create(demo_conf.catalog) this doesn't work properly in serverless workspaces with default storage for now (Metastore storage root URL does not exist error)
                except Exception as e:
                    self.report.display_schema_creation_error(e, demo_conf)
            else:
                self.report.display_schema_not_found_error(e, demo_conf)

        schema_full_name = f"{demo_conf.catalog}.{demo_conf.schema}"
        try:
            schema = ws.schemas.get(schema_full_name)
        except Exception as e:
            if create_schema:
                if debug:
                    print(f"Can't describe schema {schema_full_name}. Will now try to create it. Error: {e}")
                try:
                    schema = ws.schemas.create(demo_conf.schema, catalog_name=demo_conf.catalog)
                except Exception as e:
                    self.report.display_schema_creation_error(e, demo_conf)
            else:
                self.report.display_schema_not_found_error(e, demo_conf)

    def install_demo(self, demo_name, install_path, overwrite=False, update_cluster_if_exists = True, skip_dashboards = False, start_cluster = None,
                     use_current_cluster = False, debug = False, catalog = None, schema = None, serverless=False, warehouse_name = None, skip_genie_rooms=False, 
                     create_schema=True, dlt_policy_id = None, dlt_compute_settings = None):
        # first get the demo conf.
        if install_path is None:
            install_path = self.get_current_folder()
        elif install_path.startswith("./"):
            install_path = self.get_current_folder()+"/"+install_path[2:]
        elif not install_path.startswith("/"):
            install_path = self.get_current_folder()+"/"+install_path
        if install_path.endswith("/"):
            install_path = install_path[:-1]
        if serverless is None:
            serverless = self.cluster_is_serverless()
        self.check_demo_name(demo_name)
        demo_conf = self.get_demo_conf(demo_name, catalog, schema, install_path+"/"+demo_name)
        if (schema is not None or catalog is not None) and not demo_conf.custom_schema_supported:
            self.report.display_custom_schema_not_supported_error(Exception(f'Custom schema not supported for {demo_conf.name}'), demo_conf)
        if schema is None:
            schema = demo_conf.default_schema
        if catalog is None:
            catalog = demo_conf.default_catalog
        if "-" in schema or "-" in catalog:
            self.report.display_incorrect_schema_error(Exception('Please use a valid schema/catalog name.'), demo_conf)

        # Add schema validation/creation after demo_conf initialization
        if demo_conf.custom_schema_supported:
            self.create_or_check_schema(demo_conf, create_schema, debug)

        if demo_name.startswith("aibi"):
            use_current_cluster = True
        if serverless:
            use_current_cluster = True
            if not demo_conf.serverless_supported:
                self.report.display_serverless_warn(Exception('This DBDemo content is not yet updated to Serverless/Express!'), demo_conf)

        self.report.display_install_info(demo_conf, install_path, catalog, schema)
        self.tracker.track_install(demo_conf.category, demo_name)
        use_cluster_id = self.current_cluster_id if use_current_cluster else None
        try:
            cluster_id, cluster_name = self.load_demo_cluster(demo_name, demo_conf, update_cluster_if_exists, start_cluster, use_cluster_id)
        except ClusterException as e:
            #Fallback to current cluster if we can't create a cluster.
            cluster_id = self.current_cluster_id
            self.report.display_cluster_creation_warn(e, demo_conf)
            cluster_name = "Current Cluster"
        self.check_if_install_folder_exists(demo_name, install_path, demo_conf, overwrite, debug)
        pipeline_ids = self.load_demo_pipelines(demo_name, demo_conf, debug, serverless, dlt_policy_id, dlt_compute_settings)
        dashboards = [] if skip_dashboards else self.installer_dashboard.install_dashboards(demo_conf, install_path, warehouse_name, debug)
        repos = self.installer_repo.install_repos(demo_conf, debug)
        workflows = self.installer_workflow.install_workflows(demo_conf, use_cluster_id, warehouse_name, serverless, debug)
        init_job = self.installer_workflow.create_demo_init_job(demo_conf, use_cluster_id, warehouse_name, serverless, debug)
        all_workflows = workflows if init_job["id"] is None else workflows + [init_job]
        genie_rooms = self.installer_genie.install_genies(demo_conf, install_path, warehouse_name, skip_genie_rooms, debug)
        notebooks = self.install_notebooks(demo_name, install_path, demo_conf, cluster_name, cluster_id, pipeline_ids, dashboards, all_workflows, repos, overwrite, use_current_cluster, genie_rooms, debug)
        self.installer_workflow.start_demo_init_job(demo_conf, init_job, debug)
        for pipeline in pipeline_ids:
            if "run_after_creation" in pipeline and pipeline["run_after_creation"]:
                self.db.post(f"2.0/pipelines/{pipeline['uid']}/updates", { "full_refresh": True })

        self.report.display_install_result(demo_name, demo_conf.description, demo_conf.title, install_path, notebooks, init_job['uid'], init_job['run_id'], serverless, cluster_id, cluster_name, pipeline_ids, dashboards, workflows, genie_rooms)


    def get_demo_datasource(self, warehouse_name = None):
        data_sources = self.db.get("2.0/preview/sql/data_sources")
        if warehouse_name is not None:
            for source in data_sources:
                if source['name'] == warehouse_name:
                    return source
            raise Exception(f"""Error creating the dashboard: cannot find warehouse with warehouse_name='{warehouse_name}' to load your dashboards. Use a different name and make sure the endpoint exists.""")
        
        for source in data_sources:
            if source['name'] == "dbdemos-shared-endpoint":
                return source
        #Try to fallback to an existing shared endpoint.
        for source in data_sources:
            #Default serverless warehouse in express workspaces
            if "serverless starter warehouse" in source['name'].lower():
                return source
        for source in data_sources:
            if "shared-sql-endpoint" in source['name'].lower():
                return source
        for source in data_sources:
            if "shared" in source['name'].lower():
                return source
        return None

    def get_or_create_endpoint(self, username: str, demo_conf: DemoConf, default_endpoint_name: str ="dbdemos-shared-endpoint", warehouse_name: str = None, throw_error: bool = False):
        try:
            ds = self.get_demo_datasource(warehouse_name)
        except Exception as e:
            self.report.display_unknow_warehouse_error(e, demo_conf, warehouse_name)
    
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
            w = self.db.post("2.0/sql/warehouses", json=get_definition(serverless, default_endpoint_name))
            if "message" in w and "already exists" in w['message']:
                w = self.db.post("2.0/sql/warehouses", json=get_definition(serverless, default_endpoint_name + "-" + username))
            if "id" in w:
                return w
            if serverless:
                print(f"WARN: Couldn't create serverless warehouse ({default_endpoint_name}). Will fallback to standard SQL warehouse. Creation response: {w}")
            else:
                print(f"WARN: Couldn't create warehouse: {default_endpoint_name} and {default_endpoint_name}-{username}. Creation response: {w}. Use another warehouse to view your dashboard.")
            return None

        if try_create_endpoint(True) is None:
            #Try to fallback with classic endpoint?
            try_create_endpoint(False)
        ds = self.get_demo_datasource()
        if ds is not None:
            return ds
        print(f"ERROR: Couldn't create endpoint. Use the option warehouse_name={warehouse_name} to specify a different warehouse during the installation.")
        if throw_error:
            self.report.display_warehouse_creation_error(Exception("Couldn't create endpoint - see WARNINGS for more details."), demo_conf)
        return None

    #Check if the folder already exists, and delete it if needed.
    def check_if_install_folder_exists(self, demo_name: str, install_path: str, demo_conf: DemoConf, overwrite=False, debug=False):
        install_path = install_path+"/"+demo_name
        s = self.db.get("2.0/workspace/get-status", {"path": install_path})
        if 'object_type' in s:
            if not overwrite:
                self.report.display_folder_already_existing(ExistingResourceException(install_path, s), demo_conf)
            if debug:
                print(f"    Folder {install_path} already exists. Deleting the existing content...")
            assert install_path.lower() not in ['/users', '/repos', '/shared', '/workspace', '/workspace/shared', '/workspace/users'],\
                "Demo name is missing, shouldn't happen. Fail to prevent main deletion."
            d = self.db.post("2.0/workspace/delete", {"path": install_path, 'recursive': True})
            if 'error_code' in d:
                self.report.display_folder_permission(FolderDeletionException(install_path, d), demo_conf)

    def install_notebooks(self, demo_name: str, install_path: str, demo_conf: DemoConf, cluster_name: str, cluster_id: str,
                          pipeline_ids, dashboards, workflows, repos, overwrite=False, use_current_cluster=False, genie_rooms = [], debug=False):
        assert len(demo_name) > 4, "wrong demo name. Fail to prevent potential delete errors."
        if debug:
            print(f'    Installing notebooks')
        install_path = install_path+"/"+demo_name
        folders_created = set()
        #Avoid multiple mkdirs in parallel as it's creating error.
        folders_created_lock = threading.Lock()
        def load_notebook(notebook):
            return load_notebook_path(notebook, "bundles/"+demo_name+"/install_package/"+notebook.get_clean_path())

        def load_notebook_path(notebook: DemoNotebook, template_path):
            parent = str(Path(install_path+"/"+notebook.get_clean_path()).parent)
            with folders_created_lock:
                if parent not in folders_created:
                    r = self.db.post("2.0/workspace/mkdirs", {"path": parent})
                    folders_created.add(parent)
                    if 'error_code' in r:
                        if r['error_code'] == "RESOURCE_ALREADY_EXISTS":
                            self.report.display_folder_creation_error(FolderCreationException(install_path, r), demo_conf)
            if notebook.object_type == "FILE":
                file = self.get_resource(template_path, decode=False)
                file_encoded = base64.b64encode(file).decode("utf-8")
                r = self.db.post(f"2.0/workspace/import", {"path": install_path+"/"+notebook.get_clean_path(), "content": file_encoded, "format": "AUTO", "overwrite": False})
                if 'error_code' in r:
                    self.report.display_folder_creation_error(FolderCreationException(f"{install_path}/{notebook.get_clean_path()}", r), demo_conf)
            elif notebook.object_type == "DIRECTORY":
                zip_folder = self.get_resource(template_path+".zip", decode=False)
                zip_folder_encoded = base64.b64encode(zip_folder).decode("utf-8")
                r = self.db.post(f"2.0/workspace/import", {"path": install_path+"/"+notebook.get_clean_path()+".zip", "content": zip_folder_encoded, "format": "AUTO", "overwrite": False})
                if 'error_code' in r:
                    self.report.display_folder_creation_error(FolderCreationException(f"{install_path}/{notebook.get_clean_path()}", r), demo_conf)
            else:
                html = self.get_resource(template_path+".html")
                parser = NotebookParser(html)
                if notebook.add_cluster_setup_cell and not use_current_cluster:
                    self.add_cluster_setup_cell(parser, demo_name, cluster_name, cluster_id, self.db.conf.workspace_url)
                parser.replace_dynamic_links_lakeview_dashboards(dashboards)
                parser.replace_dynamic_links_genie(genie_rooms)
                parser.remove_automl_result_links()
                parser.replace_schema(demo_conf)
                parser.replace_dynamic_links_pipeline(pipeline_ids)
                parser.replace_dynamic_links_repo(repos)
                parser.remove_delete_cell()
                parser.replace_dynamic_links_workflow(workflows)
                parser.set_tracker_tag(self.get_org_id(), self.get_uid(), demo_conf.category, demo_name, notebook.get_clean_path(), self.db.conf.username)
                content = parser.get_html()
                content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
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
                load_notebook_path(notebook, f"template/{notebook.title}")
            collections.deque(executor.map(load_notebook_template, notebooks))
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            return [n for n in executor.map(load_notebook, demo_conf.notebooks)]

    def load_demo_pipelines(self, demo_name, demo_conf: DemoConf, debug=False, serverless=False, dlt_policy_id = None, dlt_compute_settings = None):
        #default cluster conf
        pipeline_ids = []
        for pipeline in demo_conf.pipelines:
            definition = pipeline["definition"]
            if "event_log" not in definition:
                definition["event_log"] = {"catalog": demo_conf.catalog, "schema": demo_conf.schema, "name": "dlt_event_log_"}
            if "target" in definition:
                definition["schema"] = definition["target"]
                del definition["target"] #target is deprecated now (https://docs.databricks.com/api/workspace/pipelines/create#schema)
            #Force channel to current due to issue with PREVIEW on serverless with python verison
            definition["channel"] = "CURRENT"
            today = date.today().strftime("%Y-%m-%d")
            #modify cluster definitions if serverless
            if serverless:
                del definition['clusters']
                definition['photon'] = True
                definition['serverless'] = True
                if dlt_policy_id is not None:
                    self.report.display_pipeline_error(DLTCreationException(f"Policy ID is not supported for serverless pipelines, {dlt_policy_id}", definition, None))
            else:
                #enforce demo tagging in the cluster
                for cluster in definition["clusters"]:
                    merge_dict(cluster, {"custom_tags": {"project": "dbdemos", "demo": demo_name, "demo_install_date": today}})
                    if dlt_policy_id is not None:
                        cluster["dlt_policy_id"] = dlt_policy_id
                    if self.db.conf.get_demo_pool() is not None:
                        cluster["instance_pool_id"] = self.db.conf.get_demo_pool()
                        if "node_type_id" in cluster: del cluster["node_type_id"]
                        if "enable_elastic_disk" in cluster: del cluster["enable_elastic_disk"]
                        if "aws_attributes" in cluster: del cluster["aws_attributes"]
                    if dlt_compute_settings is not None:
                        merge_dict(cluster, dlt_compute_settings)

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
                    if 'complete the migration' in str(p).lower() or 'CANNOT_SET_SCHEMA_FOR_EXISTING_PIPELINE' in str(p):
                        self.report.display_pipeline_error_migration(DLTCreationException(f"Please delete the existing DLT pipeline id {id} before re-installing this demo.", definition, p))
                    else:
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

    def load_demo_cluster(self, demo_name, demo_conf: DemoConf, update_cluster_if_exists, start_cluster = None, use_cluster_id = None):
        if use_cluster_id is not None:
            return (use_cluster_id, "Interactive cluster you used for installation - make sure the cluster configuration matches.")
        
        if demo_conf.create_cluster == False:
            return (None, "This demo doesn't require cluster")
        
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