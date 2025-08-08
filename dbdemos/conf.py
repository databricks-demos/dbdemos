import json
from pathlib import Path
from typing import List
import requests
import urllib
from datetime import date
import re
import threading

from requests import Response


def merge_dict(a, b, path=None, override = True):
    """merges dict b into a. Mutate a"""
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dict(a[key], b[key], path + [str(key)])
            elif override:
                a[key] = b[key]
        else:
            a[key] = b[key]

class Conf():
    def __init__(self, username: str, workspace_url: str, org_id: str, pat_token: str, default_cluster_template: str = None, default_cluster_job_template = None,
                 repo_staging_path: str = None, repo_name: str = None, repo_url: str = None, branch: str = "master", github_token = None, run_test_as_username="quentin.ambard@databricks.com"):
        self.username = username
        name = self.username[:self.username.rfind('@')]
        self.name = re.sub("[^A-Za-z0-9]", '_', name)
        self.workspace_url = workspace_url
        self.org_id = org_id
        self.pat_token = pat_token
        self.headers = {"Authorization": "Bearer " + pat_token, 'Content-type': 'application/json', 'User-Agent': 'dbdemos'}
        self.default_cluster_template = default_cluster_template
        self.default_cluster_job_template = default_cluster_job_template
        self.repo_staging_path = repo_staging_path
        self.repo_name = repo_name
        assert repo_url is None or ".git" not in repo_url, "repo_url should not contain .git"
        self.repo_url = repo_url
        self.branch = branch
        self.github_token = github_token
        self.run_test_as_username = run_test_as_username

    def get_repo_path(self):
        return self.repo_staging_path+"/"+self.repo_name

    #Add internal pool id to accelerate our demos & unit tests
    def get_demo_pool(self):
        if self.org_id == "1444828305810485" or "e2-demo-field-eng" in self.workspace_url:
            return "0727-104344-hauls13-pool-uftxk0r6"
        if self.org_id == "1660015457675682" or self.is_dev_env():
            return "1025-140806-yup112-pool-yz565bma"
        if self.org_id == "5206439413157315":
            return "1010-172835-slues66-pool-7dhzc23j"
        if self.org_id == "984752964297111":
            return "1010-173019-honor44-pool-ksw4stjz"
        if self.org_id == "2556758628403379":
            return "1010-173021-dance560-pool-hl7wefwy"
        return None

    def is_dev_env(self):
        return "e2-demo-tools" in self.workspace_url or "local" in self.workspace_url

    def is_demo_env(self):
        return "e2-demo-field-eng" in self.workspace_url or "eastus2" or self.org_id in ["1444828305810485"]

    def is_fe_env(self):
        return "e2-demo-field-eng" in self.workspace_url or "eastus2" in self.workspace_url or \
                self.org_id in ["5206439413157315", "984752964297111", "local", "1444828305810485", "2556758628403379"]

class DBClient():
    def __init__(self, conf: Conf):
        self.conf = conf

    def clean_path(self, path):
        if path.startswith("http"):
            raise Exception(f"Wrong path {path}, use with api path directly (no http://xxx..xxx).")
        if path.startswith("/"):
            path = path[1:]
        if path.startswith("api/"):
            path = path[len("api/"):]
        return path

    def post(self, path: str, json: dict = {}, retry = 0):
        url = self.conf.workspace_url+"/api/"+self.clean_path(path)
        with requests.post(url, headers = self.conf.headers, json=json, timeout=60) as r:
            if r.status_code == 429 and retry < 2:
                import time
                import random
                wait_time = 15 * (retry+1) + random.randint(2*retry, 10*retry)
                print(f'WARN: hitting api request limit 429 error: {path}. Sleeping {wait_time}sec and retrying...')
                time.sleep(wait_time)
                print('Retrying call.')
                return self.post(path, json, retry+1)
            else:
                return self.get_json_result(url, r)

    def put(self, path: str, json: dict = None, data: bytes = None):
        url = self.conf.workspace_url+"/api/"+self.clean_path(path)
        headers = self.conf.headers
        if data is not None:
            files = {'file': ('file', data, 'application/octet-stream')}
            with requests.put(url, headers=headers, files=files, timeout=60) as r:
                return self.get_json_result(url, r)
        else:
            with requests.put(url, headers=headers, json=json, timeout=60) as r:
                return self.get_json_result(url, r)

    def patch(self, path: str, json: dict = {}):
        url = self.conf.workspace_url+"/api/"+self.clean_path(path)
        with requests.patch(url, headers = self.conf.headers, json=json, timeout=60) as r:
            return self.get_json_result(url, r)

    def get(self, path: str, params: dict = {}, print_auth_error = True):
        url = self.conf.workspace_url+"/api/"+self.clean_path(path)
        with requests.get(url, headers = self.conf.headers, params=params, timeout=60) as r:
            return self.get_json_result(url, r, print_auth_error)

    def delete(self, path: str, params: dict = {}):
        url = self.conf.workspace_url+"/api/"+self.clean_path(path)
        with requests.delete(url, headers = self.conf.headers, params=params, timeout=60) as r:
            return self.get_json_result(url, r)

    def get_json_result(self, url: str, r: Response, print_auth_error = True):
        if r.status_code == 403:
            if print_auth_error:
                print(f"Unauthorized call. Check your PAT token {r.text} - {r.url} - {url}")
        try:
            return r.json()
        except Exception as e:
            print(f"API CALL ERROR - can't read json. status: {r.status_code} {r.text} - URL: {url} - {e}")
            raise e

    def search_cluster(self, cluster_name: str, tags: dict):
        clusters = self.db.get("2.1/clusters/list")
        for c in clusters:
            if c['cluster_name'] == cluster_name:
                match = True
                #Check if all the tags are in the cluster conf
                for k, v in tags.items():
                    if k not in c['custom_tags'] or c['custom_tags'][k] != v:
                        match = False
                if match:
                    return c
        return None

    def find_job(self, name, offset = 0, limit = 25):
        r = self.get("2.1/jobs/list", {"limit": limit, "offset": offset, "name": urllib.parse.quote_plus(name)})
        if 'jobs' in r:
            for job in r['jobs']:
                if job["settings"]["name"] == name:
                    return job
            if r['has_more']:
                return self.find_job(name, offset+limit, limit)
        return None

class GenieRoom():
    def __init__(self, id: str, display_name: str, description: str, table_identifiers: List[str], curated_questions: List[str], instructions: str, sql_instructions: List[dict], function_names: List[str], benchmarks:List[dict]):
        self.display_name = display_name
        self.id = id
        self.description = description
        self.instructions = instructions
        self.table_identifiers = table_identifiers
        self.sql_instructions = sql_instructions
        self.curated_questions = curated_questions
        self.function_names = function_names
        self.benchmarks= benchmarks
        
class DataFolder():
    def __init__(self, source_folder: str, source_format: str, target_table_name: str = None, target_volume_folder_name: str = None, target_format: str = "delta"):
        assert target_volume_folder_name or target_table_name, "Error, data folder should either has target_table_name or target_volume_folder_name set"
        self.source_folder = source_folder
        self.source_format = source_format
        self.target_table_name = target_table_name
        self.target_format = target_format
        self.target_volume_folder_name = target_volume_folder_name

class DemoNotebook():
    def __init__(self, path: str, title: str, description: str, pre_run: bool = False, publish_on_website: bool = False,
                 add_cluster_setup_cell: bool = False, parameters: dict = {}, depends_on_previous: bool = True, libraries: list = [], warehouse_id = None, object_type = None):
        self.path = path
        self.title = title
        self.description = description
        self.pre_run = pre_run
        self.publish_on_website = publish_on_website
        self.add_cluster_setup_cell = add_cluster_setup_cell
        self.parameters = parameters
        self.depends_on_previous = depends_on_previous
        self.libraries = libraries
        self.warehouse_id = warehouse_id
        self.object_type = object_type

    def __repr__(self):
        return self.path

    def get_folder(self):
        p = Path(self.get_clean_path())
        p.parts

    def get_clean_path(self):
        #Some notebook path are relatives, like ../../demo-retail/lakehouse-retail/_resources/xxx
        # DThis function removes it and returns _resources/xxx
        p = Path(self.path)
        parent_count = p.parts.count('..')
        if parent_count > 0:
            return str(p.relative_to(*p.parts[:parent_count*2-1]))
        return self.path


    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__)

class DemoConf():
    def __init__(self, path: str, json_conf: dict, catalog:str = None, schema: str = None):
        self.json_conf = json_conf
        self.notebooks = []
        self.cluster = json_conf.get('cluster', {})
        self.cluster_libraries = json_conf.get('cluster_libraries', [])
        self.workflows = json_conf.get('workflows', [])
        self.pipelines = json_conf.get('pipelines', [])
        self.repos = json_conf.get('repos', [])
        self.serverless_supported = json_conf.get('serverless_supported', False)
        self.init_job = json_conf.get('init_job', {})
        self.job_id = None
        self.run_id = None
        if path.startswith('/'):
            path = path[1:]
        self.path = path
        self.name = json_conf['name']
        self.category = json_conf['category']
        self.title = json_conf['title']
        self.description = json_conf['description']
        self.tags = json_conf.get('tags', [])
        self.custom_schema_supported = json_conf.get('custom_schema_supported', False)
        self.schema = schema
        self.catalog = catalog
        self.default_schema = json_conf.get('default_schema', "")
        self.default_catalog = json_conf.get('default_catalog', "")
        self.custom_message = json_conf.get('custom_message', "")
        self.create_cluster = json_conf.get('create_cluster', True)
        self.dashboards = json_conf.get('dashboards', [])
        self.sql_queries = json_conf.get('sql_queries', [])
        self.bundle = json_conf.get('bundle', False)
        
        self.data_folders: List[DataFolder] = []
        for data_folder in json_conf.get('data_folders', []):
            self.data_folders.append(DataFolder(data_folder['source_folder'], data_folder['source_format'], data_folder.get('target_table_name', None), 
                                                data_folder.get('target_volume_folder', None), data_folder['target_format']))

        self.genie_rooms: List[GenieRoom] = []
        for genie_room in json_conf.get('genie_rooms', []):
            self.genie_rooms.append(GenieRoom(genie_room['id'], genie_room.get('display_name', None), genie_room.get('description', None),
                                              genie_room['table_identifiers'], genie_room.get('curated_questions', []),
                                              genie_room.get('instructions', None), genie_room.get('sql_instructions', []),
                                              genie_room.get('function_names', []),genie_room.get('benchmarks', [])))

        for n in json_conf.get('notebooks', []):
            add_cluster_setup_cell = n.get('add_cluster_setup_cell', False)
            params = n.get('parameters', {})
            depends_on_previous = n.get('depends_on_previous', True)
            libraries = n.get('libraries', [])
            warehouse_id = n.get('warehouse_id', None)
            self.notebooks.append(DemoNotebook(n['path'], n['title'], n['description'], n['pre_run'], n['publish_on_website'],
                                               add_cluster_setup_cell, params, depends_on_previous, libraries, warehouse_id, n.get('object_type', None)))

        self._notebook_lock = threading.Lock()

    def __repr__(self):
        return self.path + "("+str(self.notebooks)+")"

    def update_notebook_object_type(self, notebook: DemoNotebook, object_type: str):
        with self._notebook_lock:
            for n in self.json_conf['notebooks']:
                if n['path'] == notebook.path:
                    n['object_type'] = object_type
                    break

    def add_notebook(self, notebook):
        self.notebooks.append(notebook)
        #TODO: this isn't clean, need a better solution
        self.json_conf["notebooks"].append(notebook.__dict__)

    def set_pipeline_id(self, id, uid):
        j = json.dumps(self.init_job)
        j = j.replace("{{DYNAMIC_DLT_ID_"+id+"}}", uid)
        self.init_job = json.loads(j)
        j = json.dumps(self.workflows)
        j = j.replace("{{DYNAMIC_DLT_ID_"+id+"}}", uid)
        self.workflows = json.loads(j)

    def get_job_name(self):
        return "field-bundle_"+self.name

    def get_notebooks_to_run(self):
        return [n for n in self.notebooks if n.pre_run]

    def get_notebooks_to_publish(self):
        return [n for n in self.notebooks if n.publish_on_website]

    def get_bundle_path(self):
        return self.get_bundle_root_path() + "/install_package"

    def get_bundle_dashboard_path(self):
        return self.get_bundle_root_path() + "/dashboards"

    def get_bundle_root_path(self):
        return "dbdemos/bundles/"+self.name

    def get_minisite_path(self):
        return "dbdemos/minisite/"+self.name


class ConfTemplate:
    def __init__(self, username, demo_name, catalog = None, schema = None, demo_folder = ""):
        self.catalog = catalog
        self.schema = schema
        self.username = username
        self.demo_name = demo_name
        self.demo_folder = demo_folder

    def template_TODAY(self):
        return date.today().strftime("%Y-%m-%d")

    def template_CURRENT_USER(self):
        return self.username

    def template_CATALOG(self):
        return self.catalog

    def template_SCHEMA(self):
        return self.schema

    def template_CURRENT_USER_NAME(self):
        name = self.username[:self.username.rfind('@')]
        name = re.sub("[^A-Za-z0-9]", '_', name)
        return name

    def template_DEMO_NAME(self):
        return self.demo_name

    def template_DEMO_FOLDER(self):
        return self.demo_folder

    def template_SHARED_WAREHOUSE_ID(self):
        return self.demo_folder

    def replace_template_key(self, text: str):
        for key in set(re.findall(r'\{\{(.*?)\}\}', text)):
            if "Drift_detection" not in key: #TODO need to improve that, mlops demo has {{}} in the product like tasks.Drift_detection.values.all_violations_count
                if not key.startswith("DYNAMIC") and not key.startswith("SHARED_WAREHOUSE"):
                    func = getattr(self, f"template_{key}")
                    replacement = func()
                    text = text.replace("{{"+key+"}}", replacement)
        return text