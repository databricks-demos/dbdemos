from .conf import DBClient, DemoConf, Conf, ConfTemplate, merge_dict
import time
import json
import re
import base64
from concurrent.futures import ThreadPoolExecutor
import collections
import requests

class JobBundler:
    def __init__(self, conf: Conf):
        self.bundles = {}
        self.staging_reseted = False
        self.head_commit_id = None
        self.conf = conf
        self.db = DBClient(conf)

    def get_cluster_conf(self, demo_conf: DemoConf):
        conf_template = ConfTemplate(self.conf.username, demo_conf.name)
        #default conf
        cluster_conf = json.loads(conf_template.replace_template_key(self.conf.default_cluster_template))
        #demo specific
        demo_cluster_conf = json.loads(conf_template.replace_template_key(json.dumps(demo_conf.cluster)))
        merge_dict(cluster_conf, demo_cluster_conf)
        return cluster_conf

    def load_bundles_conf(self):
        #if not self.staging_reseted:
        #    self.reset_staging_repo()
        print("scanning folder for bundles...")
        from threading import Lock

        bundle_set = set()
        bundle_lock = Lock()

        def find_conf_files(path, depth = 0):
            objects = self.db.get("2.0/workspace/list", {"path": path})
            if "objects" not in objects:
                return
            objects = objects["objects"]
            with ThreadPoolExecutor(max_workers=3 if depth <= 2 else 1) as executor:
                params = [(o['path'], depth+1) for o in objects if o['object_type'] == 'DIRECTORY']
                for _ in executor.map(lambda args, f=find_conf_files: f(*args), params):
                    pass
                for o in objects:
                    if o['object_type'] == 'NOTEBOOK' and o['path'].endswith("/bundle_config"):
                        with bundle_lock:
                            bundle_set.add(o['path'])

        find_conf_files(self.conf.get_repo_path())
        with ThreadPoolExecutor(max_workers=5) as executor:
            collections.deque(executor.map(self.add_bundle_from_config, bundle_set))

    def add_bundle_from_config(self, bundle_config_paths):
        #Remove the /Repos/xxx from the path (we need it from the repo root)
        path = bundle_config_paths[len(self.conf.get_repo_path()):]
        path = path[:-len("_resources/bundle_config")-1]
        print(f"add bundle under {path}")
        if "xxxxx" in path:
            print("WARNING --------------------------------------------------------------------------------")
            print(f"TEMPORARY DISABLING CV DEMOS - {path}")
            print("WARNING --------------------------------------------------------------------------------")
        else:
            self.add_bundle(path)

    def add_bundle(self, bundle_path, config_path: str = "_resources/bundle_config"):
        if not self.staging_reseted:
            self.reset_staging_repo()
        #Let's get the demo conf from the demo folder.
        config_path = self.conf.get_repo_path()+"/"+bundle_path+"/"+config_path

        file = self.db.get("2.0/workspace/export", {"path": config_path, "format": "SOURCE", "direct_download": False})
        if "content" not in file:
            raise Exception(f"Couldn't download bundle file: {config_path}. Check your bundle path if you added it manualy.")
        content = base64.b64decode(file['content']).decode('utf8')
        #TODO not great, we can't download a file so need to use a notebook. We could use eval() to eval the cell but it's not super safe.
        #Need to wait for file support via api (Q3)
        lines = [l for l in content.split('\n') if not l.startswith("#") and len(l) > 0]
        j = "\n".join(lines)
        j = re.sub(r'[:\s*]True', ' true', j)
        j = re.sub(r'[:\s*]False', ' false', j)
        try:
            json_conf = json.loads(j)
        except Exception as e:
            raise Exception(f"incorrect json setting for {config_path}: {e}. The cell should contain a python object. Please use double quote.\n {j}")
        demo_conf = DemoConf(bundle_path, json_conf)
        self.bundles[bundle_path] = demo_conf

    def reset_staging_repo(self, skip_pull = False):
        repo_path = self.conf.get_repo_path()
        print(f"Cloning repo { self.conf.repo_url} and pulling last content under {repo_path}...")
        repos = self.db.get("2.0/repos", {"path_prefix": repo_path})
        print(repos)
        repos = repos['repos']
        if len(repos) == 0:
            print(f"creating repo under {repo_path}")
            self.db.post("2.0/repos", {"url": self.conf.repo_url, "provider": "gitHub", "path": repo_path})
            repos = self.db.get("2.0/repos", {"path_prefix": repo_path})['repos']
        if skip_pull:
            self.head_commit_id = repos[0]['head_commit_id']
        else:
            print(f"Pulling last content from branch {self.conf.branch}")
            r = self.db.patch(f"2.0/repos/{repos[0]['id']}", {"branch": self.conf.branch})
            if 'error_code' in r :
                raise Exception(f"Couldn't pull the repo: {r}. Please solve conflict or delete repo before.")
            self.head_commit_id = r['head_commit_id']
        self.staging_reseted = True

    def start_and_wait_bundle_jobs(self, force_execution: bool = False, skip_execution: bool = False, recreate_jobs: bool = False):
        self.create_or_update_bundle_jobs(recreate_jobs)
        self.run_bundle_jobs(force_execution, skip_execution)
        self.wait_for_bundle_jobs_completion()

    def create_or_update_bundle_jobs(self, recreate_jobs: bool = False):
        with ThreadPoolExecutor(max_workers=5) as executor:
            confs = [c[1] for c in self.bundles.items()]
            def create_bundle_job(demo_conf):
                demo_conf.job_id = self.create_bundle_job(demo_conf, recreate_jobs)
            collections.deque(executor.map(create_bundle_job, confs))

    def get_head_commit(self):
        owner, repo = self.conf.repo_url.split('/')[-2:]
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {self.conf.github_token}"
        }
        # Get the latest commit (head) from the default branch
        print("~~~~~~~~~~", f"https://api.github.com/repos/{owner}/{repo}/commits/HEAD", "~~~~~~~~~~")
        response = requests.get(f"https://api.github.com/repos/{owner}/{repo}/commits/HEAD", headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error fetching head commit: {response.status_code}, {response.text}")
        return response.json()['sha']
    
    def run_bundle_jobs(self, force_execution: bool = False, skip_execution = False):
        head_commit = self.get_head_commit()
        with ThreadPoolExecutor(max_workers=5) as executor:
            def run_job(demo_conf):
                if demo_conf.job_id is not None:
                    execute = True
                    runs = self.db.get("2.1/jobs/runs/list", {"job_id": demo_conf.job_id, 'limit': 2, 'expand_tasks': "true"})
                    #Last run was successful
                    if 'runs' in runs and len(runs['runs']) > 0:
                        run = runs['runs'][0]
                        if run["status"]["state"] != "TERMINATED":
                            run = self.cancel_job_run(demo_conf, run)
                        if not force_execution:
                            if "termination_details" not in run["status"]:
                                raise Exception(f"termination_details missing, should not happen. Job {demo_conf.name} status is {run['status']}")
                            elif run["status"]["termination_details"]["code"] == "SUCCESS":
                                print(f"Job {demo_conf.name} status is {run['status']['termination_details']}...")
                                if skip_execution:
                                    execute = False
                                    demo_conf.run_id = run['run_id']
                                    print(f"skipping job execution {demo_conf.name} as it was already run and skip_execution=True.")
                                else:
                                    #last run was using the same commit version.
                                    most_recent_commit = ''
                                    for task in run['tasks']:
                                        task_commit = task['git_source']['git_snapshot'].get('used_commit', '')
                                        if task_commit > most_recent_commit:
                                            most_recent_commit = task_commit
                                    if not self.check_if_demo_file_changed_since_commit(demo_conf, most_recent_commit, head_commit) and most_recent_commit != '':
                                        execute = False
                                        demo_conf.run_id = run['run_id']
                                        print(f"skipping job execution for {demo_conf.name} as no files changed since last run. run with force_execution=true to override this check.")
                                    
                    if execute:
                        run = self.db.post("2.1/jobs/run-now", {"job_id": demo_conf.job_id})
                        demo_conf.run_id = run["run_id"]

            collections.deque(executor.map(run_job, [c[1] for c in self.bundles.items()]))

    def wait_for_bundle_jobs_completion(self):
        for _, demo_conf in self.bundles.items():
            if demo_conf.run_id is not None:
                self.wait_for_bundle_job_completion(demo_conf)

    def wait_for_bundle_job_completion(self, demo_conf: DemoConf):
        if demo_conf.run_id is not None:
            i = 0
            while self.db.get("2.1/jobs/runs/get", {"run_id": demo_conf.run_id})["state"]["life_cycle_state"] == "RUNNING":
                if i % 200 == 0:
                    print(f"Waiting for {demo_conf.get_job_name()} completion... "
                          f"{self.conf.workspace_url}/#job/{demo_conf.job_id}/run/{demo_conf.run_id}")
                i += 1
                time.sleep(5)
    import json

    def create_bundle_job(self, demo_conf: DemoConf, recreate_jobs: bool = False):
        notebooks_to_run = demo_conf.get_notebooks_to_run()
        if len(notebooks_to_run) == 0:
            return None
        else:
            #default job conf. TODO: add specific job setup per demo if required?
            conf_template = ConfTemplate(self.conf.username, demo_conf.name)
            default_job_conf = json.loads(conf_template.replace_template_key(self.conf.default_cluster_job_template))
            default_job_conf["git_source"]["git_url"] = self.conf.repo_url
            default_job_conf["git_source"]["git_branch"] = self.conf.branch

            cluster_conf = self.get_cluster_conf(demo_conf)
            #Update the job cluster with the specific demo setup if any
            for job_cluster in default_job_conf["job_clusters"]:
                merge_dict(job_cluster["new_cluster"], cluster_conf)
                job_cluster["new_cluster"]["single_user_name"] = self.conf.run_test_as_username
                # Custom instance (ex: gpu), not i3, remove the pool
                # expected format: {"AWS": "g5.4xlarge", "AZURE": "Standard_NC8as_T4_v3", "GCP": "a2-highgpu-1g"}
                if "node_type_id" in job_cluster["new_cluster"] and "AWS" in job_cluster["new_cluster"]["node_type_id"]:
                    job_cluster["new_cluster"].pop('instance_pool_id', None)
                    job_cluster["new_cluster"]["node_type_id"] = job_cluster["new_cluster"]["node_type_id"]["AWS"]
                    job_cluster["new_cluster"]["driver_node_type_id"] = job_cluster["new_cluster"]["driver_node_type_id"]["AWS"]
                elif self.db.conf.get_demo_pool() is not None:
                    job_cluster["new_cluster"]["instance_pool_id"] = self.db.conf.get_demo_pool()
                    job_cluster["new_cluster"].pop("node_type_id", None)
                    job_cluster["new_cluster"].pop("enable_elastic_disk", None)
                    job_cluster["new_cluster"].pop("aws_attributes", None)
                elif 'instance_pool_id' in job_cluster["new_cluster"]:
                    job_cluster["new_cluster"].pop('node_type_id', None)
                    job_cluster["new_cluster"].pop("enable_elastic_disk", None)
                    job_cluster["new_cluster"].pop("aws_attributes", None)

                job_cluster["new_cluster"].pop('cluster_name', None)
                job_cluster["new_cluster"].pop('autotermination_minutes', None)
                if job_cluster["new_cluster"]["spark_conf"].get("spark.databricks.cluster.profile", "") == "singleNode":
                    del job_cluster["new_cluster"]["autoscale"]
                    job_cluster["new_cluster"]["num_workers"] = 0
            default_job_conf['tasks'] = []

            for i, notebook in enumerate(notebooks_to_run):
                task = {
                    "task_key": f"bundle_{demo_conf.name}_{i}",
                    "notebook_task": {
                        "notebook_path": demo_conf.path+"/"+notebook.path,
                        "base_parameters": {"reset_all_data": "false"},
                        "source": "GIT"
                    },
                    "libraries": notebook.libraries,
                    "job_cluster_key": default_job_conf["job_clusters"][0]["job_cluster_key"],
                    "timeout_seconds": 0,
                    "email_notifications": {}}
                merge_dict(task["notebook_task"]["base_parameters"], notebook.parameters)
                if notebook.warehouse_id:
                    del task["job_cluster_key"]
                    task["notebook_task"]["warehouse_id"] = notebook.warehouse_id
                if notebook.depends_on_previous:
                    task["depends_on"] = [{"task_key": f"bundle_{demo_conf.name}_{i-1}"}]
                default_job_conf['tasks'].append(task)
            if "depends_on" in default_job_conf['tasks'][0]:
                del default_job_conf['tasks'][0]["depends_on"]

            return self.create_or_update_job(demo_conf, default_job_conf, recreate_jobs)

    def create_or_update_job(self, demo_conf: DemoConf, job_conf: dict, recreate_jobs: bool = False):
        print(f'searching for job {job_conf["name"]}')
        existing_job = self.db.find_job(job_conf["name"])
        if recreate_jobs:
            self.db.post("2.1/jobs/delete", {'job_id': existing_job['job_id']})
            existing_job = None
        if existing_job is not None:
            # update the job
            print(f"test job {existing_job['job_id']} already existing for {demo_conf.name}, updating it with last config")
            self.db.post("2.1/jobs/reset", {'job_id': existing_job['job_id'], 'new_settings': job_conf})
            return existing_job['job_id']
        else:
            # create the job from scratch
            print(f"test job doesn't exist for {demo_conf.name}, creating a new one")
            r = self.db.post("2.1/jobs/creatze", job_conf)
            if 'job_id' not in r:
                raise Exception(f"Error starting the job for demo {demo_conf.name}: {r}. Please check your cluster/job setup {job_conf}")
            return r['job_id']

    def check_if_demo_file_changed_since_commit(self, demo_conf: DemoConf, base_commit, last_commit = None):
        if base_commit is None or base_commit == '':
            return True
        owner, repo = self.conf.repo_url.split('/')[-2:]
        files = self.get_changed_files_since_commit(owner, repo, base_commit, last_commit)
        return any(f.startswith(demo_conf.path) for f in files)

    def get_changed_files_since_commit(self, owner, repo, base_commit, last_commit = None):
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {self.conf.github_token}"
        }

        if last_commit is None:
            last_commit = ""

        # Compare the base commit with the latest commit
        compare_url = f"https://api.github.com/repos/{owner}/{repo}/compare/{base_commit}...{last_commit}"
        compare_response = requests.get(compare_url, headers=headers)

        if compare_response.status_code == 200:
            data = compare_response.json()
            files = data['files']
            return [file['filename'] for file in files]
        else:
            raise Exception(f"Error fetching latest commit: {compare_response.status_code}, {compare_response.text}")

    def cancel_job_run(self, demo_conf: DemoConf, run):
        """Cancel a running job and wait for termination"""
        print(f"Job {demo_conf.name} status is {run['status']['state']}, cancelling it...")   
        self.db.post("2.1/jobs/runs/cancel-all", {"job_id": demo_conf.job_id})
        time.sleep(5)
        while True:
            run = self.db.get("2.1/jobs/runs/get", {"run_id": run['run_id']})
            if run["status"]["state"] == "TERMINATED":
                break
            print(f"Waiting for job {demo_conf.name} to be terminated after cancellation...")   
            time.sleep(10)
        return run

  
