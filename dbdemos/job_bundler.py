from .conf import DBClient, DemoConf, Conf, ConfTemplate, merge_dict
import time
import json
import re
import base64
from concurrent.futures import ThreadPoolExecutor
import collections

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
        if not self.staging_reseted:
            self.reset_staging_repo()
        print("scanning folder for bundles...")
        def find_conf_files(path, list, depth = 0):
            objects = self.db.get("2.0/workspace/list", {"path": path})["objects"]
            with ThreadPoolExecutor(max_workers=3 if depth <= 2 else 1) as executor:
                params = [(o['path'], list, depth+1) for o in objects if o['object_type'] == 'DIRECTORY']
                for r in executor.map(lambda args, f=find_conf_files: f(*args), params):
                    list.update(r)
                for o in objects:
                    if o['object_type'] == 'NOTEBOOK' and o['path'].endswith("bundle_config"):
                        list.add(o['path'])
                return list
        bundles = find_conf_files(self.conf.get_repo_path(), set())
        for b in bundles:
            self.add_bundle_from_config(b)

    def add_bundle_from_config(self, bundle_config_paths):
        #Remove the /Repos/xxx from the path (we need it from the repo root)
        path = bundle_config_paths[len(self.conf.get_repo_path()):]
        path = path[:-len("_resources/bundle_config")-1]
        print(f"add bundle under {path}")
        self.add_bundle(path)

    def add_bundle(self, bundle_path, config_path: str = "_resources/bundle_config"):
        if not self.staging_reseted:
            self.reset_staging_repo()
        #Let's get the demo conf from the demo folder.
        config_path = self.conf.get_repo_path()+"/"+bundle_path+"/"+config_path

        file = self.db.get("2.0/workspace/export", {"path": config_path, "format": "SOURCE", "direct_download": False})
        if "content" not in file:
            raise Exception(f"Couldn't download bundle file: {config_path}. Check your bundle path if you added it manualy.")
        content = base64.b64decode(file['content']).decode('ascii')
        #TODO not great, we can't download a file so need to use a notebook. We could use eval() to eval the cell but it's not super safe.
        #Need to wait for file support via api (Q3)
        lines = [l for l in content.split('\n') if not l.startswith("#") and len(l) > 0]
        j = "\n".join(lines)
        j = re.sub(r'[:\s*]True', ' true', j)
        j = re.sub(r'[:\s*]False', ' false', j)
        try:
            json_conf = json.loads(j)
        except:
            raise Exception(f"incorrect json setting for {config_path}. The cell should contain a python object. Please use double quote.\n {j}")
        demo_conf = DemoConf(bundle_path, json_conf)
        self.bundles[bundle_path] = demo_conf

    def reset_staging_repo(self, skip_pull = False):
        repo_path = self.conf.get_repo_path()
        print(f"Cloning repo { self.conf.repo_url} and pulling last content under {repo_path}...")
        repos = self.db.get("2.0/repos", {"path_prefix": repo_path})['repos']
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

    def start_and_wait_bundle_jobs(self, force_execution: bool = False, skip_execution: bool = False):
        self.create_or_update_bundle_jobs()
        self.cancel_bundle_jobs()
        self.run_bundle_jobs(force_execution, skip_execution)
        self.wait_for_bundle_jobs_completion()

    def create_or_update_bundle_jobs(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            confs = [c[1] for c in self.bundles.items()]
            def create_bundle_job(demo_conf):
                demo_conf.job_id = self.create_bundle_job(demo_conf)
            collections.deque(executor.map(create_bundle_job, confs))

    def cancel_bundle_jobs(self):
        for _, demo_conf in self.bundles.items():
            if demo_conf.job_id is not None:
                self.db.post("2.1/jobs/runs/cancel-all", {"job_id": demo_conf.job_id})

    def run_bundle_jobs(self, force_execution: bool = False, skip_execution = False):
        for _, demo_conf in self.bundles.items():
            if demo_conf.job_id is not None:
                execute = True
                if not force_execution:
                    runs = self.db.get("2.1/jobs/runs/list", {"job_id": demo_conf.job_id, 'limit': 2, 'expand_tasks': "true"})
                    #Last run was successful
                    if 'runs' in runs and len(runs['runs']) > 0:
                        run = runs['runs'][0]
                        if run["state"]["life_cycle_state"] == "TERMINATED" and run["state"]["result_state"] == "SUCCESS":
                            if skip_execution:
                                execute = False
                                demo_conf.run_id = run['run_id']
                                print("skipping job execution as it was already run and skip_execution=True.")
                            else:
                                #last run was using the same commit version.
                                tasks_with_different_commit = [t for t in run['tasks'] if t['git_source']['git_snapshot']['used_commit'] != self.head_commit_id]
                                if len(tasks_with_different_commit) == 0:
                                    execute = False
                                    demo_conf.run_id = run['run_id']
                                    print("skipping job execution as previous run is already at staging. run with force_execution=true to override this check.")
                if execute:
                    run = self.db.post("2.1/jobs/run-now", {"job_id": demo_conf.job_id})
                    demo_conf.run_id = run["run_id"]

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

    def create_bundle_job(self, demo_conf: DemoConf):
        notebooks_to_run = demo_conf.get_notebooks_to_run()
        if len(notebooks_to_run) == 0:
            return None
        else:
            #default job conf. TODO: add specific job setup per demo if required?
            conf_template = ConfTemplate(self.conf.username, demo_conf.name)
            default_job_conf = json.loads(conf_template.replace_template_key(self.conf.default_cluster_job_template))
            cluster_conf = self.get_cluster_conf(demo_conf)
            #Update the job cluster with the specific demo setup if any
            for job_cluster in default_job_conf["job_clusters"]:
                merge_dict(job_cluster["new_cluster"], cluster_conf)
                #TODO: to be improved, we use only use AWS for now
                if 'instance_pool_id':
                    job_cluster["new_cluster"].pop('node_type_id', None)
                    job_cluster["new_cluster"].pop('aws_attributes', None)
                job_cluster["new_cluster"].pop('cluster_name', None)
                job_cluster["new_cluster"].pop('autotermination_minutes', None)
                if job_cluster["new_cluster"]["spark_conf"].get("spark.databricks.cluster.profile", "") == "singleNode":
                    del job_cluster["new_cluster"]["autoscale"]
                    job_cluster["new_cluster"]["num_workers"] = 0
            default_job_conf['tasks'] = []
            for i, notebook in enumerate(notebooks_to_run):
                task = {
                    "task_key": f"bundle_{demo_conf.name}_{i}",
                    "depends_on": [
                        {
                            "task_key": f"bundle_{demo_conf.name}_{i-1}",
                        }
                    ],
                    "notebook_task": {
                        "notebook_path": demo_conf.path+"/"+notebook.path,
                        "base_parameters": {"reset_all_data": "true"},
                        "source": "GIT"
                    },
                    "job_cluster_key": default_job_conf["job_clusters"][0]["job_cluster_key"],
                    "timeout_seconds": 0,
                    "email_notifications": {}}
                merge_dict(task["notebook_task"]["base_parameters"], notebook.parameters)
                default_job_conf['tasks'].append(task)
            del default_job_conf['tasks'][0]["depends_on"]
            #TODO: need to be improved
            for i, task in enumerate(demo_conf.extra_init_task):
                task = {
                    "task_key": f"bundle_{demo_conf.name}_extra_{i}",
                    "notebook_task": {
                        "notebook_path": demo_conf.path+"/"+task["path"],
                        "base_parameters": {"reset_all_data": "true"},
                        "source": "GIT"
                    },
                    "job_cluster_key": default_job_conf["job_clusters"][0]["job_cluster_key"],
                    "timeout_seconds": 0,
                    "email_notifications": {}}
                default_job_conf['tasks'].append(task)

            return self.create_or_update_job(demo_conf, default_job_conf)

    def create_or_update_job(self, demo_conf: DemoConf, job_conf: dict):
        print(f'searching for job {job_conf["name"]}')
        existing_job = self.db.find_job(job_conf["name"])
        if existing_job is not None:
            #update the job
            print(f"test job {existing_job['job_id']} already existing for {demo_conf.name}, updating it with last config")
            self.db.post("2.1/jobs/reset", {'job_id': existing_job['job_id'], 'new_settings': job_conf})
            return existing_job['job_id']
        else:
            #create the job from scratch
            print(f"test job doesn't exist for {demo_conf.name}, creating a new one")
            r = self.db.post("2.1/jobs/create", job_conf)
            if 'job_id' not in r:
                raise Exception(f"Error starting the job for demo {demo_conf.name}: {r}. Please check your cluster/job setup {job_conf}")
            return r['job_id']