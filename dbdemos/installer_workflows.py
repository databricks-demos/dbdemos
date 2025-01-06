from .conf import DemoConf, merge_dict, ConfTemplate
import json
import time

from .exceptions.dbdemos_exception import WorkflowException
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .installer import Installer


class InstallerWorkflow:
    def __init__(self, installer: 'Installer'):
        self.installer = installer
        self.db = installer.db

    #Start the init job if it exists
    def install_workflows(self, demo_conf: DemoConf, use_cluster_id = None, warehouse_name: str = None, serverless = False, debug = False):
        workflows = []
        if len(demo_conf.workflows) > 0:
            if debug:
                print(f"    Loading demo workflows")
            # We have an init jon
            for workflow in demo_conf.workflows:
                definition = workflow['definition']
                job_name = definition["settings"]["name"]
                # add cloud specific setup
                job_id, run_id = self.create_or_replace_job(demo_conf, definition, job_name, workflow['start_on_install'], use_cluster_id, warehouse_name, serverless, debug)
                # print(f"    Demo workflow available: {self.installer.db.conf.workspace_url}/#job/{job_id}/tasks")
                workflows.append({"uid": job_id, "run_id": run_id, "id": workflow['id']})
        return workflows

    #create or update the init job if it exists
    def create_demo_init_job(self, demo_conf: DemoConf, use_cluster_id = None, warehouse_name: str = None, serverless = False, debug = False):
        if "settings" in demo_conf.init_job:
            job_name = demo_conf.init_job["settings"]["name"]
            if debug:
                print(f"    Searching for existing demo initialisation job {job_name}")
            #We have an init json
            job_id, run_id = self.create_or_replace_job(demo_conf, demo_conf.init_job, job_name, False, use_cluster_id, warehouse_name, serverless, debug)
            return {"uid": job_id, "run_id": run_id, "id": "init-job"}
        return {"uid": None, "run_id": None, "id": None}

    #Start the init job if it exists.
    def start_demo_init_job(self, demo_conf: DemoConf, init_job, debug = False):
        if init_job['uid'] is not None:
            j = self.installer.db.post("2.1/jobs/run-now", {"job_id": init_job['uid']})
            if debug:
                print(f'Starting init job {init_job}: {j}')
            if "error_code" in j:
                self.installer.report.display_workflow_error(WorkflowException("Can't start the workflow", {"job_id": init_job['uid']}, init_job, j), demo_conf.name)
            init_job['run_id'] = j['run_id']
            return j['run_id']

    def create_or_replace_job(self, demo_conf: DemoConf, definition: dict,  job_name: str, run_now: bool, use_cluster_id = None, warehouse_name: str = None, serverless = False, debug = False):
        cloud = self.installer.get_current_cloud()
        conf_template = ConfTemplate(self.db.conf.username, demo_conf.name)
        cluster_conf = self.installer.get_resource("resources/default_cluster_job_config.json")
        cluster_conf = json.loads(conf_template.replace_template_key(cluster_conf))
        cluster_conf_cloud = json.loads(self.installer.get_resource(f"resources/default_cluster_config-{cloud}.json"))
        merge_dict(cluster_conf, cluster_conf_cloud)
        definition = self.replace_warehouse_id(demo_conf, definition, warehouse_name)
        #Use a given interactive cluster, change the job setting accordingly.
        if use_cluster_id is not None:
            del definition["settings"]["job_clusters"]
            for task in definition["settings"]["tasks"]:
                if "job_cluster_key" in task:
                    del task["job_cluster_key"]
                    task["existing_cluster_id"] = use_cluster_id
        #otherwise set the job properties based on the definition & add pool for our dev workspace.
        else:
            for cluster in definition["settings"]["job_clusters"]:
                if "new_cluster" in cluster:
                    merge_dict(cluster["new_cluster"], cluster_conf, override=False)
                    #Let's make sure we add our dev pool for faster startup
                    if self.db.conf.get_demo_pool() is not None:
                        cluster["new_cluster"]["instance_pool_id"] = self.db.conf.get_demo_pool()
                        cluster["new_cluster"].pop("node_type_id", None)
                        cluster["new_cluster"].pop("enable_elastic_disk", None) 
                        cluster["new_cluster"].pop("aws_attributes", None)

        # Add support for clsuter specific task
        for task in definition["settings"]["tasks"]:
            if "new_cluster" in task:
                merge_dict(task["new_cluster"], cluster_conf, override=False)

        # if we're installing from a serverless cluster, update the job to be fully serverless
        if serverless:
            environments = []
            for task in definition["settings"]["tasks"]:
                task.pop("new_cluster", None)
                task.pop("job_cluster_key", None)
                task.pop("existing_cluster_id", None)
                
                # Serverless doesn't support libraries. Instead, they have environements and we can link these env to each task.
                # Extract libraries if they exist and convert to environment for serverless compute.
                if "libraries" in task:
                    env_key = "env_" + task["task_key"]
                    dependencies = []
                    for lib in task["libraries"]:
                        if "pypi" in lib and "package" in lib["pypi"]:
                            dependencies.append(lib["pypi"]["package"])
                    
                    if dependencies:
                        environments.append({
                            "environment_key": env_key,
                            "spec": {
                                "client": "1",
                                "dependencies": dependencies
                            }
                        })
                        task["environment_key"] = env_key
                    task.pop("libraries", None)
            
            definition["settings"].pop("job_clusters", None)
            if environments:
                definition["settings"]["environments"] = environments
        
        existing_job = self.installer.db.find_job(job_name)
        if existing_job is not None:
            job_id = existing_job["job_id"]
            self.installer.db.post("/2.1/jobs/runs/cancel-all", {"job_id": job_id})
            self.wait_for_run_completion(job_id, debug=debug)
            if debug:
                print("    Updating existing job")
            job_config = {"job_id": job_id, "new_settings": definition["settings"]}
            r = self.installer.db.post("2.1/jobs/reset", job_config)
            if "error_code" in r:
                self.installer.report.display_workflow_error(WorkflowException("Can't update the workflow",
                                                                               f"error resetting the workflow, do you have permission?.", job_config, r), demo_conf.name)
        else:
            if debug:
                print("    Creating a new job for demo initialization (data & table setup).")
            r_jobs = self.installer.db.post("2.1/jobs/create", definition["settings"])
            if "error_code" in r_jobs:
                self.installer.report.display_workflow_error(WorkflowException("Can't create the workflow", {}, definition["settings"], r_jobs), demo_conf.name)
            job_id = r_jobs["job_id"]
        if run_now:
            j = self.installer.db.post("2.1/jobs/run-now", {"job_id": job_id})
            if "error_code" in j:
                self.installer.report.display_workflow_error(WorkflowException("Can't start the workflow", {"job_id": job_id}, j), demo_conf.name)
            return job_id, j['run_id']
        return job_id, None

    def replace_warehouse_id(self, demo_conf: DemoConf, definition, warehouse_name: str = None):
        # Jobs need a warehouse ID. Let's replace it with the one created. TODO: should be in the template?
        if "{{SHARED_WAREHOUSE_ID}}" in json.dumps(definition):
            endpoint = self.installer.get_or_create_endpoint(self.db.conf.name, demo_conf, warehouse_name = warehouse_name)
            if endpoint is None:
                print(
                    "ERROR: couldn't create or get a SQL endpoint for dbdemos. Do you have permission? Your workflow won't be able to execute the task.")
                #TODO: quick & dirty, need to improve
                definition = json.loads(json.dumps(definition).replace(""", "warehouse_id": "{{SHARED_WAREHOUSE_ID}}"}""", ""))
            else:
                definition = json.loads(json.dumps(definition).replace("{{SHARED_WAREHOUSE_ID}}", endpoint['warehouse_id']))
        return definition

    def wait_for_run_completion(self, job_id, max_retry=10, debug = False):
        def is_still_running(job_id):
            runs = self.installer.db.get("2.1/jobs/runs/list", {"job_id": job_id, "active_only": "true"})
            return "runs" in runs and len(runs["runs"]) > 0
        i = 0
        while i <= max_retry and is_still_running(job_id):
            if debug:
                print(f"      A run is still running for job {job_id}, waiting for termination...")
            time.sleep(5)