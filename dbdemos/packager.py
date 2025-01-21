import pkg_resources
from pathlib import Path
from .conf import DBClient, DemoConf, Conf, DemoNotebook
from .notebook_parser import NotebookParser
import json
import os
import re
import shutil
import base64
from .job_bundler import JobBundler
from concurrent.futures import ThreadPoolExecutor
import collections


class Packager:
    DASHBOARD_IMPORT_API = "_import_api"
    def __init__(self, conf: Conf, jobBundler: JobBundler):
        self.db = DBClient(conf)
        self.jobBundler = jobBundler

    def package_all(self, iframe_root_src = "./"):
        def package_demo(demo_conf: DemoConf):
            self.clean_bundle(demo_conf)
            self.package_demo(demo_conf)
            if len(demo_conf.dashboards) > 0:
                self.extract_lakeview_dashboards(demo_conf)
            self.build_minisite(demo_conf, iframe_root_src)

        confs = [demo_conf for _, demo_conf in self.jobBundler.bundles.items()]
        for demo_conf in confs:
            package_demo(demo_conf)

    def clean_bundle(self, demo_conf: DemoConf):
        if Path(demo_conf.get_bundle_root_path()).exists():
            shutil.rmtree(demo_conf.get_bundle_root_path())


    def extract_lakeview_dashboards(self, demo_conf: DemoConf):
        for d in demo_conf.dashboards:
            repo_path = self.jobBundler.conf.get_repo_path()+"/"+demo_conf.path+"/_resources/dashboards/"+d['id']+".lvdash.json"
            repo_path = os.path.realpath(repo_path)
            dashboard_file = self.db.get("2.0/workspace/export", {"path": repo_path, "format": "SOURCE", "direct_download": False})
            if 'error_code' in dashboard_file:
                raise Exception(f"Couldn't find dashboard {repo_path} in repo. Check repo ID in bundle conf file and make sure the dashboard is here. "
                                f"{dashboard_file['error_code']} - {dashboard_file['message']}")
            dashboard_file = base64.b64decode(dashboard_file['content']).decode('utf-8')
            full_path = demo_conf.get_bundle_path()+"/_resources/dashboards/"+d['id']+".lvdash.json"
            Path(full_path[:full_path.rindex("/")]).mkdir(parents=True, exist_ok=True)
            with open(full_path, "w") as f:
                f.write(dashboard_file)


    def package_demo(self, demo_conf: DemoConf):
        print(f"packaging demo {demo_conf.name} ({demo_conf.path})")
        if len(demo_conf.get_notebooks_to_publish()) > 0 and not self.jobBundler.staging_reseted:
            self.jobBundler.reset_staging_repo()
        if len(demo_conf.get_notebooks_to_run()) > 0:
            run = self.db.get("2.1/jobs/runs/get", {"run_id": demo_conf.run_id, "include_history": False})
            if run['state']['result_state'] != 'SUCCESS':
                raise Exception(f"last job {self.db.conf.workspace_url}/#job/{demo_conf.job_id}/run/{demo_conf.run_id} failed for demo {demo_conf.name}. Can't package the demo. {run['state']}")

        def download_notebook_html(notebook):
            full_path = demo_conf.get_bundle_path()+"/"+notebook.get_clean_path()+".html"
            print(f"downloading {notebook.path} to {full_path}")
            Path(full_path[:full_path.rindex("/")]).mkdir(parents=True, exist_ok=True)
            if not notebook.pre_run:
                repo_path = self.jobBundler.conf.get_repo_path()+"/"+demo_conf.path+"/"+notebook.path
                repo_path = os.path.realpath(repo_path)
                #print(f"downloading from repo {repo_path}")
                file = self.db.get("2.0/workspace/export", {"path": repo_path, "format": "HTML", "direct_download": False})
                if 'error_code' in file:
                    raise Exception(f"Couldn't find file {repo_path} in workspace. Check notebook path in bundle conf file. {file['error_code']} - {file['message']}")
                html = base64.b64decode(file['content']).decode('utf-8')
            else:
                tasks = [t for t in run['tasks'] if t['notebook_task']['notebook_path'].endswith(notebook.get_clean_path())]
                if len(tasks) == 0:
                    raise Exception(f"couldn't find task for notebook {notebook.path}. Please re-run the job & make sure the stating git repo is synch / reseted.")
                #print(f"Exporting notebook from job run {tasks[0]['run_id']}")
                notebook_result = self.db.get("2.1/jobs/runs/export", {'run_id': tasks[0]['run_id'], 'views_to_export': 'ALL'})
                if "views" not in notebook_result:
                    raise Exception(f"couldn't get notebook for run {tasks[0]['run_id']} - {notebook.path}. {demo_conf.name}. You probably did a run repair. Please re run the job.")
                html = notebook_result["views"][0]["content"]
            #Replace notebook content.
            parser = NotebookParser(html)
            parser.remove_uncomment_tag()
            parser.remove_dbdemos_build()
            #parser.remove_static_settings()
            parser.hide_commands_and_results()
            #Moving away from the initial 00-global-setup, remove it once migration is completed
            requires_global_setup_v2 = False
            if parser.contains("00-global-setup-v2"):
                parser.replace_in_notebook('(?:\.\.\/)*_resources\/00-global-setup-v2', './00-global-setup-v2', True)
                requires_global_setup_v2 = True
            elif parser.contains("00-global-setup"):
                raise Exception("00-global-setup is deprecated. Please use 00-global-setup-v2 instead.")
            with open(full_path, "w") as f:
                f.write(parser.get_html())
            return requires_global_setup_v2

        requires_global_setup_v2 = False
        
        # Process notebooks in parallel with max 5 workers
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all notebooks for processing and collect futures
            futures = [executor.submit(download_notebook_html, notebook) for notebook in demo_conf.notebooks]
            
            # Process results as they complete
            for future in futures:
                rv1 = future.result()
                if rv1:
                    requires_global_setup_v2 = True

        #Add the global notebook if required
        if requires_global_setup_v2:
            init_notebook = DemoNotebook("_resources/00-global-setup-v2", "Global init", "Global init")
            demo_conf.add_notebook(init_notebook)
            file = self.db.get("2.0/workspace/export", {"path": self.jobBundler.conf.get_repo_path() +"/"+ init_notebook.path, "format": "HTML", "direct_download": False})
            if 'error_code' in file:
                raise Exception(f"Couldn't find file '{self.jobBundler.conf.get_repo_path()}/{init_notebook.path}' in workspace. Check notebook path in bundle conf file. {file['error_code']} - {file['message']}")
            html = base64.b64decode(file['content']).decode('utf-8')
            with open(demo_conf.get_bundle_path() + "/" + init_notebook.path+".html", "w") as f:
                f.write(html)

    def get_html_menu(self, path: str, title: str, description: str, notebook_link: str):
        # Add padding for subfolder for better visualization
        padding = path.count("/")*20+10
        return f"""
                <a href="#" class="_left_menu list-group-item list-group-item-action py-3 lh-sm" iframe-src="{notebook_link}" style="padding: 2px 2px 2px {padding}px;">
                    <div class="d-flex w-100 align-items-center justify-content-between">
                        <span class="notebook_path"><strong class="mb-1">{title}</strong></span>
                    </div>
                    <div class="small notebook_description">{description}</div>
                </a>"""

    #Build HTML pages with index.
    # - If the notebook is pre-run, load them from the install_package folder
    # - If the notebook isn't pre-run, download them from the pacakge workspace as HTML (ex: can't run DLT pipelines)
    def build_minisite(self, demo_conf: DemoConf, iframe_root_src = "./"):
        notebooks_to_publish = demo_conf.get_notebooks_to_publish()
        print(f"Build minisite for demo {demo_conf.name} ({demo_conf.path}) - {notebooks_to_publish}")
        minisite_path = demo_conf.get_minisite_path()
        html_menu = {}
        previous_folder = ""
        for notebook in notebooks_to_publish:
            Path(minisite_path).mkdir(parents=True, exist_ok=True)
            full_path = minisite_path+"/"+notebook.get_clean_path()+".html"
            Path(full_path[:full_path.rindex("/")]).mkdir(parents=True, exist_ok=True)
            with open(demo_conf.get_bundle_path()+"/"+notebook.get_clean_path()+".html", "r") as f:
                parser = NotebookParser(f.read())
            with open(full_path, "w") as f:
                parser.remove_robots_meta()
                parser.add_cell_as_html_for_seo()
                parser.remove_delete_cell()
                parser.add_javascript_to_minisite_relative_links()
                f.write(parser.get_html())
            menu_entry = ""
            title = notebook.get_clean_path()
            i = title.rfind("/")
            if i > 0:
                folder = title[:i+1]
                title = title[i+1:]
            else:
                folder = ""
            if folder != previous_folder:
                previous_folder = folder
                if len(folder) > 0:
                    menu_entry = f"""<div style="padding: 2px 0px 2px 10px; border-bottom: 1px solid rgba(0,0,0,.125);">
                                        <svg xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" fill="none" viewBox="0 0 16 16" aria-hidden="true" focusable="false" class=""><path fill="currentColor" d="M.75 2a.75.75 0 0 0-.75.75v10.5c0 .414.336.75.75.75h14.5a.75.75 0 0 0 .75-.75v-8.5a.75.75 0 0 0-.75-.75H7.81L6.617 2.805A2.75 2.75 0 0 0 4.672 2H.75Z"></path></svg>
                                        {folder[:-1]}</div>"""

            menu_entry += self.get_html_menu(notebook.get_clean_path(), title, notebook.description, iframe_root_src+notebook.get_clean_path()+".html")
            html_menu[notebook.get_clean_path()] = menu_entry

        #create the index file
        template = pkg_resources.resource_string("dbdemos", "template/index.html").decode('UTF-8')
        #Sort the menu to display  proper order.
        menu_keys = [*html_menu]
        menu_keys.sort()
        template = template.replace("{{LEFT_MENU}}", ' '.join([html_menu[k] for k in menu_keys]))
        template = template.replace("{{TITLE}}", demo_conf.title)
        template = template.replace("{{DESCRIPTION}}", demo_conf.description)
        template = template.replace("{{DEMO_NAME}}", demo_conf.name)
        with open(minisite_path+"/index.html", "w") as f:
            f.write(template)
        #dump the conf
        with open(demo_conf.get_bundle_root_path()+"/conf.json", "w") as f:
            f.write(json.dumps(demo_conf.json_conf))