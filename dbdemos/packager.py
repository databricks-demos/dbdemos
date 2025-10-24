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
import zipfile
import io


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
        with ThreadPoolExecutor(max_workers=3) as executor:
            collections.deque(executor.map(package_demo, confs))

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


    def process_file_content(self, file, destination_path, extension = ""):
        # Decode base64 content from the folder dict
        file_content = base64.b64decode(file['content'])
        with open(destination_path + extension, "wb") as f:
            f.write(file_content)

    def process_notebook_content(self, html, full_path):
        #Replace notebook content.
        parser = NotebookParser(html)
        parser.remove_uncomment_tag()
        parser.set_environement_metadata()
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

    def package_demo(self, demo_conf: DemoConf):
        print(f"packaging demo {demo_conf.name} ({demo_conf.path})")
        if len(demo_conf.get_notebooks_to_publish()) > 0 and not self.jobBundler.staging_reseted:
            self.jobBundler.reset_staging_repo()
        if len(demo_conf.get_notebooks_to_run()) > 0:
            run = self.db.get("2.1/jobs/runs/get", {"run_id": demo_conf.run_id, "include_history": False})
            if 'state' not in run:
                raise Exception(f"Can't get the last job {self.db.conf.workspace_url}/#job/{demo_conf.job_id}/run/{demo_conf.run_id} state for demo {demo_conf.name}: {run}")
            if run['state']['result_state'] != 'SUCCESS':
                raise Exception(f"last job {self.db.conf.workspace_url}/#job/{demo_conf.job_id}/run/{demo_conf.run_id} failed for demo {demo_conf.name}. Can't package the demo. {run['state']}")

        def download_notebook_html(notebook: DemoNotebook):
            full_path = demo_conf.get_bundle_path()+"/"+notebook.get_clean_path()
            print(f"downloading {notebook.path} to {full_path}")
            Path(full_path[:full_path.rindex("/")]).mkdir(parents=True, exist_ok=True)
            if not notebook.pre_run:
                repo_path = self.jobBundler.conf.get_repo_path()+"/"+demo_conf.path+"/"+notebook.path
                repo_path = os.path.realpath(repo_path)
                #print(f"downloading from repo {repo_path}")
                status = self.db.get("2.0/workspace/get-status", {"path": repo_path})
                if 'error_code' in status:
                    raise Exception(f"Couldn't find file {repo_path} in workspace. Check notebook path in bundle conf file. {status['error_code']} - {status['message']}")
                #We add the type of the object in the conf to know how to load it back.
                demo_conf.update_notebook_object_type(notebook, status['object_type'])
                if status['object_type'] == 'NOTEBOOK':
                    file = self.db.get("2.0/workspace/export", {"path": repo_path, "format": "HTML", "direct_download": False})
                    if 'error_code' in file:
                        raise Exception(f"Couldn't find file {repo_path} in workspace. Check notebook path in bundle conf file. {file['error_code']} - {file['message']}")
                    html = base64.b64decode(file['content']).decode('utf-8')
                    return self.process_notebook_content(html, full_path+".html")
                elif status['object_type'] == 'DIRECTORY':
                    folder = self.db.get("2.0/workspace/export", {"path": repo_path, "format": "AUTO", "direct_download": True})
                    return self.process_file_content(folder, full_path, ".zip")
                elif status['object_type'] == 'FILE':
                    file = self.db.get("2.0/workspace/export", {"path": repo_path, "format": "AUTO", "direct_download": True})
                    return self.process_file_content(file, full_path)
                else:
                    raise Exception(f"Unsupported object type {status['object_type']} for {repo_path}")
            else:
                tasks = [t for t in run['tasks'] if t['notebook_task']['notebook_path'].endswith(notebook.get_clean_path())]
                if len(tasks) == 0:
                    raise Exception(f"couldn't find task for notebook {notebook.path}. Please re-run the job & make sure the stating git repo is synch / reseted.")
                #print(f"Exporting notebook from job run {tasks[0]['run_id']}")
                notebook_result = self.db.get("2.1/jobs/runs/export", {'run_id': tasks[0]['run_id'], 'views_to_export': 'ALL'})
                if "views" not in notebook_result:
                    raise Exception(f"couldn't get notebook for run {tasks[0]['run_id']} - {notebook.path}. {demo_conf.name}. You probably did a run repair. Please re run the job. - {notebook_result}")
                html = notebook_result["views"][0]["content"]
                return self.process_notebook_content(html, full_path+".html")
            

        requires_global_setup_v2 = False
        
        # Process notebooks in parallel with max 5 workers
        with ThreadPoolExecutor(max_workers=10) as executor:
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

    def get_file_icon_svg(self, file_path: str) -> str:
        """
        Get the appropriate SVG icon based on file type

        Args:
            file_path: Path to the file

        Returns:
            SVG icon as HTML string
        """
        # Notebook icon (for .html notebook files)
        notebook_icon = '''<svg class="file-icon" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" fill="none" viewBox="0 0 16 16" aria-hidden="true" focusable="false"><path fill="currentColor" fill-rule="evenodd" d="M3 1.75A.75.75 0 0 1 3.75 1h10.5a.75.75 0 0 1 .75.75v12.5a.75.75 0 0 1-.75.75H3.75a.75.75 0 0 1-.75-.75V12.5H1V11h2V8.75H1v-1.5h2V5H1V3.5h2zm1.5.75v11H6v-11zm3 0v11h6v-11z" clip-rule="evenodd"></path></svg>'''

        # Generic file icon (for .py, .sql, and other code files)
        file_icon = '''<svg class="file-icon" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" fill="none" viewBox="0 0 16 16" aria-hidden="true" focusable="false"><path fill="currentColor" fill-rule="evenodd" d="M2 1.75A.75.75 0 0 1 2.75 1h6a.75.75 0 0 1 .53.22l4.5 4.5c.141.14.22.331.22.53v9a.75.75 0 0 1-.75.75H2.75a.75.75 0 0 1-.75-.75zm1.5.75v12h9V7H8.75A.75.75 0 0 1 8 6.25V2.5zm6 1.06 1.94 1.94H9.5z" clip-rule="evenodd"></path></svg>'''

        if file_path.endswith(('.py', '.sql')):
            return file_icon
        else:
            return notebook_icon

    def build_tree_structure(self, notebooks_to_publish):
        """
        Build a hierarchical tree structure from notebook paths

        Returns a nested dict representing the folder/file hierarchy
        """
        tree = {}

        for notebook in notebooks_to_publish:
            parts = notebook.get_clean_path().split('/')
            current = tree

            # Navigate/create the folder structure
            for i, part in enumerate(parts[:-1]):
                if part not in current:
                    current[part] = {'__type__': 'folder', '__children__': {}}
                current = current[part]['__children__']

            # Add the file at the end
            filename = parts[-1]
            current[filename] = {
                '__type__': 'file',
                '__notebook__': notebook,
                '__path__': notebook.get_clean_path()
            }

        return tree

    def render_tree_html(self, tree, iframe_root_src="./", level=0):
        """
        Recursively render the tree structure as HTML with CSS-based tree lines

        Args:
            tree: The tree structure dict
            iframe_root_src: Root path for iframe sources
            level: Current depth level
        """
        html = ""
        items = sorted(tree.items(), key=lambda x: (x[1].get('__type__') != 'folder', x[0]))

        for idx, (name, node) in enumerate(items):
            is_last = (idx == len(items) - 1)

            if node['__type__'] == 'folder':
                # Render folder
                folder_id = f"folder_{level}_{idx}_{name.replace(' ', '_')}"

                html += f'''
                <div class="tree-item tree-folder {'tree-last' if is_last else ''}">
                    <div class="tree-item-row folder-row expanded" data-folder-id="{folder_id}" onclick="toggleFolder('{folder_id}')">
                        <svg class="folder-icon" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" fill="none" viewBox="0 0 16 16">
                            <path fill="currentColor" d="M.75 2a.75.75 0 0 0-.75.75v10.5c0 .414.336.75.75.75h14.5a.75.75 0 0 0 .75-.75v-8.5a.75.75 0 0 0-.75-.75H7.81L6.617 2.805A2.75 2.75 0 0 0 4.672 2z"></path>
                        </svg>
                        <span class="folder-name">{name}</span>
                    </div>
                    <div class="tree-children" id="{folder_id}">
                '''

                # Recursively render children
                html += self.render_tree_html(node['__children__'], iframe_root_src, level + 1)

                html += '''
                    </div>
                </div>
                '''
            else:
                # Render file
                notebook = node['__notebook__']
                path = node['__path__']
                file_icon = self.get_file_icon_svg(path)
                notebook_link = iframe_root_src + path + ".html"
                # Use the filename (last part of the path)
                filename = path.split('/')[-1]

                html += f'''
                <div class="tree-item tree-file {'tree-last' if is_last else ''}">
                    <a href="#" class="tree-item-row file-row _left_menu" iframe-src="{notebook_link}">
                        {file_icon}
                        <span class="file-name">{filename}</span>
                    </a>
                </div>
                '''

        return html

    def generate_html_from_code_file(self, code_file_path: str, output_html_path: str, demo_name: str):
        """
        Generate HTML file from .py or .sql code file with syntax highlighting

        Args:
            code_file_path: Path to the source code file (.py or .sql)
            output_html_path: Path where the HTML file should be saved
            demo_name: Name of the demo (for metadata)
        """
        import html

        # Determine file type and language
        file_extension = code_file_path.split('.')[-1]
        file_name = os.path.basename(code_file_path)
        file_path_display = code_file_path

        if file_extension == 'py':
            language = 'python'
            file_type = 'Python'
        elif file_extension == 'sql':
            language = 'sql'
            file_type = 'SQL'
        else:
            raise ValueError(f"Unsupported file type: {file_extension}. Only .py and .sql are supported.")

        # Read the code file
        with open(code_file_path, 'r', encoding='utf-8') as f:
            code_content = f.read()

        # HTML escape the code content to prevent XSS
        code_content_escaped = html.escape(code_content)

        # Load the code viewer template
        template = pkg_resources.resource_string("dbdemos", "template/code_viewer.html").decode('UTF-8')

        # Replace placeholders
        template = template.replace("{{FILE_NAME}}", file_name)
        template = template.replace("{{FILE_TYPE}}", file_type)
        template = template.replace("{{FILE_PATH}}", file_path_display)
        template = template.replace("{{LANGUAGE}}", language)
        template = template.replace("{{CODE_CONTENT}}", code_content_escaped)
        template = template.replace("{{DEMO_NAME}}", demo_name)

        # Write the HTML file
        with open(output_html_path, 'w', encoding='utf-8') as f:
            f.write(template)

    #Build HTML pages with index.
    # - If the notebook is pre-run, load them from the install_package folder
    # - If the notebook isn't pre-run, download them from the pacakge workspace as HTML (ex: can't run SDP pipelines)
    def build_minisite(self, demo_conf: DemoConf, iframe_root_src = "./"):
        notebooks_to_publish = demo_conf.get_notebooks_to_publish()
        print(f"Build minisite for demo {demo_conf.name} ({demo_conf.path}) - {notebooks_to_publish}")
        minisite_path = demo_conf.get_minisite_path()

        for notebook in notebooks_to_publish:
            Path(minisite_path).mkdir(parents=True, exist_ok=True)
            full_path = minisite_path+"/"+notebook.get_clean_path()+".html"
            Path(full_path[:full_path.rindex("/")]).mkdir(parents=True, exist_ok=True)

            # Check if we have a code file (.py or .sql) or notebook HTML file
            # Code files are stored with their full extension in the bundle
            clean_path = notebook.get_clean_path()

            if notebook.path.endswith(('.py', '.sql')):
                # Code file - path already includes extension (.py or .sql)
                source_file_path = demo_conf.get_bundle_path() + "/" + clean_path
                file_type = clean_path.split('.')[-1].upper()
                print(f"  Generating HTML from {file_type} file: {source_file_path}")
                self.generate_html_from_code_file(source_file_path, full_path, demo_conf.name)
            else:
                # Standard notebook HTML file - append .html extension
                source_file_path = demo_conf.get_bundle_path() + "/" + clean_path + ".html"
                if not os.path.exists(source_file_path):
                    raise FileNotFoundError(f"Could not find notebook file: {source_file_path}")
                with open(source_file_path, "r") as f:
                    parser = NotebookParser(f.read())
                with open(full_path, "w") as f:
                    parser.remove_robots_meta()
                    parser.add_cell_as_html_for_seo()
                    parser.remove_delete_cell()
                    parser.add_javascript_to_minisite_relative_links(notebook.get_clean_path())
                    f.write(parser.get_html())

        # Build the tree structure from all notebooks
        tree = self.build_tree_structure(notebooks_to_publish)

        # Render the tree as HTML
        tree_html = self.render_tree_html(tree, iframe_root_src)

        # Create the index file
        template = pkg_resources.resource_string("dbdemos", "template/index.html").decode('UTF-8')
        template = template.replace("{{LEFT_MENU}}", tree_html)
        template = template.replace("{{TITLE}}", demo_conf.title)
        template = template.replace("{{DESCRIPTION}}", demo_conf.description)
        template = template.replace("{{DEMO_NAME}}", demo_conf.name)
        with open(minisite_path+"/index.html", "w") as f:
            f.write(template)
        #dump the conf
        with open(demo_conf.get_bundle_root_path()+"/conf.json", "w") as f:
            f.write(json.dumps(demo_conf.json_conf))