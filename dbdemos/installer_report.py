from dbsqlclone.utils.load_dashboard import DashboardWidgetException

from .conf import DBClient, DemoConf, Conf, ConfTemplate, merge_dict, DemoNotebook
from .exceptions.dbdemos_exception import ClusterCreationException, ExistingResourceException, FolderDeletionException, \
    DLTException, WorkflowException, FolderCreationException
from pathlib import Path
import json

class InstallerReport:

    NOTEBOOK_SVG = """<svg width="1em" height="1em" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false" class=""><path fill-rule="evenodd" clip-rule="evenodd" d="M3 1.75A.75.75 0 013.75 1h10.5a.75.75 0 01.75.75v12.5a.75.75 0 01-.75.75H3.75a.75.75 0 01-.75-.75V12.5H1V11h2V8.75H1v-1.5h2V5H1V3.5h2V1.75zm1.5.75v11H6v-11H4.5zm3 0v11h6v-11h-6z" fill="currentColor"></path></svg>"""
    FOLDER_SVG = """<svg width="1em" height="1em" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false" class=""><path d="M.75 2a.75.75 0 00-.75.75v10.5c0 .414.336.75.75.75h14.5a.75.75 0 00.75-.75v-8.5a.75.75 0 00-.75-.75H7.81L6.617 2.805A2.75 2.75 0 004.672 2H.75z" fill="currentColor"></path></svg>"""
    DASHBOARD_SVG = """<svg width="1em" height="1em" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false" class=""><path fill-rule="evenodd" clip-rule="evenodd" d="M1 1.75A.75.75 0 011.75 1h12.5a.75.75 0 01.75.75v12.5a.75.75 0 01-.75.75H1.75a.75.75 0 01-.75-.75V1.75zm1.5 8.75v3h4.75v-3H2.5zm0-1.5h4.75V2.5H2.5V9zm6.25-6.5v3h4.75v-3H8.75zm0 11V7h4.75v6.5H8.75z" fill="currentColor"></path></svg>"""

    CSS_REPORT = """
    <style>
    .dbdemos_install{
                        font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji,FontAwesome;
    color: #3b3b3b;
    box-shadow: 0 .15rem 1.15rem 0 rgba(58,59,69,.15)!important;
    padding: 10px;
    margin: 10px;
    }
    .dbdemos_block{
        display: block !important;
    }
    .code {
        padding: 5px;
        border: 1px solid #e4e4e4;
        font-family: monospace;
        background-color: #f5f5f5;
        margin: 5px 0px 0px 0px;
        display: inline;
    }
    .subfolder {
        padding-left: 30px;
    }
    .notebook {
        margin-bottom: 3px;
    }
    .dbdemos_install a {
        color: #3835a4;
    }
    .container_dbdemos {
        padding-left: 20px;
    }
    .path_desc {
        color: #928e9b;
        font-style: oblique;
    }
    </style>"""

    def __init__(self, workspace_url: str):
        self.workspace_url = workspace_url

    def displayHTML_available(self):
        try:
            from dbruntime.display import displayHTML
            return True
        except:
            return False

    def display_cluster_creation_error(self, exception: ClusterCreationException, demo_conf: DemoConf):
        self.display_error(exception, f"By default, dbdemos try to create a new cluster for your demo with the proper settings. <br/>"
                                      f"If you don't have cluster creation permission, dbdemos can use the current cluster to run the content.<br/>"
                                      f"For the demo to run properly, <strong>make sure your cluster has UC enabled and using Databricks Runtime (DBR) version {exception.cluster_conf['spark_version']}</strong>.<br/>"
                                      f"""<div class="code dbdemos_block">dbdemos.install('{demo_conf.name}', use_current_cluster = True)</div><br/>"""
                                      f"<strong>Cluster creation details</strong><br/>"
                                      f"""Full cluster configuration: <div class="code dbdemos_block">{json.dumps(exception.cluster_conf)}.</div><br/>"""
                                      f"""Full error: <div class="code dbdemos_block">{json.dumps(exception.response)}</div>""")


    def display_dashboard_widget_exception(self, exception: DashboardWidgetException, demo_conf: DemoConf):
        self.display_error(exception, f"""The dashboard has widget queries, and these queries need to be run before importing other queries.<br/>
                                          dbdemo started a job to run these queries but it failed with the following error<br/>
                                          <div class="code dbdemos_block">{exception.error}</div><br/>
                                          For more details, open the job run page: {exception.run_url}<br/>
                                          You can skip the dashboard installation with skip_dashboards = True:
                                          <div class="code dbdemos_block">dbdemos.install('{demo_conf.name}', skip_dashboards = True)</div><br/>""")

    def display_dashboard_error(self, exception: Exception, demo_conf: DemoConf):
        self.display_error(exception, f"""Couldn't create or update a dashboard. <br/>
                                          If this is a permission error, we recommend you to search the existing dashboard and delete it manually.<br/>
                                          You can skip the dashboard installation with skip_dashboards = True:
                                          <div class="code dbdemos_block">dbdemos.install('{demo_conf.name}', skip_dashboards = True)</div><br/>""")

    def display_folder_already_existing(self, exception: ExistingResourceException, demo_conf: DemoConf):
        self.display_error(exception, f"""Please install demo with overwrite=True to replace the existing folder content under {exception.install_path}:
                                         <div class="code dbdemos_block">dbdemos.install('{demo_conf.name}', overwrite=True, path='{exception.install_path}')</div><br/>
                                         All content under {exception.install_path} will be deleted.<br/><br/>
                                         <strong>Details</strong><br/>
                                         Folder list response: <div class="code dbdemos_block">{json.dumps(exception.response)}</div>""")

    def display_folder_permission(self, exception: FolderDeletionException, demo_conf: DemoConf):
        self.display_error(exception, f"""Can't delete the folder {exception.install_path}. <br/>
                                          Do you have read/write permission?<br/><br/>
                                         <strong>Details</strong><br/>
                                         Delete response: <div class="code dbdemos_block">{json.dumps(exception.response)}</div>""")

    def display_folder_creation_error(self, exception: FolderCreationException, demo_conf: DemoConf):
        self.display_error(exception, f"""Couldn't load the model in the current folder. Do you have permissions to write in {exception.install_path}?
                                     Please install demo with overwrite=True to replace the existing folder:
                                     <div class="code dbdemos_block">dbdemos.install('{demo_conf.name}', overwrite=True, path='{exception.install_path}')</div><br/>
                                     All content under {exception.install_path} will be deleted.<br/><br/>
                                     <strong>Details</strong><br/>
                                     Folder list response: <div class="code dbdemos_block">{json.dumps(exception.response)}</div>""")

    def display_non_premium_warn(self, exception: Exception, response):
        self.display_error(exception, f"""DBSQL isn't available in this workspace. Only Premium/Enterprise workspaces are supported.<br/>
                                          dbdemos will try its best to install the demo and load the notebooks, but some component won't be available (DLT pipelines, Dashboards etc).<br/>
                                          Forcing skip_dashboards = True and continuing.<br/><br/>
                                          <strong>Details</strong><br/>
                                          API response: <div class="code dbdemos_block">{json.dumps(response)}</div>""", raise_error=False, warning=True)

    def display_pipeline_error(self, exception: DLTException):
        self.display_error(exception, f"""{exception.description}. <br/>
                                         Skipping pipelines. Your demo will be installed without DLT pipelines.<br/><br/>
                                         <strong>Details</strong><br/>
                                         Pipeline configuration: <div class="code dbdemos_block">{json.dumps(exception.pipeline_conf)}</div>
                                         API response: <div class="code dbdemos_block">{json.dumps(exception.response)}</div>""", raise_error=False, warning=True)

    def display_workflow_error(self, exception: WorkflowException, demo_name: str):
        self.display_error(exception, f"""{exception.details}. <br/>
                                         dbdemos creates jobs to load your demo data. If you don't have cluster creation permission, you can start the job using the current cluster.
                                         <div class="code dbdemos_block">dbdemos.install('{demo_name}', use_current_cluster=True)</div><br/>
                                         <strong>Details</strong><br/>
                                         Pipeline configuration: <div class="code dbdemos_block">{json.dumps(exception.job_config)}</div>
                                         API response: <div class="code dbdemos_block">{json.dumps(exception.response)}</div>""")

    def display_demo_name_error(self, name, demos):
        html = "<h2>Demos available:</h2>"
        for cat in demos:
            html += f"<strong>{cat}</strong>"
            for demo in demos[cat]:
                html += f"""<div style="padding-left: 40px">{demo.name}: <span class="path_desc">{demo.description}</span></div>"""
        self.display_error(Exception(f"Demo '{name}' doesn't exist"),
                                    f"""This demo doesn't exist, please check your demo name and re-run the installation.<br/>
                                    {html}                
                                    <br/><br/>
                                     To get a full demo list, please run
                                     <div class="code dbdemos_block">dbdemos.list_demos()</div>""")

    def display_error(self, exception, message, raise_error = True, warning = False):
        color = "#d18b2a" if warning else "#eb0707"
        level = "warning" if warning else "error"
        error = f"""{InstallerReport.CSS_REPORT}<div class="dbdemos_install">
                      <h1 style="color: {color}">Installation {level}: {exception}</h1> 
                        {message}
                      </div>"""
        if self.displayHTML_available():
            from dbruntime.display import displayHTML
            displayHTML(error)
        else:
            print(error)
        if raise_error:
            raise exception


    def display_install_result(self, demo_name, description, title, install_path = None, notebooks = [], job_id = None, run_id = None, cluster_id = None, cluster_name = None, pipelines_ids = [], dashboards = [], workflows = []):
        if self.displayHTML_available():
            self.display_install_result_html(demo_name, description, title, install_path, notebooks, job_id, run_id, cluster_id, cluster_name, pipelines_ids, dashboards, workflows)
        else:
            self.display_install_result_console(demo_name, description, title, install_path, notebooks, job_id, run_id, cluster_id, cluster_name, pipelines_ids, dashboards, workflows)

    def get_install_result_html(self, demo_name, description, title, install_path = None, notebooks = [], job_id = None, run_id = None, cluster_id = None, cluster_name = None, pipelines_ids = [], dashboards = [], workflows = []):
        html = f"""{InstallerReport.CSS_REPORT}
        <div class="dbdemos_install">
            <img style="float:right; width: 100px; padding: 20px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/resources/{demo_name}.png" />
            <h1>Your demo {title} is ready!</h1>
            <i>{description}</i><br/><br/>
            """
        if cluster_id is not None:
            cluster_section = f"""
            <h2>Interactive cluster for the demo:</h2>
            <a href="{self.workspace_url}/#setting/clusters/{cluster_id}/configuration">{cluster_name}</a>. You can refresh your demo cluster with:
            <div class="code">
                dbdemos.create_cluster('{demo_name}')
            </div>"""
            cluster_instruction = f' using the cluster <a href="{self.workspace_url}/#setting/clusters/{cluster_id}/configuration">{cluster_name}</a>'
        else:
            cluster_section = ""
            cluster_instruction = ""
        if len(notebooks) > 0:
            first = list(filter(lambda n: "/" not in n.get_clean_path(), notebooks))
            first.sort(key=lambda n: n.get_clean_path())
            html += f"""Start with the first notebook {InstallerReport.NOTEBOOK_SVG} <a href="{self.workspace_url}/#workspace{install_path}/{demo_name}/{first[0].get_clean_path()}">{demo_name}/{first[0].get_clean_path()}</a>{cluster_instruction}\n"""
            html += """<h2>Notebook installed:</h2><div class="container_dbdemos">\n """
            if len(pipelines_ids)>0 or len(dashboards)>0:
                html += """<div style="float: right; width: 300px">"""
                if len(pipelines_ids)>0:
                    html += f"""<img src="https://www.dbdemos.ai/assets/img/dbdemos/{demo_name}-dlt-0.png" style="width: 300px; margin-bottom: 10px">"""
                if len(dashboards)>0:
                    html += f"""<img src="https://www.dbdemos.ai/assets/img/dbdemos/{demo_name}-dashboard-0.png" style="width: 300px">"""
                html += """</div>"""
            previous_folder = ""
            for n in notebooks:
                if "_resources" not in n.get_clean_path():
                    #from pathlib import Path
                    parts = Path(n.get_clean_path()).parts
                    path = n.get_clean_path()
                    if len(parts) > 1 :
                        path = str(Path(*parts[1:]))
                        if previous_folder != parts[0]:
                            div_class = "subfolder"
                            html += f"""<div class="notebook">{InstallerReport.FOLDER_SVG} {parts[0]}</div>\n"""
                            previous_folder = parts[0]
                    elif len(parts) == 1:
                        div_class = ""
                    html += f"""<div class="notebook {div_class}">{InstallerReport.NOTEBOOK_SVG} <a href="{self.workspace_url}/#workspace{install_path}/{demo_name}/{n.get_clean_path()}">{path}</a>: <span class="path_desc">{n.title}</span></div>"""
            html += """</div>"""
        if len(pipelines_ids) > 0:
            html += f"""<h2>Delta Live Table Pipelines</h2><ul>"""
            for p in pipelines_ids:
                if 'error' in p:
                    html += f"""<li>{p['name']}: Installation error</li>"""
                else:
                    html += f"""<li><a href="{self.workspace_url}/#joblist/pipelines/{p['uid']}">{p['name']}</a></li>"""
            html +="</ul>"
        if len(dashboards) > 0:
            html += f"""<h2>DBSQL Dashboards</h2><div class="container_dbdemos">"""
            for d in dashboards:
                if "error" in d:
                    error_already_installed  = ""
                    if d["installed_id"] is not None:
                        error_already_installed = f""" A dashboard with the same name exists: <a href="{self.workspace_url}/sql/dashboards/{d['installed_id']}">{d['name']}</a>"""
                    html += f"""<div>ERROR INSTALLING DASHBOARD {d['name']}: {d['error']}. The Import/Export API must be enabled.{error_already_installed}</div>"""
                else:
                    html += f"""<div>{InstallerReport.DASHBOARD_SVG} <a href="{self.workspace_url}/sql/dashboards/{d['installed_id']}">{d['name']}</a></div>"""
            html +="</div>"
        if len(workflows) > 0:
            html += f"""<h2>Workflows</h2><ul>"""
            for w in workflows:
                if w['run_id'] is not None:
                    html += f"""We created and started a <a href="{self.workspace_url}/#job/{w['uid']}/run/{w['run_id']}">workflow</a> as part of your demo !"""
                else:
                    html += f"""We created a <a href="{self.workspace_url}/#job/{w['uid']}">workflow</a> as part of your demo !"""
            html +="</ul>"
        if job_id is not None:
            html += f"""<h2>Initialization job started</h2>
                        <div style="background-color: #e8f1ff; padding: 10px">
                            We started a <a href="{self.workspace_url}/#job/{job_id}/run/{run_id}">job to initialize your demo data</a> (for DBSQL Dashboards & Delta Live Table). 
                            <strong>Please wait for the job completion to be able to access the dataset & dashboards...</strong>
                        </div>"""
        html += cluster_section+"</div>"
        return html

    def display_install_result_html(self, demo_name, description, title, install_path = None, notebooks = [], job_id = None, run_id = None, cluster_id = None, cluster_name = None, pipelines_ids = [], dashboards = [], workflows = []):
        from dbruntime.display import displayHTML
        html = self.get_install_result_html(demo_name, description, title, install_path, notebooks, job_id, run_id, cluster_id, cluster_name, pipelines_ids, dashboards, workflows)
        displayHTML(html)

    def display_install_result_console(self, demo_name, description, title, install_path = None, notebooks = [], job_id = None, run_id = None, cluster_id = None, cluster_name = None, pipelines_ids = [], dashboards = [], workflows = []):
        if len(notebooks) > 0:
            print("----------------------------------------------------")
            print("-------------- Notebook installed: -----------------")
            for n in notebooks:
                if "_resources" not in n.get_clean_path():
                    print(f"   - {n.title}: {self.workspace_url}/#workspace{install_path}/{demo_name}/{n.get_clean_path()}")
        if job_id is not None:
            print("----------------------------------------------------")
            print("--- Job initialization started (load demo data): ---")
            print(f"    - Job run available under: {self.workspace_url}/#job/{job_id}/run/{run_id}")
        if cluster_id is not None:
            print("----------------------------------------------------")
            print("------------ Demo interactive cluster: -------------")
            print(f"    - {cluster_name}: {self.workspace_url}/#setting/clusters/{cluster_id}/configuration")
            cluster_instruction = f" using the cluster {cluster_name}"
        else:
            cluster_instruction = ""
        if len(pipelines_ids) > 0:
            print("----------------------------------------------------")
            print("------------ Delta Live Table available: -----------")
            for p in pipelines_ids:
                if 'error' in p:
                    print(f"    - {p['name']}: Installation error")
                else:
                    print(f"    - {p['name']}: {self.workspace_url}/#joblist/pipelines/{p['uid']}")
        if len(dashboards) > 0:
            print("----------------------------------------------------")
            print("------------- DBSQL Dashboard available: -----------")
            for d in dashboards:
                error_already_installed  = ""
                if d["installed_id"] is not None:
                    error_already_installed = f""" A dashboard with the same name exists: <a href="{self.workspace_url}/sql/dashboards/{d['installed_id']}">{d['name']}</a>"""
                if "error" in d:
                    print(f"    - ERROR INSTALLING DASHBOARD {d['name']}: {d['error']}. The Import/Export API must be enabled.{error_already_installed}")
                else:
                    print(f"    - {d['name']}: {self.workspace_url}/sql/dashboards/{d['installed_id']}")
        if len(workflows) > 0:
            print("----------------------------------------------------")
            print("-------------------- Workflows: --------------------")
            for w in workflows:
                if w['run_id'] is not None:
                    print(f"""We created and started a workflow as part of your demo: {self.workspace_url}/#job/{w['uid']}/run/{w['run_id']}""")
                else:
                    print(f"""We created a workflow as part of your demo: {self.workspace_url}/#job/{w['uid']}/tasks""")
        print("----------------------------------------------------")
        print(f"Your demo {title} is ready! ")
        if len(notebooks) > 0:
            first = list(filter(lambda n: "/" not in n.get_clean_path(), notebooks))
            first.sort(key=lambda n: n.get_clean_path())
            print(f"Start with the first notebook {demo_name}/{first[0].get_clean_path()}{cluster_instruction}: {self.workspace_url}/#workspace{install_path}/{demo_name}/{first[0].get_clean_path()}.")
