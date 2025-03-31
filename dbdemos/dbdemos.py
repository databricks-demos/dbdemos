from .exceptions.dbdemos_exception import TokenException
from .installer import Installer
from collections import defaultdict

from .installer_report import InstallerReport

CSS_LIST = """
<style>
.dbdemo {
  font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji,FontAwesome;
  color: #3b3b3b;
  padding: 0px 0px 20px 0px;
}
.dbdemo_box {
  width: 400px;
  padding: 10px;
  box-shadow: 0 .15rem 1.15rem 0 rgba(58,59,69,.15)!important;
  float: left;
  min-height: 170px;
  margin: 0px 20px 20px 20px;
}
.dbdemo_category {
  clear: both;
}
.category {
  margin-left: 20px;
  margin-bottom: 5px;

}
.dbdemo_logo {
  width: 100%;
  height: 225px;
}
.code {
  padding: 5px;
  border: 1px solid #e4e4e4;
  font-family: monospace;
  background-color: #f5f5f5;
  margin: 5px 0px 0px 0px;
}
.dbdemo_description {
  height: 100px;
}
.menu_button {
  font-size: 15px;
  cursor: pointer;
  border: 0px;
  padding: 10px 20px 10px 20px;
  margin-right: 10px;
  background-color: rgb(238, 237, 233);
  border-radius: 20px;
}
.menu_button:hover {
  background-color: rgb(245, 244, 242)
}
.menu_button.selected {
  background-color: rgb(158, 214, 196)
}
.new_tag {
  background-color: red;
  color: white;
  font-size: 13px;
  padding: 2px 7px;
  border-radius: 3px;
  margin-right: 5px;
}
</style>
"""

JS_LIST = """<script>
    const buttons = document.querySelectorAll('.menu_button');
    const sections = document.querySelectorAll('.dbdemo_category');

    buttons.forEach(button => {
        button.addEventListener('click', () => {
            const selectedCategory = button.getAttribute('category');

            sections.forEach(section => {
                if (section.id === `category-${selectedCategory}`) {
                    section.style.display = 'block';
                } else {
                    section.style.display = 'none';
                }
            });

            buttons.forEach(btn => {
                if (btn === button) {
                    btn.classList.add('selected');
                } else {
                    btn.classList.remove('selected');
                }
            });
        });
    });
</script>"""
def help():
    installer = Installer()
    if installer.report.displayHTML_available():
        from dbruntime.display import displayHTML
        displayHTML("""<style>
            .dbdemos_install{
                font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji,FontAwesome;
                color: #3b3b3b;
                box-shadow: 0 .15rem 1.15rem 0 rgba(58,59,69,.15)!important;
                padding: 10px;
                margin: 10px;
            }
            .code {
                padding: 0px 5px;
                border: 1px solid #e4e4e4;
                font-family: monospace;
                background-color: #f5f5f5;
                margin: 5px 0px 0px 0px;
                display: inline;
            }
            </style>
            <div class="dbdemos_install">
              <h1>DBDemos</h1>
              <i>Install databricks demos: notebooks, Delta Live Table Pipeline, DBSQL Dashboards, ML Models etc.</i>
              <ul>
                <li>
                  <div class="code">dbdemos.help()</div>: display help.<br/><br/>
                </li>
                <li>
                  <div class="code">dbdemos.list_demos(category: str = None)</div>: list all demos available, can filter per category (ex: 'governance').<br/><br/>
                </li>
                <li>
                  <div class="code">dbdemos.install(demo_name: str, path: str = "./", overwrite: bool = False, use_current_cluster = False, username: str = None, pat_token: str = None, workspace_url: str = None, skip_dashboards: bool = False, cloud: str = "AWS", catalog: str = None, schema: str = None, serverless: bool = None, warehouse_name: str = None, skip_genie_rooms: bool = False, policy_id: str = None, cluster_custom_settings: dict = None)</div>: install the given demo to the given path.<br/><br/>
                  <ul>
                  <li>If overwrite is True, dbdemos will delete the given path folder and re-install the notebooks.</li>
                  <li>use_current_cluster = True will not start a new cluster to init the demo but use the current cluster instead. <strong>Set it to True it if you don't have cluster creation permission</strong>.</li>
                  <li>skip_dashboards = True will not load the DBSQL dashboard if any (faster, use it if the dashboard generation creates some issue).</li>                  
                  <li>If no authentication are provided, dbdemos will use the current user credential & workspace + cloud to install the demo.</li>
                  <li>catalog and schema options let you chose where to load the data and other assets.</li>
                  <li>Dashboards require a warehouse, you can specify it with the warehouse_name='xx' option.</li>
                  <li>Dbdemos will detect serverless compute and use the current cluster when you're running serverless. You can force it with the serverless=True option.</li>
                  <li>Genie rooms are in beta. You can skip the genie room installation with skip_genie_rooms = True.</li>
                  <li>policy_id will be used in the dlt (example: "0003963E5B551CE4"). Use it with cluster_custom_settings = {"autoscale": {"min_workers": 1, "max_workers": 5}} to respect the policy requirements.</li>
                  </ul><br/>
                </li>
                <li>
                  <div class="code">dbdemos.create_cluster(demo_name: str)</div>: install update the interactive cluster for the demo (scoped to the user).<br/><br/>
                </li>
                <li>
                  <div class="code">dbdemos.install_all(path: str = "./", overwrite: bool = False, username: str = None, pat_token: str = None, workspace_url: str = None, skip_dashboards: bool = False, cloud: str = "AWS")</div>: install all the demos to the given path.<br/><br/>
                </li>
               </ul>
            </div>""")
    else:
        print("------------ DBDemos ------------------")
        print("""dbdemos.help(): display help.""")
        print("""dbdemos.list_demos(category: str = None): list all demos available, can filter per category (ex: 'governance').""")
        print("""dbdemos.install(demo_name: str, path: str = "./", overwrite: bool = False, username: str = None, pat_token: str = None, workspace_url: str = None, skip_dashboards: bool = False, cloud: str = "AWS"): install the given demo to the given path.""")
        print("""dbdemos.create_cluster(demo_name: str): install update the interactive cluster for the demo (scoped to the user).""")
        print("""dbdemos.install_all(path: str = "./", overwrite: bool = False, username: str = None, pat_token: str = None, workspace_url: str = None, skip_dashboards: bool = False, cloud: str = "AWS")</div>: install all the demos to the given path.""")

def list_demos(category = None, installer = None, pat_token = None):
    check_version()
    deprecated_demos = ["uc-04-audit-log", "llm-dolly-chatbot"]
    if installer == None:
        installer = Installer(pat_token=pat_token)
    installer.tracker.track_list()
    demos = defaultdict(lambda: [])
    #Define category order
    demos["lakehouse"] = []
    demos["data-engineering"] = []
    demos["governance"] = []
    demos["DBSQL"] = []
    demos["data-science"] = []
    demos["AI-BI"] = []
    for demo in installer.get_demos_available():
        conf = installer.get_demo_conf(demo)
        if (category is None or conf.category == category.lower()) and conf.name not in deprecated_demos:
            demos[conf.category].append(conf)
    if installer.report.displayHTML_available():
        content = get_html_list_demos(demos)
        from dbruntime.display import displayHTML
        displayHTML(content)
    else:
        list_console(demos)

def get_html_list_demos(demos):
    categories = list(demos.keys())
    content = f"""{CSS_LIST}<div class="dbdemo">
     <div style="padding: 10px 0px 20px 20px">"""
    for i, cat in enumerate(categories):
        content += f"""<button category="{cat}" class="menu_button {"selected" if i == 0 else ""}" type="button">{f'<span class="new_tag">NEW!</span>' if cat == 'AI-BI' else ''}<span>{cat.capitalize()}</span></button>"""
    content += """</div>"""
    for i, cat in enumerate(categories):
        content += f"""<div class="dbdemo_category" style="min-height: 200px; display: {"block" if i == 0 else "none"}" id="category-{cat}">"""
        ds = list(demos[cat])
        ds.sort(key=lambda d: d.name)
        for demo in ds:
            content += f"""
            <div class="dbdemo_box">
              <img class="dbdemo_logo" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/icon/{demo.name}.jpg" />
              <div class="dbdemo_description">
                <h2>{demo.title}</h2>
                {demo.description}
              </div>
              <div class="code"> 
                dbdemos.install('{demo.name}')
              </div>
            </div>"""
        content += """</div>"""
    content += f"""</div>{JS_LIST}"""
    return content


def list_console(demos):
    print("----------------------------------------------------")
    print("----------------- Demos Available ------------------")
    print("----------------------------------------------------")
    categories = list(demos.keys())
    for cat in categories:
        print(f"{cat.capitalize()}")
        ds = list(demos[cat])
        ds.sort(key=lambda d: d.name)
        for demo in ds:
            print(f"   - {demo.name}: {demo.title} ({demo.description}) => dbdemos.install('{demo.name}')")
        print("")
    print("----------------------------------------------------")

def list_delta_live_tables(category = None):
    pass

def list_dashboards(category = None):
    pass

def install(demo_name, path = None, overwrite = False, username = None, pat_token = None, workspace_url = None, skip_dashboards = False, cloud = "AWS", start_cluster: bool = None,
            use_current_cluster: bool = False, current_cluster_id = None, warehouse_name = None, debug = False, catalog = None, schema = None, serverless=None, skip_genie_rooms=False, 
            create_schema=True, policy_id = None, cluster_custom_settings = None):
    check_version()
    if demo_name == "lakehouse-retail-churn":
        print("WARN: lakehouse-retail-churn has been renamed to lakehouse-retail-c360")
        demo_name = "lakehouse-retail-c360"
    try:
        installer = Installer(username, pat_token, workspace_url, cloud, current_cluster_id = current_cluster_id)
    except TokenException as e:
        report = InstallerReport(workspace_url)
        report.display_token_error(e, demo_name)

    if not installer.test_premium_pricing():
        #Force dashboard skip as dbsql isn't available to avoid any error.
        skip_dashboards = True
    installer.install_demo(demo_name, path, overwrite, skip_dashboards = skip_dashboards, start_cluster = start_cluster, use_current_cluster = use_current_cluster,
                           debug = debug, catalog = catalog, schema = schema, serverless = serverless, warehouse_name=warehouse_name, skip_genie_rooms=skip_genie_rooms, create_schema=create_schema, policy_id = policy_id, cluster_custom_settings = cluster_custom_settings)


def install_all(path = None, overwrite = False, username = None, pat_token = None, workspace_url = None, skip_dashboards = False, cloud = "AWS", start_cluster = None, use_current_cluster = False, catalog = None, schema = None, policy_id = None, cluster_custom_settings = None):
    """
    Install all the bundle demos.
    """
    installer = Installer(username, pat_token, workspace_url, cloud)
    for demo_name in installer.get_demos_available():
        installer.install_demo(demo_name, path, overwrite, skip_dashboards = skip_dashboards, start_cluster = start_cluster, use_current_cluster = use_current_cluster, catalog = catalog, schema = schema, policy_id = policy_id, cluster_custom_settings = cluster_custom_settings)

def check_status_all(username = None, pat_token = None, workspace_url = None, cloud = "AWS"):
    """
    Check all dbdemos bundle demos installation status (see #check_status)
    """
    installer = Installer(username, pat_token, workspace_url, cloud)
    for demo_name in installer.get_demos_available():
        check_status(demo_name, username, pat_token, workspace_url, cloud)

def check_status(demo_name:str, username = None, pat_token = None, workspace_url = None, cloud = "AWS", catalog = None, schema = None):
    """
    Check the status of the given demo installation. Will pool the installation job if any and wait for its completion.
    Throw an error if the job wasn't successful.
    """
    installer = Installer(username, pat_token, workspace_url, cloud)
    demo_conf = installer.get_demo_conf(demo_name, catalog, schema)
    if schema is None:
        schema = demo_conf.default_schema
    if catalog is None:
        catalog = demo_conf.default_catalog
    if "settings" in demo_conf.init_job:
        job_name = demo_conf.init_job["settings"]["name"]
        existing_job = installer.db.find_job(job_name)
        if existing_job == None:
            raise Exception(f"Couldn't find job for demo {demo_name}. Did you install it first?")
        installer.installer_workflow.wait_for_run_completion(existing_job['job_id'], debug=True)
        runs = installer.db.get("2.1/jobs/runs/list", {"job_id": existing_job['job_id'], "limit": 1})
        if runs['runs'][0]['state']['result_state'] != "SUCCESS":
            raise Exception(f"Job {existing_job['job_id']} for demo {demo_name} failed: {installer.db.conf.workspace_url}/#job/{existing_job['job_id']}/run/{runs['runs'][0]['run_id']} - {runs}")


def create_cluster(demo_name, username = None, pat_token = None, workspace_url = None, cloud = "AWS"):
    installer = Installer(username, pat_token, workspace_url, cloud = cloud)
    installer.check_demo_name(demo_name)
    print(f"Updating cluster for demo {demo_name}...")
    demo_conf = installer.get_demo_conf(demo_name)
    installer.tracker.track_create_cluster(demo_conf.category, demo_name)
    cluster_id, cluster_name = installer.load_demo_cluster(demo_name, demo_conf, True)
    installer.report.display_install_result(demo_name, demo_conf.description, demo_conf.title, cluster_id = cluster_id, cluster_name = cluster_name)


def check_version():
    """
    Check if a newer version of dbdemos is available on PyPI.
    Prints a warning if the installed version is outdated.
    """
    try:
        import pkg_resources
        import requests
        import json
        
        # Get installed version
        installed_version = pkg_resources.get_distribution('dbdemos').version
        
        # Get latest version from PyPI
        pypi_response = requests.get("https://pypi.org/pypi/dbdemos/json")
        latest_version = json.loads(pypi_response.text)['info']['version']
        
        # Compare versions
        if pkg_resources.parse_version(latest_version) > pkg_resources.parse_version(installed_version):
            print(f"\nWARNING: You are using dbdemos version {installed_version}, however version {latest_version} is available. You should consider upgrading:")
            print("%pip install --upgrade dbdemos")
            print("dbutils.library.restartPython()")
            
    except Exception as e:
        # Silently handle any errors during version check
        pass
