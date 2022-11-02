from .installer import Installer
from collections import defaultdict

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
  float: right;
  width: 50px;
  height: 50px;
}
.code {
  padding: 5px;
  border: 1px solid #e4e4e4;
  font-family: monospace;
  background-color: #f5f5f5;
  margin: 5px 0px 0px 0px;
}
.dbdemo_tag {
    display: inline;
    border-radius: 5px;
    color: white;
    padding: 3px;
}

.dbdemo_tag.dbdemo_delta {
    background-color: #00b7dc;
}
.dbdemo_tag.dbdemo_dbsql {
    background-color: #bf49e8;
}
.dbdemo_tag.dbdemo_ds {
    background-color: #4ab970;
}
.dbdemo_tag.dbdemo_uc {
    background-color: #3664ff;
}
.dbdemo_tag.dbdemo_dlt {
    background-color: #f58742;
}
.dbdemo_tag.dbdemo_autoloader {
    background-color: #e36132;
}
.dbdemo_tags {
  padding-top: 10px;
}
.dbdemo_description {
  height: 95px;
}
</style>
"""

def help():
    installer = Installer()
    if installer.displayHTML_available():
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
                padding: 5px;
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
                  <div class="code">dbdemos.install_demo(demo_name: str, path: str = "./", overwrite: bool = False, username: str = None, pat_token: str = None, workspace_url: str = None)</div>: install the given demo to the given path.<br/><br/>
                  If overwrite is True, will delete the given folder and re-install the notebooks.<br/>
                  If no authentication are provided, will use the current user credential & workspace to install the demo.<br/><br/>
                </li>
                <li>
                  <div class="code">dbdemos.create_cluster(demo_name: str)</div>: install update the interactive cluster for the demo (scoped to the user).<br/><br/>
                </li>
               </ul>
            </div>""")
    else:
        print("------------ DBDemos ------------------")
        print("""dbdemos.help(): display help.""")
        print("""dbdemos.list_demos(category: str = None): list all demos available, can filter per category (ex: 'governance').""")
        print("""dbdemos.install_demo(demo_name: str, path: str = "./", overwrite: bool = False, username: str = None, pat_token: str = None, workspace_url: str = None): install the given demo to the given path.""")
        print("""dbdemos.create_cluster(demo_name: str): install update the interactive cluster for the demo (scoped to the user).""")

def list_demos(category = None):
    installer = Installer()
    installer.tracker.track_list()
    demos = defaultdict(lambda: [])
    for demo in installer.get_demos_available():
        conf = installer.get_demo_conf(demo)
        if category is None or conf.category == category.lower():
            demos[conf.category].append(conf)
    if installer.displayHTML_available():
        list_html(demos)
    else:
        list_console(demos)

def list_html(demos):
    content = f"""{CSS_LIST}<div class="dbdemo">"""
    categories = list(demos.keys())
    categories.sort()
    for cat in categories:
        content += f"""<div class="dbdemo_category" style="min-height: 200px">
                       <h1 class="category">{cat.capitalize()}</h1>"""
        ds = list(demos[cat])
        ds.sort(key=lambda d: d.name)
        for demo in ds:
            content += f"""
            <div class="dbdemo_box">
              <img class="dbdemo_logo" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/resources/{demo.name}.png" />
              <div class="dbdemo_description">
                <h2>{demo.title}</h2>
                {demo.description}
              </div>
              <div class="code"> 
                dbdemos.install('{demo.name}')
              </div>
              <div class="dbdemo_tags">
                {" ".join([f'<div class="dbdemo_tag dbdemo_{list(t.keys())[0]}">{t[list(t.keys())[0]]}</div>' for t in demo.tags])}
              </div>
            </div>"""
        content += """</div>"""
    content += """</div>"""
    from dbruntime.display import displayHTML
    displayHTML(content)


def list_console(demos):
    print("----------------------------------------------------")
    print("----------------- Demos Available ------------------")
    print("----------------------------------------------------")
    categories = list(demos.keys())
    categories.sort()
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

def install(demo_name, path = None, overwrite = False, username = None, pat_token = None, workspace_url = None):
    installer = Installer(username, pat_token, workspace_url)
    installer.install_demo(demo_name, path, overwrite)


def create_cluster(demo_name, username = None, pat_token = None, workspace_url = None):
    installer = Installer(username, pat_token, workspace_url)
    installer.check_demo_name(demo_name)
    print(f"Updating cluster for demo {demo_name}...")
    demo_conf = installer.get_demo_conf(demo_name)
    installer.tracker.track_create_cluster(demo_conf.category, demo_name)
    cluster_id, cluster_name = installer.load_demo_cluster(demo_name, demo_conf, True)
    installer.display_install_result(demo_name, demo_conf.description, demo_conf.title, cluster_id = cluster_id, cluster_name = cluster_name)
