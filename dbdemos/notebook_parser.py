from .tracker import Tracker
import urllib
import re
import base64
import json

class NotebookParser:

    def __init__(self, html):
        self.html = html
        self.raw_content, self.content = self.get_notebook_content(html)

    def get_notebook_content(self, html):
        match = re.search(r'__DATABRICKS_NOTEBOOK_MODEL = \'(.*?)\'', html)
        raw_content = match.group(1)
        content = base64.b64decode(raw_content).decode('utf-8')
        content = urllib.parse.unquote(content)
        return raw_content, content

    def get_html(self):
        content = urllib.parse.quote(self.content, safe="()*''")
        return self.html.replace(self.raw_content, base64.b64encode(content.encode('utf-8')).decode('utf-8'))

    def contains(self, str):
        return str in self.content

    def remove_static_settings(self):
        #Remove the static settings tags are it's too big & unecessary to repeat in each notebook.
        self.html = re.sub("""<script>\s?window\.__STATIC_SETTINGS__.*</script>""", "", self.html)

    def set_tracker_tag(self, org_id, uid, category, demo_name, notebook):
        #Replace internal tags with dbdemos
        if Tracker.enable_tracker:
            tracker = Tracker(org_id, uid)
            #Our demos in the repo already have tags used when we clone the notebook directly.
            #We need to update the tracker with the demo configuration & dbdemos setup.
            r = r"""(<img\s*width=\\?"1px\\?"\s*src=\\?")(https:\/\/www\.google-analytics\.com\/collect.*?)(\\?"\s?\/?>)"""
            tracker_url = tracker.get_track_url(category, demo_name, "VIEW", notebook)
            self.content = re.sub(r, rf'\1{tracker_url}\3', self.content)
        else:
            #Remove all the tracker from the notebook
            self.replace_in_notebook(r"""<img\s*width=\\?"1px\\?"\s*src=\\?"https:\/\/www\.google-analytics\.com\/collect.*?\\?"\s?\/?>""", "", True)

    def remove_uncomment_tag(self):
        self.replace_in_notebook('[#-]{1,2}\s*UNCOMMENT_FOR_DEMO ?', '', True)

    def replace_in_notebook(self, old, new, regex = False):
        if regex:
            self.content = re.sub(old, new, self.content)
        else:
            self.content = self.content.replace(old, new)

    def get_dashboard_ids(self):
        pattern = re.compile(r'\/sql\/dashboards\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.IGNORECASE)
        return pattern.findall(self.content)

    def add_extra_cell(self, cell_content, position = 1):
        command = {
            "version": "CommandV1",
            "subtype": "command",
            "commandType": "auto",
            "position": position,
            "command": cell_content
        }
        content = json.loads(self.content)
        content["commands"].insert(position, command)
        self.content = json.dumps(content)

    #as auto ml links are unique per workspace, we have to delete them
    def remove_automl_result_links(self):
        content = json.loads(self.content)
        for c in content["commands"]:
            if re.search('display_[a-zA-Z]*_churn_link', c["command"]):
                if 'results' in c and 'data' in c['results'] and len(c['results']['data']) > 0 and 'data' in c['results']['data'][0]:
                    c['results']['data'][0]['data'] = 'Please run the notebook cells to get your AutoML links (from the begining)'
        self.content = json.dumps(content)


    def add_ga_website_tracker(self):
        if Tracker.enable_tracker:
            tracker = f"""
            <head>
            <script async src="https://www.googletagmanager.com/gtag/js?id={Tracker.website_tracker_id}"></script>
                <script>
                window.dataLayer = window.dataLayer || [];
                function gtag(){{dataLayer.push(arguments);}}
                gtag('js', new Date());
            
                gtag('config', '{Tracker.website_tracker_id}');
            </script>"""
        self.html = re.sub("""<head>""", tracker, self.html)

    def hide_commands_and_results(self):
        content = json.loads(self.content)
        for c in content["commands"]:
            if "#hide_this_code" in c["command"].lower():
                c["hideCommandCode"] = True
            if "%run " in c["command"]:
                c["hideCommandResult"] = True
            if "results" in c and  c["results"] is not None and "data" in c["results"] and c["results"]["data"] is not None and \
                c["results"]["type"] == "table" and len(c["results"]["data"])>0 and str(c["results"]["data"][0][0]).startswith("This Delta Live Tables query is syntactically valid"):
                c["hideCommandResult"] = True
        self.content = json.dumps(content)

    def replace_dashboard_links(self, dashboards):
        def replace_link_with_error(pattern, c):
            for p in pattern.findall(c["command"]):
                c["command"] = c["command"].replace(p[0], f"{p[1]}: ERROR - could not load the dashboard {d['name']}. {d['error']}")
        if "sql/dashboards" in self.content:
            content = json.loads(self.content)
            for d in dashboards:
                pattern1 = re.compile(rf'\[(.*?)\]\(\/sql\/dashboards\/{d["id"]}.*?\)', re.IGNORECASE)
                pattern2 = re.compile(rf'(<a.*?\/sql\/dashboards\/{d["id"]}.*?>(.*?)</a>)', re.IGNORECASE)
                for c in content["commands"]:
                    if "sql/dashboards" in c["command"]:
                        if d["installed_id"] is None:
                            replace_link_with_error(pattern1, c)
                            replace_link_with_error(pattern2, c)
                        else:
                            c["command"] = c["command"].replace(d['id'], d["installed_id"])
            self.content = json.dumps(content)

    def replace_dynamic_links_pipeline(self, pipelines_id):
        matches = re.finditer(r'<a\s*dbdemos-pipeline-id=\\?"(?P<dlt_id>.*?)\\?"\s*href=\\?".*?\/pipelines\/(?P<dlt_uid>[a-z0-9-]*).*?>', self.content)
        for match in matches:
            pipeline_id = match.groupdict()["dlt_id"]
            installed = False
            for p in pipelines_id:
                if p["id"] == pipeline_id:
                    installed = True
                    self.content = self.content.replace(match.groupdict()["dlt_uid"], p['uid'])
            if not installed:
                print(f'''ERROR: couldn't find DLT pipeline with dbdemos-pipeline-id={pipeline_id}''')