from dbdemos.conf import DemoConf

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
        content = json.loads(self.content)
        #force the position to avoid bug during import
        for i in range(len(content["commands"])):
            content["commands"][i]['position'] = i
        content = json.dumps(content)
        content = urllib.parse.quote(content, safe="()*''")
        return self.html.replace(self.raw_content, base64.b64encode(content.encode('utf-8')).decode('utf-8'))

    def contains(self, str):
        return str in self.content

    def remove_static_settings(self):
        #Remove the static settings tags are it's too big & unecessary to repeat in each notebook.
        self.html = re.sub("""<script>\s?window\.__STATIC_SETTINGS__.*</script>""", "", self.html)

    def set_tracker_tag(self, org_id, uid, category, demo_name, notebook, username):
        #Replace internal tags with dbdemos
        if Tracker.enable_tracker:
            tracker = Tracker(org_id, uid, username)
            #Our demos in the repo already have tags used when we clone the notebook directly.
            #We need to update the tracker with the demo configuration & dbdemos setup.
            tracker_url = tracker.get_track_url(category, demo_name, "VIEW", notebook)
            r = r"""(<img\s*width=\\?"1px\\?"\s*src=\\?")(https:\/\/ppxrzfxige\.execute-api\.us-west-2\.amazonaws\.com\/v1\/analytics.*?)(\\?"\s?\/?>)"""
            self.content = re.sub(r, rf'\1{tracker_url}\3', self.content)

            #old legacy tracker, to be migrted & emoved
            r = r"""(<img\s*width=\\?"1px\\?"\s*src=\\?")(https:\/\/www\.google-analytics\.com\/collect.*?)(\\?"\s?\/?>)"""
            self.content = re.sub(r, rf'\1{tracker_url}\3', self.content)
        else:
            #Remove all the tracker from the notebook
            self.replace_in_notebook(r"""<img\s*width=\\?"1px\\?"\s*src=\\?"https:\/\/www\.google-analytics\.com\/collect.*?\\?"\s?\/?>""", "", True)
            self.replace_in_notebook(r"""<img\s*width=\\?"1px\\?"\s*src=\\?"https:\/\/ppxrzfxige\.execute-api\.us-west-2\.amazonaws\.com\/v1\/analytics.*?\\?"\s?\/?>""", "", True)

    def remove_uncomment_tag(self):
        self.replace_in_notebook('[#-]{1,2}\s*UNCOMMENT_FOR_DEMO ?', '', True)

    ##Remove the __build to avoid catalog conflict during build vs test
    # TODO: improve build and get a separate metastore for tests vs build.
    def remove_dbdemos_build(self):
        self.replace_in_notebook('dbdemos__build', 'dbdemos')

    def remove_robots_meta(self):
        #Drop the noindex tag
        self.html = self.html.replace('<meta name="robots" content="nofollow, noindex">', '')

    def add_cell_as_html_for_seo(self):
        #Add div as hidden HTML for SEO to capture the main information in the page.
        def md_to_html(text):
            if text.startswith('%md-sandbox'):
                text = text[len('%md-sandbox'):]
            if text.startswith('%md'):
                text = text[len('%md'):]
            #quick translation to html for seo
            for i in reversed(range(1,6)):
                tag = "#"*i
                text = re.sub(rf'\s*{tag}\s*(.*)', rf'<h{i}>\1</h{i}>', text)
            text = text.replace('\n', '<br/>')
            return text
        #Drop the noindex tag
        content = json.loads(self.content)
        html = ""
        for c in content["commands"]:
            if c['command'].startswith('%md'):
                html += '<div>'+md_to_html(c['command'])+'</div>'
        if len(html) > 0:
            self.html = self.html.replace('<body>', f'''<body><div id='no_js_render' style='display: none'>{html}</div>''')
            self.html = self.html.replace('<script>', "<script>window.addEventListener('load', function(event) { "
                                                        "if (/bot|google|baidu|bing|msn|teoma|slurp|yandex/i.test(navigator.userAgent)) {"
                                                            "document.getElementById('no_js_render').style.display = 'block';"
                                                        "};"
                                                      "});", 1)

    @staticmethod
    def _replace_with_optional_escaped_quotes(content: str, old: str, new: str) -> str:
        """
        Helper to replace text handling both escaped and unescaped quotes.
        In JSON content, quotes are escaped as \", but in parsed content they're not.
        We handle both by trying replacements with escaped quotes first, then unescaped.
        This is much faster than using regex.
        """
        # Try with escaped quotes first (JSON format: \")
        old_escaped = old.replace('"', '\\"')
        new_escaped = new.replace('"', '\\"')
        content = content.replace(old_escaped, new_escaped)

        # Then try with unescaped quotes (plain text format: ")
        content = content.replace(old, new)

        return content

    @staticmethod
    def replace_schema_in_content(content: str, demo_conf: DemoConf) -> str:
        """
        Static method to replace schema/catalog references in any content string.
        Used for both notebook content and FILE object types.
        """
        #main__build is used during the build process to avoid collision with default main.
        # #main_build is used because agent don't support __ in their catalog name - TODO should improve this and move everything to main_build
        content = NotebookParser._replace_with_optional_escaped_quotes(content, 'catalog = "main__build"', 'catalog = "main"')
        content = NotebookParser._replace_with_optional_escaped_quotes(content, 'catalog = "main_build"', 'catalog = "main"')
        content = content.replace(f'main__build.{demo_conf.default_schema}', f'main.{demo_conf.default_schema}')
        content = content.replace(f'main_build.{demo_conf.default_schema}', f'main.{demo_conf.default_schema}')
        content = content.replace('Volumes/main__build', 'Volumes/main')
        content = content.replace('Volumes/main_build', 'Volumes/main')

        #TODO we need to unify this across all demos.
        if demo_conf.custom_schema_supported:
            content = re.sub(r"\$catalog=[0-9a-z_]*\s{1,3}\$schema=[0-9a-z_]*", f"$catalog={demo_conf.catalog} $schema={demo_conf.schema}", content)
            content = re.sub(r"\$catalog=[0-9a-z_]*\s{1,3}\$db=[0-9a-z_]*", f"$catalog={demo_conf.catalog} $db={demo_conf.schema}", content)
            content = content.replace(f"{demo_conf.default_catalog}.{demo_conf.default_schema}", f"{demo_conf.catalog}.{demo_conf.schema}")
            content = NotebookParser._replace_with_optional_escaped_quotes(content, f'dbutils.widgets.text("catalog", "{demo_conf.default_catalog}"', f'dbutils.widgets.text("catalog", "{demo_conf.catalog}"')
            content = NotebookParser._replace_with_optional_escaped_quotes(content, f'dbutils.widgets.text("schema", "{demo_conf.default_schema}"', f'dbutils.widgets.text("schema", "{demo_conf.schema}"')
            content = NotebookParser._replace_with_optional_escaped_quotes(content, f'dbutils.widgets.text("db", "{demo_conf.default_schema}"', f'dbutils.widgets.text("db", "{demo_conf.schema}"')
            content = content.replace(f'Volumes/{demo_conf.default_catalog}/{demo_conf.default_schema}', f'Volumes/{demo_conf.catalog}/{demo_conf.schema}')

            content = NotebookParser._replace_with_optional_escaped_quotes(content, f'catalog = "{demo_conf.default_catalog}"', f'catalog = "{demo_conf.catalog}"')
            content = NotebookParser._replace_with_optional_escaped_quotes(content, f'dbName = db = "{demo_conf.default_schema}"', f'dbName = db = "{demo_conf.schema}"')
            content = NotebookParser._replace_with_optional_escaped_quotes(content, f'schema = dbName = db = "{demo_conf.default_schema}"', f'schema = dbName = db = "{demo_conf.schema}"')
            content = NotebookParser._replace_with_optional_escaped_quotes(content, f'db = "{demo_conf.default_schema}"', f'db = "{demo_conf.schema}"')
            content = NotebookParser._replace_with_optional_escaped_quotes(content, f'schema = "{demo_conf.default_schema}"', f'schema = "{demo_conf.schema}"')
            content = content.replace(f'USE SCHEMA {demo_conf.default_schema}', f'USE SCHEMA {demo_conf.schema}')
            content = content.replace(f'USE CATALOG {demo_conf.default_catalog}', f'USE CATALOG {demo_conf.catalog}')
            content = content.replace(f'CREATE CATALOG IF NOT EXISTS {demo_conf.default_catalog}', f'CREATE CATALOG IF NOT EXISTS {demo_conf.catalog}')
            content = content.replace(f'CREATE SCHEMA IF NOT EXISTS {demo_conf.default_schema}', f'CREATE SCHEMA IF NOT EXISTS {demo_conf.schema}')

        return content

    def replace_schema(self, demo_conf: DemoConf):
        """Replace schema/catalog in notebook content"""
        self.content = NotebookParser.replace_schema_in_content(self.content, demo_conf)

    def replace_in_notebook(self, old, new, regex = False):
        if regex:
            self.content = re.sub(old, new, self.content)
        else:
            self.content = self.content.replace(old, new)

    def add_extra_cell(self, cell_content, position = 1):
        command = {
            "version": "CommandV1",
            "bindings": {},
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
        if "display_automl_" in self.content:
            content = json.loads(self.content)
            for c in content["commands"]:
                if re.search('display_automl_[a-zA-Z]*_link', c["command"]):
                    if 'results' in c and c['results'] is not None and 'data' in c['results'] and c['results']['data'] is not None and len(c['results']['data']) > 0:
                        contains_exp_link = len([d for d in c['results']['data'] if 'Data exploration notebook' in d['data']]) > 0
                        if contains_exp_link:
                            c['results']['data'] = [{'type': 'ansi', 'data': 'Please run the notebook cells to get your AutoML links (from the begining)', 'name': None, 'arguments': {}, 'addedWidgets': {}, 'removedWidgets': [], 'datasetInfos': [], 'metadata': {}}]
            self.content = json.dumps(content)


    #Will change the content to
    def change_relative_links_for_minisite(self):
        #self.replace_in_notebook("""<a\s*(?:target="_blank")?\s*(?:rel="noopener noreferrer")?\s*href="\$\.\/(.*)">""", """<a href="./$1">""", True)
        self.replace_in_notebook("""\]\(\$\.\/(.*?)\)""", """](./\g<1>.html)""", True)


    def add_javascript_to_minisite_relative_links(self, notebook_path):
        # Add JavaScript to the HTML (not content) that intercepts link clicks
        # This is much more reliable than trying to modify the notebook content

        # Get the notebook's directory (remove filename)
        notebook_dir = '/'.join(notebook_path.split('/')[:-1])
        script = f"""
        <script type="text/javascript">
        (function() {{
            const NOTEBOOK_PATH = '{notebook_path}';
            const NOTEBOOK_DIR = '{notebook_dir}';

            function resolvePath(relativePath) {{
                // If path starts with /, it's absolute from root
                if (relativePath.startsWith('/')) {{
                    return relativePath.substring(1);
                }}

                // Otherwise, resolve relative to current notebook's directory
                if (!NOTEBOOK_DIR || NOTEBOOK_DIR === '') {{
                    return relativePath;
                }}

                // Combine directory with relative path
                let parts = NOTEBOOK_DIR.split('/').filter(p => p !== '');
                let pathParts = relativePath.split('/').filter(p => p !== '');

                for (let part of pathParts) {{
                    if (part === '..') {{
                        parts.pop();
                    }} else if (part !== '.') {{
                        parts.push(part);
                    }}
                }}

                return parts.join('/');
            }}

            function setupMinisiteLinks() {{
                // Find all links in the page
                const links = document.querySelectorAll('a[href]');

                links.forEach(function(link) {{
                    const href = link.getAttribute('href');

                    if (!href) return;

                    // Check if link has $ (internal demo link)
                    if (href.includes('$')) {{
                        // All $ links are relative to the current notebook directory
                        // Remove various prefixes: /$./  $./  /$.  $.
                        let relativePath = href;

                        // Remove /$./
                        if (relativePath.includes('/$' + './')) {{
                            relativePath = relativePath.replace('/$' + './', '');
                        }}
                        // Remove $./
                        else if (relativePath.includes('$' + './')) {{
                            relativePath = relativePath.replace('$' + './', '');
                        }}
                        // Remove /$.
                        else if (relativePath.includes('/$' + '.')) {{
                            relativePath = relativePath.replace('/$' + '.', '');
                        }}
                        // Remove $.
                        else if (relativePath.includes('$' + '.')) {{
                            relativePath = relativePath.replace('$' + '.', '');
                        }}
                        // Just remove $
                        else {{
                            relativePath = relativePath.replace('$', '');
                        }}

                        // Remove only a single leading ./ if present (preserve ../ for navigation)
                        if (relativePath.startsWith('./')) {{
                            relativePath = relativePath.substring(2);
                        }}

                        // Always resolve against notebook directory
                        let targetPath = resolvePath(relativePath) + '.html';

                        // Remove target and rel attributes immediately
                        link.removeAttribute('target');
                        link.removeAttribute('rel');

                        // Change href to # immediately to prevent navigation
                        link.setAttribute('href', '#');

                        // Store targetPath in data attribute for debugging
                        link.setAttribute('data-target-path', targetPath);

                        // Add click handler with capture phase
                        link.addEventListener('click', function(e) {{
                            e.preventDefault();
                            e.stopPropagation();

                            // Send message to parent window
                            if (window.parent && window.parent !== window) {{
                                window.parent.postMessage({{
                                    type: 'dbdemos-navigate',
                                    targetPath: targetPath
                                }}, '*');
                            }} else {{
                                window.location.href = targetPath;
                            }}

                            return false;
                        }}, true);
                    }} else if (!href.startsWith('#') && !href.startsWith('http')) {{
                        // Remove non-demo internal links (convert to plain text)
                        const text = document.createTextNode(link.textContent);
                        link.parentNode.replaceChild(text, link);
                    }}
                }});
            }}

            // Run when DOM is ready
            if (document.readyState === 'loading') {{
                document.addEventListener('DOMContentLoaded', setupMinisiteLinks);
            }} else {{
                setupMinisiteLinks();
            }}

            // Also run after a short delay to catch dynamically loaded content
            setTimeout(setupMinisiteLinks, 500);
        }})();
        </script>
        """

        # Insert the script before </body>
        self.html = self.html.replace('</body>', script + '</body>')

    #Set the environment metadata to the notebook.
    # TODO: might want to re-evaluate this once we move to ipynb format as it'll be set in the ipynb file, as metadata.
    def set_environement_metadata(self, client_version: str = "2"):
        content = json.loads(self.content)
        env_metadata = content.get("environmentMetadata", {})
        if env_metadata is None:
            env_metadata = {}
        if ("client" not in env_metadata or 
            env_metadata["client"] is None or 
            int(env_metadata["client"]) < int(client_version)):
            env_metadata["client"] = client_version
        content["environmentMetadata"] = env_metadata
        self.content = json.dumps(content)

    def hide_commands_and_results(self):
        #
        self.replace_in_notebook('e2-demo-tools', 'xxxx', True)
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

    def remove_delete_cell(self):
        content = json.loads(self.content)
        content["commands"] = [c for c in content["commands"] if "#dbdemos__delete_this_cell" not in c["command"].lower()]
        self.content = json.dumps(content)

    def replace_dynamic_links(self, items, name, link_path):
        if len(items) == 0:
            return
        matches = re.finditer(rf'<a\s*dbdemos-{name}-id=\\?[\'"](?P<item_id>.*?)\\?[\'"]\s*href=\\?[\'"].*?\/?{link_path}\/(?P<item_uid>[a-zA-Z0-9_-]*).*?>', self.content)
        for match in matches:
            item_id = match.groupdict()["item_id"]
            installed = False
            for i in items:
                if i["id"] == item_id:
                    installed = True
                    self.content = self.content.replace(match.groupdict()["item_uid"], str(i['uid']))
            if not installed:
                print(f'''ERROR: couldn't find {name} with dbdemos-{name}-id={item_id}''')


    def replace_dynamic_links_workflow(self, workflows):
        """
        Replace the links in the notebook with the workflow installed if any
        """
        self.replace_dynamic_links(workflows, "workflow", "#job")

    def replace_dynamic_links_repo(self, repos):
        for r in repos:
            if r["uid"].startswith("/"):
                r["uid"] = r["uid"][1:]
        """
        Replace the links in the notebook with the repos installed if any
        """
        self.replace_dynamic_links(repos, "repo", "#workspace")

    def replace_dynamic_links_pipeline(self, pipelines_id):
        """
        Replace the links in the notebook with the SDP pipeline installed if any
        """
        self.replace_dynamic_links(pipelines_id, "pipeline", "#joblist/pipelines")


    def replace_dynamic_links_lakeview_dashboards(self, dashboards_id):
        """
        Replace the links in the notebook with the Lakeview dashboard installed if any
        """
        self.replace_dynamic_links(dashboards_id, "dashboard", "/sql/dashboardsv3")


    def replace_dynamic_links_genie(self, genie_rooms):
        """
        Replace the links in the notebook with the Genie room installed if any
        """
        self.replace_dynamic_links(genie_rooms, "genie", "/genie/rooms")