from .conf import DemoConf
import pkg_resources

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .installer import Installer


class InstallerDashboard:
    def __init__(self, installer: 'Installer'):
        self.installer = installer
        self.db = installer.db

    def install_dashboards(self, demo_conf: DemoConf, install_path, warehouse_name = None, debug=True):
        if len(demo_conf.dashboards) > 0:
            try:
                if debug:
                    print(f'installing {len(demo_conf.dashboards)} dashboards...')
                installed_dash = [self.load_lakeview_dashboard(demo_conf, install_path, d, warehouse_name) for d in demo_conf.dashboards]
                if debug:
                    print(f'dashboard installed: {installed_dash}')
                return installed_dash
            except Exception as e:
                self.installer.report.display_dashboard_error(e, demo_conf)
        elif "dashboards" in pkg_resources.resource_listdir("dbdemos", "bundles/"+demo_conf.name):
            raise Exception("Old dashboard are not supported anymore. This shouldn't happen - please fill a bug")
        return []

    def replace_dashboard_schema(self, demo_conf: DemoConf, definition: str):
        import re
        #main__build is used during the build process to avoid collision with default main.
        re.sub(r"`?main__build`?\.", "main", definition)
        definition = definition.replace("main__build.", f"main.")
        definition = definition.replace("`main__build`.", f"`main`.")
        if demo_conf.custom_schema_supported:
            return re.sub(r"`?" + re.escape(demo_conf.default_catalog) + r"`?\.`?" + re.escape(demo_conf.default_schema) + r"`?", f"`{demo_conf.catalog}`.`{demo_conf.schema}`", definition)
        return definition

    def load_lakeview_dashboard(self, demo_conf: DemoConf, install_path, dashboard, warehouse_name = None):
        endpoint = self.installer.get_or_create_endpoint(self.db.conf.name, demo_conf, warehouse_name = warehouse_name)
        try:
            definition = self.installer.get_resource(f"bundles/{demo_conf.name}/install_package/_resources/dashboards/{dashboard['id']}.lvdash.json")
            definition = self.replace_dashboard_schema(demo_conf, definition)
        except Exception as e:
            raise Exception(f"Can't load dashboard {dashboard} in demo {demo_conf.name}. Check bundle configuration under dashboards: [..]. "
                            f"The dashboard id should match the file name under the _resources/dashboard/<dashboard> folder.. {e}")
        dashboard_path = f"{install_path}/{demo_conf.name}/_dashboards"
        #Make sure the dashboard folder exists
        f = self.db.post("2.0/workspace/mkdirs", {"path": dashboard_path})
        if "error_code" in f:
            raise Exception(f"ERROR - wrong install path, can't save dashboard here: {f}")
        dashboard_creation = self.db.post(f"2.0/lakeview/dashboards", {
            "display_name": dashboard['name'],
            "warehouse_id": endpoint['warehouse_id'],
            "serialized_dashboard": definition,
            "parent_path": dashboard_path
        })
        dashboard['uid'] = dashboard_creation['dashboard_id']
        dashboard['is_lakeview'] = True
        return dashboard