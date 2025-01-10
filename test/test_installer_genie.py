import dbdemos
from dbdemos.conf import DemoNotebook, DemoConf, DataFolder

from dbdemos.installer import Installer
from dbdemos.installer_genie import InstallerGenie
from databricks.sdk import WorkspaceClient



def test_room_install():
    room = [{'display_name': 'test quentin API',
             'description': 'test Desc',
             'table_identifiers': ['main.quentin_test.*'],
             'sql_instructions': [{"title": "test", "content": "select * from test"}],
             'instructions': 'This is a description',
             'curated_questions': ["What's the Churn?", "Hoow many turbines do I have?"]}]
    demo_conf = {"genie_rooms": room, "name": "test", "category": "test", "title": "title", "description": "description", "bundle": True}
    conf = DemoConf(path="/Users/quentin.ambard@databricks.com/test_install_quentin", json_conf=demo_conf)
    with open("../local_conf_E2FE.json", "r") as r:
        import json
        c = json.loads(r.read())
    installer = Installer(c['username'], c['pat_token'], c['url'], cloud = "AWS")
    genie_installer = InstallerGenie(installer)
    genie_installer.install_genies(conf, "/Users/quentin.ambard@databricks.com/test_install_quentin", warehouse_id="475b94ddc7cd5211", debug=True)


def test_load_genie_data():
    data_folders = [
        {"source_folder":"fsi/fraud-transaction/customers", "source_format": "parquet", "target_table_name":"iot_parts", "target_format":"delta"},
        {"source_folder":"fsi/fraud-transaction/fraud_report", "source_format": "parquet", "target_table_name":"iot_turbines", "target_format":"delta"}
    ]
    demo_conf = {"data_folders": data_folders, "name": "test", "category": "test", "title": "title", "description": "description", "bundle": True}
    conf = DemoConf(path="/Users/quentin.ambard@databricks.com/test_install_quentin", json_conf=demo_conf)
    with open("../local_conf_E2FE.json", "r") as r:
        import json
        c = json.loads(r.read())
    installer = Installer(c['username'], c['pat_token'], c['url'], cloud = "AWS")
    genie_installer = InstallerGenie(installer)
    conf.catalog = 'dbdemos'
    conf.schema = 'test_quentin'
    genie_installer.load_genie_data(conf, warehouse_id="475b94ddc7cd5211", debug=True)

def test_schema_creation():
    demo_conf = {"name": "test", "category": "test", "title": "title", "description": "description", "bundle": True}
    conf = DemoConf(path="/Users/quentin.ambard@databricks.com/test_install_quentin", json_conf=demo_conf)
    with open("../local_conf_E2FE.json", "r") as r:
        import json
        c = json.loads(r.read())
    installer = Installer(c['username'], c['pat_token'], c['url'], cloud = "AWS")
    genie_installer = InstallerGenie(installer)
    conf.catalog = 'dbdemos'
    conf.schema = 'test_quentin2'
    ws = WorkspaceClient(token=installer.db.conf.pat_token, host=installer.db.conf.workspace_url)
    genie_installer.create_schema(ws,conf, debug=True)




def load_data_to_volume():
    demo_conf = {"name": "test", "category": "test", "title": "title", "description": "description", "bundle": True}
    conf = DemoConf(path="/Users/quentin.ambard@databricks.com/test_install_quentin", json_conf=demo_conf)
    with open("../local_conf_E2FE.json", "r") as r:
        import json
        c = json.loads(r.read())
    installer = Installer(c['username'], c['pat_token'], c['url'], cloud = "AWS")
    genie_installer = InstallerGenie(installer)
    conf.catalog = 'dbdemos'
    conf.schema = 'test_quentin2'
    ws = WorkspaceClient(token=installer.db.conf.pat_token, host=installer.db.conf.workspace_url)
    data_folder = DataFolder(source_folder="fsi/fraud-transaction/customers", source_format="parquet", target_table_name="iot_parts", target_format="delta")
    genie_installer.load_data_to_volume(ws, data_folder, warehouse_id="475b94ddc7cd5211", conf=conf, debug=True)


def test_load_data():
    demo_conf = {"name": "test", "category": "test", "title": "title", "description": "description", "bundle": True}
    conf = DemoConf(path="/Users/quentin.ambard@databricks.com/test_install_quentin", json_conf=demo_conf)
    with open("../local_conf_E2FE.json", "r") as r:
        import json
        c = json.loads(r.read())
    installer = Installer(c['username'], c['pat_token'], c['url'], cloud = "AWS")
    genie_installer = InstallerGenie(installer)
    conf.catalog = 'dbdemos'
    conf.schema = 'test_quentin2'
    ws = WorkspaceClient(token=installer.db.conf.pat_token, host=installer.db.conf.workspace_url)
    data_folder = DataFolder(source_folder="fsi/fraud-transaction/customers", source_format="parquet", target_table_name="dbdemos_aibi_customers", target_format="delta")
    genie_installer.load_data(ws, data_folder, warehouse_id="475b94ddc7cd5211", conf=conf, debug=True)



    
#test_load_data()
test_room_install()
#test_schema_creation()
#test_html()