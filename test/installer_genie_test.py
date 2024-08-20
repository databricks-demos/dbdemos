import dbdemos
from dbdemos.conf import DemoNotebook, DemoConf

from dbdemos.installer import Installer
from dbdemos.installer_genie import InstallerGenie



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

test_room_install()
#test_html()