import re
import base64
import urllib.parse
import json
from dbdemos.notebook_parser import NotebookParser



def test_close_cell():
    with open("../dbdemos/template/LICENSE.html", "r") as f:
        p = NotebookParser(f.read())
        p.hide_commands_and_results()
        #print(p.get_html())
        #p.hide_command_result(0)


def test_automl():
    with open("../dbdemos/bundles/mlops-end2end/install_package/01_feature_engineering.html", "r") as f:
        p = NotebookParser(f.read())
        assert "Data exploration notebook" in p.content
        assert "Please run the notebook cells to get your AutoML links" not in p.content
        p.remove_automl_result_links()
        assert "Data exploration notebook" not in p.content
        assert "Please run the notebook cells to get your AutoML links" in p.content
        #print(p.get_html())
        #p.hide_command_result(0)

def test_parser_contains():
    with open("../dbdemos/bundles/mlops-end2end/install_package/_resources/00-setup.html", "r") as f:
        p = NotebookParser(f.read())
        assert p.contains("00-global-setup")
        p.replace_in_notebook('(?:\.\.\/)*_resources\/00-global-setup', './00-global-setup-test', True)
        assert p.contains("./00-global-setup-test")
        #print(p.get_html())
        #p.hide_command_result(0)


test_close_cell()
test_automl()
test_parser_contains()