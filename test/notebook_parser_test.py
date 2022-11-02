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


test_close_cell()