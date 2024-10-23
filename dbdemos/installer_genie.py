from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from .conf import DemoConf

from .exceptions.dbdemos_exception import GenieCreationException, DataLoaderException
from .installer import Installer


class InstallerGenie:
    def __init__(self, installer: Installer):
        self.installer = installer
        self.db = installer.db

    def install_genies(self, demo_conf: DemoConf, install_path, warehouse_id, debug=True):
        path = self.db.get("2.0/workspace/get-status", {"path": install_path})
        if 'object_id' not in path:
            raise GenieCreationException(f"folder {install_path} doesn't exist", None, path)
        print(path)
        for room in demo_conf.genie_rooms:
            self.install_genie(room, install_path, warehouse_id, debug)
    def install_genie(self, room, warehouse_id, debug=True):
        room_payload = {
            "display_name": room.display_name,
            "description": room.description,
            "warehouse_id": warehouse_id,
            "table_identifiers": room.table_identifiers,
            "parent_folder": f"folders/{path['object_id']}",
            "run_as_type": "VIEWER"
        }
        created_room = self.db.post("2.0/data-rooms", json = room_payload)
        if 'id' not in created_room:
            raise GenieCreationException(f"Error creating room {room_payload}", room, created_room)

        if debug:
            print(f"Genie room created created_room: {created_room} - {room_payload}")
        actions = [{
            "action_type": "CREATE",
            "curated_question": {
                "data_room_id": created_room['id'],
                "question_text": q,
                "question_type": "SAMPLE_QUESTION"
            }
        } for q in room.curated_questions]
        questions = self.db.post(f"2.0/data-rooms/{created_room['id']}/curated-questions/batch-actions", {"actions": actions})
        if debug:
            print(f"Genie room question created:{questions}")
        if room.instructions:
            instructions = self.db.post(f"2.0/data-rooms/{created_room['id']}/instructions", {"title": "Notes", "content": room.instructions, "instruction_type": "TEXT_INSTRUCTION"})
            if debug:
                print(f"genie room instructions: {instructions}")
        for sql in room.sql_instructions:
            instructions = self.db.post(f"2.0/data-rooms/{created_room['id']}/instructions", {"title": sql['title'], "content": sql['content'], "instruction_type": "SQL_INSTRUCTION"})
            if debug:
                print(f"genie room SQL instructions: {instructions}")

    def create_schema(self, demo_conf: DemoConf, warehouse_id, debug=True):
        ws = WorkspaceClient(token=self.installer.db.conf.pat_token, host=self.installer.db.conf.workspace_url)
        #Test if catalog exists
        st = f"DESCRIBE CATALOG `{demo_conf.catalog}`"
        e = ws.statement_execution.execute_statement(statement=st, warehouse_id=warehouse_id, wait_timeout="50s")
        if e.status.state == StatementState.FAILED:
            if debug:
                print(f"Can't describe catalog {demo_conf.catalog}. {st} Try creating it {e.status.error.message}")
            st = f"CREATE CATALOG IF NOT EXISTS `{demo_conf.catalog}`"
            e = ws.statement_execution.execute_statement(statement=st, warehouse_id=warehouse_id, wait_timeout="50s")
            if e.status.state == StatementState.FAILED:
                raise DataLoaderException(f"Can't create catalog `{demo_conf.catalog}` and it doesn't seem to be existing. <br/>"
                                          f"Please create the catalog or grant you USAGE/READ permission, or install the demo in another catalog= dbdemos.install(xxx, catalog=xxx, schema=xxx).<br/>"
                                          f" {st} - {e.status.error.message}")

        #Schema
        st = f"DESCRIBE SCHEMA `{demo_conf.catalog}`.`{demo_conf.schema}`"
        e = ws.statement_execution.execute_statement(statement=st, warehouse_id=warehouse_id, wait_timeout="50s")
        if e.status.state == StatementState.FAILED:
            if debug:
                print(f"Can't describe schema {demo_conf.catalog}. {st} Try creating it {e.status.error.message}")
            st = f"CREATE SCHEMA IF NOT EXISTS `{demo_conf.catalog}`.`{demo_conf.schema}`"
            e = ws.statement_execution.execute_statement(statement=st, warehouse_id=warehouse_id, wait_timeout="50s")
            if e.status.state == StatementState.FAILED:
                raise DataLoaderException(f"Can't create schema `{demo_conf.catalog}`.`{demo_conf.schema}` and it doesn't seem to be existing. <br/>"
                                          f"Please create the catalog or grant you USAGE/READ permission, or install the demo in another catalog: dbdemos.install(xxx, catalog=xxx, schema=xxx, warehouse_id=xx).<br/>"
                                          f" {st} - {e.status.error.message}")


    def load_genie_data(self, demo_conf: DemoConf, warehouse_id, debug=True):
        if len(demo_conf.data_folders) > 0:
            self.create_schema(demo_conf, warehouse_id, debug)
            for data_folder in demo_conf.data_folders:
                ws = WorkspaceClient(token=self.installer.db.conf.pat_token, host=self.installer.db.conf.workspace_url)
                "https://notebooks.databricks.com/demos/dbdemos-dataset/"
                print(f"Loading data {data_folder}")
                self.download_file_from_git(data_folder.source_folder)
