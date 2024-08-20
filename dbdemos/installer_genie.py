from .conf import DemoConf, GenieRoom
import pkg_resources

from .exceptions.dbdemos_exception import GenieCreationException
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

    def load_genie_data(self, demo_conf: DemoConf, warehouse_id, debug=True):
        for data_folder in demo_conf.data_folders:
            print(f"Loading data {data_folder}")
