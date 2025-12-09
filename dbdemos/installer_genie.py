import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.sql import StatementState

from dbdemos.sql_query import SQLQueryExecutor
from .conf import DataFolder, DemoConf, GenieRoom
from .exceptions.dbdemos_exception import GenieCreationException, DataLoaderException, SQLQueryException

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .installer import Installer


class InstallerGenie:
    VOLUME_NAME = "dbdemos_raw_data"

    def __init__(self, installer: 'Installer'):
        self.installer = installer
        self.db = installer.db
        self.sql_query_executor = SQLQueryExecutor()

    def install_genies(self, demo_conf: DemoConf, install_path: str, warehouse_name: str, skip_genie_rooms: bool, debug=True):
        rooms = []
        if len(demo_conf.genie_rooms) > 0 or len(demo_conf.data_folders) > 0:
            warehouse = self.installer.get_or_create_endpoint(self.db.conf.name, demo_conf, warehouse_name = warehouse_name, throw_error=True)
            try:
                warehouse_id = warehouse['endpoint_id']
                self.load_genie_data(demo_conf, warehouse_id, debug)
                if not skip_genie_rooms and len(demo_conf.genie_rooms) > 0:
                    if debug:
                        print(f"Installing genie room {demo_conf.genie_rooms}")
                    genie_path = f"{install_path}/{demo_conf.name}/_genie_spaces"
                    #Make sure the genie folder exists
                    self.db.post("2.0/workspace/mkdirs", {"path": genie_path})
                    path = self.db.get("2.0/workspace/get-status", {"path": genie_path})
                    if "error_code" in path:
                        raise Exception(f"ERROR - wrong install path, can't save genie spaces here: {path}")
                    for room in demo_conf.genie_rooms:
                        rooms.append(self.install_genie(room, path, warehouse_id, debug))
            except Exception as e:
                self.installer.report.display_genie_room_creation_error(e, demo_conf)
        return rooms

    def install_genie(self, room: GenieRoom, genie_path, warehouse_id, debug=True):
        #Genie rooms don't allow / anymore
        ws = WorkspaceClient(token=self.installer.db.conf.pat_token, host=self.installer.db.conf.workspace_url)
        self.create_temp_table_for_genie_creation(ws, room, warehouse_id, debug)
        room.display_name = room.display_name.replace("/", "-")
        room_payload = {
            "display_name": room.display_name,
            "description": room.description,
            "warehouse_id": warehouse_id,
            "table_identifiers": room.table_identifiers,
            "parent_folder": f'folders/{genie_path["object_id"]}',
            "run_as_type": "VIEWER"
        }
        created_room = self.db.post("2.0/data-rooms", json=room_payload)
        if 'id' not in created_room:
            raise GenieCreationException(f"Error creating room {room_payload} - {created_room}", room, created_room)

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
        
        for function_name in room.function_names:
            instructions = self.db.post(f"2.0/data-rooms/{created_room['id']}/instructions", {"title": "SQL Function", "content": function_name, "instruction_type": "CERTIFIED_ANSWER"})
            if debug:
                print(f"genie room function: {instructions}")
        for sql in room.sql_instructions:
            instructions = self.db.post(f"2.0/data-rooms/{created_room['id']}/instructions", {"title": sql['title'], "content": sql['content'], "instruction_type": "SQL_INSTRUCTION"})
            if debug:
                print(f"genie room SQL instructions: {instructions}")
        for b in room.benchmarks:
            benchmark =    {
                            "question_text": b["question_text"],
                            "question_type":"BENCHMARK",
                            "answer_text": b["answer_text"],
                            "is_deprecated": False,
                            "updatable_fields_mask":[]
                            }
            instructions = self.db.post(f"2.0/data-rooms/{created_room['id']}/curated-questions", {"curated_question": benchmark, "data_room_id":created_room['id']})
            if debug:
                print(f"genie room benchmarks: {instructions}")
        self.delete_temp_table_for_genie_creation(ws, room, debug)
        return {"id": room.id, "uid": created_room['id'], 'name': room.display_name}

    # we need to have the table existing before creating the genie room, however they're created in SDP which is in a job and not yet available.
    # This is a workaround to create a temp table with a property that will be used to delete it once the genie room is created so that the SDP table can run without issue.
    def create_temp_table_for_genie_creation(self, ws: WorkspaceClient, room: GenieRoom, warehouse_id, debug=False):
        for table in room.table_identifiers:
            if not ws.tables.exists(table).table_exists:
                sql_query = f"CREATE TABLE IF NOT EXISTS {table} TBLPROPERTIES ('dbdemos.mock_table_for_genie' = 1);"
                if debug:
                    print(f"Creating temp genie table {table}: {sql_query}")
                self.sql_query_executor.execute_query(ws, sql_query, warehouse_id=warehouse_id, debug=debug)

    def delete_temp_table_for_genie_creation(self, ws, room: GenieRoom, debug=False):
        for table in room.table_identifiers:
            if ws.tables.exists(table).table_exists and 'dbdemos.mock_table_for_genie' in ws.tables.get(table).properties:
                if debug:
                    print(f'Deleting temp genie table {table}')
                ws.tables.delete(table)

    def load_genie_data(self, demo_conf: DemoConf, warehouse_id, debug=True):
        if demo_conf.data_folders:
            print(f"Loading data in your schema {demo_conf.catalog}.{demo_conf.schema} using warehouse {warehouse_id}, this might take a few seconds (you can use another warehouse with the option: warehouse_name='xxx')...")
            ws = WorkspaceClient(token=self.installer.db.conf.pat_token, host=self.installer.db.conf.workspace_url)
            if any(d.target_volume_folder_name is not None for d in demo_conf.data_folders):
                self.create_raw_data_volume(ws, demo_conf, debug)

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(self.load_data, ws, data_folder, warehouse_id, demo_conf, debug) 
                        for data_folder in demo_conf.data_folders]
                for future in futures:
                    future.result()
        if demo_conf.sql_queries:
            self.run_sql_queries(ws, demo_conf, warehouse_id, debug)

    def run_sql_queries(self, ws: WorkspaceClient, demo_conf: DemoConf, warehouse_id, debug=True):
        for batch in demo_conf.sql_queries:
            with ThreadPoolExecutor(max_workers=5) as ex:
                futures = [ex.submit(self.sql_query_executor.execute_query, ws, q, warehouse_id=warehouse_id, debug=debug) for q in batch]
                for f in as_completed(futures):
                    try:
                        f.result()
                    except SQLQueryException as e:
                        if "tag name" in str(e).lower():
                            print(f"Warn - SQL error on tag ignored - probably free edition: {e}")
                        else: raise

    def get_current_cluster_id(self):
        return json.loads(self.installer.get_dbutils_tags_safe()['clusterId'])

    def load_data(self, ws: WorkspaceClient, data_folder: DataFolder, warehouse_id, conf: DemoConf, debug=True):
        # Load table to a table
        if data_folder.target_table_name:
            try:
                sql_query = f"""CREATE TABLE IF NOT EXISTS {conf.catalog}.{conf.schema}.{data_folder.target_table_name} as 
                            SELECT * FROM read_files('s3://dbdemos-dataset/{data_folder.source_folder}',  
                            format => '{data_folder.source_format}', 
                            pathGlobFilter => '*.{data_folder.source_format}')"""
                if debug:
                    print(f"Loading data {data_folder}: {sql_query}")
                self.sql_query_executor.execute_query(ws, sql_query, warehouse_id=warehouse_id, debug=debug)
            except Exception as e:
                if "com.amazonaws.auth.BasicSessionCredentials" in str(e):
                    print("INFO: Basic Credential error detected downloading the files from our demo bucket. Will try to load data to volume first, please wait as this is a slower workflow...")
                    self.create_raw_data_volume(ws, conf, debug)
                    self.load_data_to_volume(ws, data_folder, conf, debug)
                    self.create_table_from_volume(ws, data_folder, warehouse_id, conf, debug)
                else:
                    raise DataLoaderException(f"Error loading data from S3: {str(e)}")
        else:
            self.load_data_to_volume(ws, data_folder, conf, debug)
    
    # Class-level lock for volume creation
    import threading
    _volume_creation_lock = threading.Lock()

    def create_raw_data_volume(self, ws: WorkspaceClient, demo_conf: DemoConf, debug=True):
        with InstallerGenie._volume_creation_lock:
            full_volume_name = f"{demo_conf.catalog}/{demo_conf.schema}/{InstallerGenie.VOLUME_NAME}"
            try:
                ws.volumes.read(f"{demo_conf.catalog}.{demo_conf.schema}.{InstallerGenie.VOLUME_NAME}")
            except Exception as e:
                if debug:
                    print(f"Volume {full_volume_name} doesn't seem to exist, creating it - {e}")
                try:
                    ws.volumes.create(
                        catalog_name=demo_conf.catalog,
                        schema_name=demo_conf.schema,
                        name=InstallerGenie.VOLUME_NAME,
                        volume_type=VolumeType.MANAGED
                    )
                except Exception as e:  
                    raise DataLoaderException(f"Can't create volume {full_volume_name} to load data demo, and it doesn't seem to be existing. <br/>"
                                            f"Please create the volume or grant you USAGE/READ permission, or install the demo in another catalog: dbdemos.install(xxx, catalog=xxx, schema=xxx, warehouse_id=xx).<br/>"
                                            f" {e}")


    # --------------------------------------------------------------------------------------------------------------------------------------------  
    # Experimental, first upload data to the volume as some warehouse don't have access to the S3 bucket directly when instance profiles exist.
    # --------------------------------------------------------------------------------------------------------------------------------------------  
    def load_data_through_volume(self, ws: WorkspaceClient, data_folders: list[DataFolder], warehouse_id: str, demo_conf: DemoConf, debug=True):
        print('INFO: Basic Credential error detected downloading the files from our demo S3 bucket. Will try to load data to volume first, please wait as this might take a while...')
        self.create_raw_data_volume(ws, demo_conf, debug)

        def load_data_and_create_table(ws: WorkspaceClient, data_folder: DataFolder, warehouse_id: str, demo_conf: DemoConf, debug=True):
            self.load_data_to_volume(ws, demo_conf, data_folder, debug)
            self.create_table_from_volume(ws, data_folder, warehouse_id, demo_conf, debug)

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(load_data_and_create_table, ws, data_folder, warehouse_id, demo_conf, debug)
                    for data_folder in data_folders]
            for future in futures:
                future.result()


    def load_data_to_volume(self, ws: WorkspaceClient, data_folder: DataFolder, demo_conf: DemoConf, debug=True):
        assert data_folder.source_format in ["csv", "json", "parquet"], "data loader through volume only support csv, json and parquet"

        import requests
        import collections
        dbutils = self.installer.get_dbutils()
        try:
            folder = data_folder.target_volume_folder_name if data_folder.target_volume_folder_name else data_folder.source_folder
            #first try with a dbutils copy if available
            copied_successfully = False
            if debug:
                print(f"Copying {data_folder.source_folder} to {f'/Volumes/{demo_conf.catalog}/{demo_conf.schema}/{InstallerGenie.VOLUME_NAME}/{folder}'} using dbutils fs.cp")
            if dbutils is not None:
                try:
                    dbutils.fs.cp(f"s3://dbdemos-dataset/{data_folder.source_folder}", f"/Volumes/{demo_conf.catalog}/{demo_conf.schema}/{InstallerGenie.VOLUME_NAME}/{folder}", recurse=True)
                    copied_successfully = True
                except Exception as e:
                    copied_successfully = False
                    if debug:
                        print(f"Error copying {data_folder.source_folder} to {f'/Volumes/{demo_conf.catalog}/{demo_conf.schema}/{InstallerGenie.VOLUME_NAME}/{folder}'} using dbutils fs.cp: {e}")
                if copied_successfully and debug:
                    print(f"Copied {data_folder.source_folder} to {f'/Volumes/{demo_conf.catalog}/{demo_conf.schema}/{InstallerGenie.VOLUME_NAME}/{folder}'} using dbutils fs.cp")
            if not copied_successfully:
                # Get list of files from GitHub API, to avoid adding a S3 boto dependency just for this
                github_path = f"https://api.github.com/repos/databricks-demos/dbdemos-dataset/contents/{data_folder.source_folder}"
                if debug:
                    print(f"Getting files from {github_path}")
                files = requests.get(github_path).json()
                if 'message' in files:
                    print(f"Error getting files from {github_path}: {files}")
                files = [f['download_url'] for f in files]
                
                if debug:
                    print(f"Found {len(files)} files in GitHub repo for {data_folder.source_folder}")
                                
                def copy_file(file_url):
                    if not file_url.endswith('/'):
                        file_name = file_url.split('/')[-1]
                        target_path = f"/Volumes/{demo_conf.catalog}/{demo_conf.schema}/{InstallerGenie.VOLUME_NAME}/{folder}/{file_name}"
                        
                        s3_url = file_url.replace("https://raw.githubusercontent.com/databricks-demos/dbdemos-dataset/main/", 
                                                "https://dbdemos-dataset.s3.amazonaws.com/")

                        if debug:
                            print(f"Copying {s3_url} to {target_path}")
                        response = requests.get(s3_url)
                        response.raise_for_status()
                        if debug:
                            print(f"File {file_name} in memory. sending to volume...")
                        import io
                        buffer = io.BytesIO(response.content)
                        ws.files.upload(target_path, buffer, overwrite=True)
                        if debug:
                            print(f"File {file_name} in volume!")
                
                with ThreadPoolExecutor(max_workers=5) as executor:
                    collections.deque(executor.map(copy_file, files))

        except Exception as e:
            raise DataLoaderException(f"Error loading data from S3: {str(e)}")

    def create_table_from_volume(self, ws: WorkspaceClient, data_folder: DataFolder, warehouse_id, conf: DemoConf, debug=True):
        self.sql_query_executor.execute_query(ws, f"""CREATE TABLE IF NOT EXISTS {conf.catalog}.{conf.schema}.{data_folder.target_table_name} as 
                                            SELECT * FROM read_files('/Volumes/{conf.catalog}/{conf.schema}/{InstallerGenie.VOLUME_NAME}/{data_folder.source_folder}',  
                                            format => '{data_folder.source_format}', 
                                            pathGlobFilter => '*.{data_folder.source_format}')""", warehouse_id=warehouse_id, debug=debug)
