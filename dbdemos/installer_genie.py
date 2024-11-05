import json
from concurrent.futures import ThreadPoolExecutor
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
    def __init__(self, installer: 'Installer'):
        self.installer = installer
        self.db = installer.db
        self.sql_query_executor = SQLQueryExecutor()

    def install_genies(self, demo_conf: DemoConf, install_path, warehouse_name, debug=True):
        rooms = []
        if debug:
            print(f"Installing genie room {demo_conf.genie_rooms}")
            print(f"Loading data folders {demo_conf.data_folders}")
        if len(demo_conf.genie_rooms) > 0 or len(demo_conf.data_folders) > 0:
            try:
                warehouse = self.installer.get_or_create_endpoint(self.db.conf.name, demo_conf, warehouse_name = warehouse_name, throw_error=True)
                warehouse_id = warehouse['endpoint_id']
                self.load_genie_data(demo_conf, warehouse_id, debug)
                if len(demo_conf.genie_rooms) > 0:
                    genie_path = f"{install_path}/{demo_conf.name}/_genie_spaces"
                    #Make sure the genie folder exists
                    self.db.post("2.0/workspace/mkdirs", {"path": genie_path})
                    path = self.db.get("2.0/workspace/get-status", {"path": genie_path})
                    if "error_code" in path:
                        raise Exception(f"ERROR - wrong install path, can't save genie spaces here: {path}")
                    for room in demo_conf.genie_rooms:
                        rooms.append(self.install_genie(demo_conf, room, path, warehouse_id, debug))
            except Exception as e:
                self.installer.report.display_genie_room_creation_error(e, demo_conf)
        return rooms

    def install_genie(self, demo_conf: DemoConf, room: GenieRoom, genie_path, warehouse_id, debug=True):
        room_payload = {
            "display_name": room.display_name,
            "description": room.description,
            "warehouse_id": warehouse_id,
            "table_identifiers": room.table_identifiers,
            "parent_folder": f'folders/{genie_path["object_id"]}',
            "run_as_type": "VIEWER"
        }
        print(room_payload)
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
        for sql in room.sql_instructions:
            instructions = self.db.post(f"2.0/data-rooms/{created_room['id']}/instructions", {"title": sql['title'], "content": sql['content'], "instruction_type": "SQL_INSTRUCTION"})
            if debug:
                print(f"genie room SQL instructions: {instructions}")
        self.load_genie_data(demo_conf, warehouse_id, debug)
        return {"id": room.id, "uid": created_room['id'], 'name': room.display_name}


    def create_schema(self, ws, demo_conf: DemoConf, debug=True):
        ws = WorkspaceClient(token=self.installer.db.conf.pat_token, host=self.installer.db.conf.workspace_url)
        try:
            catalog = ws.catalogs.get(demo_conf.catalog)
        except Exception as e:
            if debug:
                print(f"Can't describe catalog {demo_conf.catalog}. Will now try to create it. Error: {e}")
            try:
                catalog = ws.catalogs.create(demo_conf.catalog)
            except Exception as e:
                raise DataLoaderException(f"Can't create catalog `{demo_conf.catalog}` and it doesn't seem to be existing. <br/>"
                                        f"Please create the catalog or grant you USAGE/READ permission, or install the demo in another catalog= dbdemos.install(xxx, catalog=xxx, schema=xxx).<br/>"
                                        f" {e} ")

        schema_full_name = f"{demo_conf.catalog}.{demo_conf.schema}"
        try:
            schema = ws.schemas.get(schema_full_name)
        except Exception as e:
            if debug:
                print(f"Can't describe schema {schema_full_name}. {e} Will now try to create it. Error:{e}")
            try:
                schema = ws.schemas.create(demo_conf.schema, catalog_name=demo_conf.catalog)
            except Exception as e:
                raise DataLoaderException(f"Can't create schema {schema_full_name} and it doesn't seem to be existing. <br/>"
                                        f"Please create the catalog or grant you USAGE/READ permission, or install the demo in another catalog: dbdemos.install(xxx, catalog=xxx, schema=xxx, warehouse_id=xx).<br/>"
                                        f" {e}")

    def load_genie_data(self, demo_conf: DemoConf, warehouse_id, debug=True):
        if demo_conf.data_folders:
            ws = WorkspaceClient(token=self.installer.db.conf.pat_token, host=self.installer.db.conf.workspace_url)
            self.create_schema(ws, demo_conf, debug)
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(self.load_data, ws, data_folder, warehouse_id, demo_conf, debug) 
                          for data_folder in demo_conf.data_folders]
                for future in futures:
                    future.result()

    def get_current_cluster_id(self):
        return json.loads(self.installer.get_dbutils_tags_safe()['clusterId'])

    def load_data(self, ws: WorkspaceClient, data_folder: DataFolder, warehouse_id, conf: DemoConf, debug=True):
        sql_query = f"""CREATE TABLE IF NOT EXISTS {conf.catalog}.{conf.schema}.{data_folder.target_table_name} as 
                       SELECT * FROM read_files('s3://dbdemos-dataset/{data_folder.source_folder}',  
                       format => '{data_folder.source_format}', 
                       pathGlobFilter => '*.{data_folder.source_format}')"""
        if debug:
            print(f"Loading data {data_folder}: {sql_query}")
        self.sql_query_executor.execute_query(ws, sql_query, warehouse_id=warehouse_id, debug=debug)
    



    # --------------------------------------------------------------------------------------------------------------------------------------------  
    # Experimental, first upload data to the volume as some warehouse don't have access to the S3 bucket directly.
    # --------------------------------------------------------------------------------------------------------------------------------------------  
    def load_data_to_volume(self, ws: WorkspaceClient, data_folder: DataFolder, warehouse_id, conf: DemoConf, debug=True):
        import boto3
        from botocore.client import Config
        from botocore import UNSIGNED
        print(f"Loading data {data_folder}")
        s3 = boto3.client('s3', region_name='us-west-2', config=Config(signature_version=UNSIGNED))
        bucket = "dbdemos-dataset"
        
        try:
            files = []
            for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=data_folder.source_folder):
                if "Contents" in page:
                    files.extend([obj["Key"] for obj in page["Contents"]])
            
            if debug:
                print(f"Found {len(files)} files in s3://{bucket}/{data_folder.source_folder}")
                
            volume_name = f"{conf.catalog}/{conf.schema}/dbdemos_raw_data"
            try:
                volume = ws.volumes.read(f"{conf.catalog}.{conf.schema}.dbdemos_raw_data")
            except Exception as e:
                if debug:
                    print(f"Volume {volume_name} doesn't seem to exist, creating it - {e}")
                try:
                    volume = ws.volumes.create(
                        catalog_name=conf.catalog,
                        schema_name=conf.schema,
                        name="dbdemos_raw_data",
                        volume_type=VolumeType.MANAGED
                    )
                except Exception as e:  
                    raise DataLoaderException(f"Can't create volume {volume_name} and it doesn't seem to be existing. <br/>"
                                            f"Please create the volume or grant you USAGE/READ permission, or install the demo in another catalog: dbdemos.install(xxx, catalog=xxx, schema=xxx, warehouse_id=xx).<br/>"
                                            f" {e}")
            
            def copy_file(s3_path):
                if not s3_path.endswith('/'):
                    file_name = s3_path.split('/')[-1]
                    target_path = f"/Volumes/{volume_name}/{data_folder.source_folder}/{file_name}"
                    
                    if debug:
                        print(f"Copying {s3_path} to {target_path}")
                        
                    response = s3.get_object(Bucket=bucket, Key=s3_path)
                    ws.files.upload(target_path, response['Body'].read(), overwrite=False)
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                list(executor.map(copy_file, files))

            self.sql_query_executor.execute_query(ws, f"""CREATE TABLE IF NOT EXISTS {conf.catalog}.{conf.schema}.{data_folder.target_table_name} as 
                                                SELECT * FROM read_files('/Volumes/{volume_name}/{data_folder.source_folder}',  
                                                format => '{data_folder.source_format}', 
                                                pathGlobFilter => '*.{data_folder.source_format}')""", warehouse_id=warehouse_id, debug=debug)

        except Exception as e:
            raise DataLoaderException(f"Error loading data from S3: {str(e)}")
