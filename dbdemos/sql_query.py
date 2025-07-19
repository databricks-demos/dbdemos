import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, ExecuteStatementRequestOnWaitTimeout
from databricks.sdk.service.sql import ResultData, ResultManifest
from typing import List, Dict, Any
import time

from dbdemos.exceptions.dbdemos_exception import SQLQueryException

class SQLQueryExecutor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def get_or_create_shared_warehouse(self, ws: WorkspaceClient) -> str:
        warehouses = ws.warehouses.list()
        
        # First, look for a shared warehouse
        for warehouse in warehouses:
            if 'shared' in warehouse.name.lower():
                return warehouse.id

        # If no shared warehouse, look for a running warehouse
        for warehouse in warehouses:
            if warehouse.state == 'RUNNING':
                return warehouse.id
        
        # If no running warehouse, return the first available warehouse
        if warehouses:
            return warehouses[0].id
        
        # If no warehouses at all, create a new one
        new_warehouse = ws.warehouses.create(
            name="shared-warehouse",
            cluster_size="Small",
            auto_stop_mins=10
        )
        return new_warehouse.id

    def execute_query_as_list(self, ws: WorkspaceClient, query: str, timeout: int = 50, warehouse_id: str = None, debug: bool = False) -> tuple[ResultData, ResultManifest]:
        data, manifest = self.execute_query(ws, query, timeout, warehouse_id, debug)
        return self.get_results_formatted_as_list(data, manifest)

    def execute_query(self, ws: WorkspaceClient, query: str, timeout: int = 50, warehouse_id: str = None, debug: bool = False) -> tuple[ResultData, ResultManifest]:
        if not warehouse_id:
            warehouse_id = self.get_or_create_shared_warehouse(ws)
        if debug:
            print(f"Executing query: {query} with warehouse {warehouse_id}")
        # Execute the query with a maximum wait timeout of 50 seconds
        statement = ws.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout=f"{timeout}s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE
        )
        
        # If the statement is not completed within the wait_timeout, poll for results
        while statement.status.state in [StatementState.PENDING, StatementState.RUNNING]:
            time.sleep(1)
            statement = ws.statement_execution.get_statement(statement.statement_id)
        if statement.status.state == StatementState.FAILED:
            raise SQLQueryException(f"Query execution failed: {statement.status.error}")
        
        # Fetch initial results
        results = ws.statement_execution.get_statement(statement.statement_id)
        
        # Initialize combined result data
        combined_data = ResultData(data_array=[])
        if results.result and results.result.data_array:
            combined_data.data_array.extend(results.result.data_array)
            
        # Fetch additional chunks if they exist
        if results.manifest and results.manifest.chunks:
            chunk_index = results.manifest.chunks[0].chunk_index + 1
            while chunk_index < results.manifest.total_chunk_count:
                chunk = ws.statement_execution.get_statement_result_chunk_n(
                    statement_id=statement.statement_id,
                    chunk_index=chunk_index
                )
                if chunk.data_array:
                    combined_data.data_array.extend(chunk.data_array)
                chunk_index += 1
            
        return combined_data, results.manifest

    def get_results_formatted_as_list(self, result_data: ResultData, result_manifest: ResultManifest) -> List[Dict[str, Any]]:
        column_names = [col.name for col in result_manifest.schema.columns]
        
        result_list = []
        
        if result_data.data_array:  
            for row in result_data.data_array:
                result_dict = {column_names[i]: value for i, value in enumerate(row)}
                result_list.append(result_dict)
        
        return result_list