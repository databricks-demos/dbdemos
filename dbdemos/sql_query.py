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
        # Once we hit the dotted-tag bug on the Statement Execution API, every
        # subsequent call will hit it too — flip this flag so we go straight to
        # the spark.sql fallback instead of paying the timeout each time.
        self._prefer_spark_sql = False

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
        # If we've already established that this workspace's Statement Execution
        # API rejects dotted custom-tag keys, skip the SDK call and use spark.sql
        # directly (when available). This avoids paying the timeout per call.
        if self._prefer_spark_sql:
            spark_result = self._try_spark_sql_fallback(query)
            if spark_result is not None:
                return spark_result
        if not warehouse_id:
            warehouse_id = self.get_or_create_shared_warehouse(ws)
        if debug:
            print(f"Executing query: {query} with warehouse {warehouse_id}")
        # Execute the query with a maximum wait timeout of 50 seconds
        try:
            statement = ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=query,
                wait_timeout=f"{timeout}s",
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE
            )
        except Exception as e:
            # The Statement Execution API rejects workspace custom_tags whose
            # keys contain `.` (e.g. `system.Certified`, auto-added on some
            # internal / managed workspaces). On affected workspaces every
            # SDK-driven query call will fail the same way. Fall back to
            # spark.sql when we have an active SparkSession.
            if self._looks_like_dotted_tag_failure(e):
                spark_result = self._try_spark_sql_fallback(query)
                if spark_result is not None:
                    self.logger.warning(
                        "Statement Execution API failed with a tag-related "
                        "error; using spark.sql for subsequent calls. "
                        "Original error: %s", e,
                    )
                    self._prefer_spark_sql = True
                    return spark_result
            raise
        
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

    @staticmethod
    def _looks_like_dotted_tag_failure(exc) -> bool:
        """Heuristic: does this exception look like the dotted custom-tag bug?

        The Statement Execution API rejects workspace ``custom_tags`` whose keys
        contain ``.`` (e.g. ``system.Certified`` on certain managed workspaces).
        The SDK surfaces this as a generic SDK error mentioning ``tag`` and
        either ``invalid`` / ``name`` / ``dotted``. Be permissive — false
        positives only mean we fall through to spark.sql, which usually works.
        """
        msg = str(exc).lower()
        if "tag" not in msg:
            return False
        return any(token in msg for token in ("invalid", "name", "dotted", "."))

    def _try_spark_sql_fallback(self, query: str):
        """Run the query via ``spark.sql`` when a SparkSession is available.

        Returns a ``(ResultData, None)`` tuple matching the SDK signature, or
        ``None`` when no SparkSession is available (in which case the caller
        must fall back to re-raising the original error).
        """
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            return None
        spark = SparkSession.getActiveSession()
        if spark is None:
            return None
        try:
            df = spark.sql(query)
            data_array = [[str(c) if c is not None else None for c in row] for row in df.collect()]
            return ResultData(data_array=data_array), None
        except Exception as e:
            self.logger.warning("spark.sql fallback also failed: %s", e)
            return None