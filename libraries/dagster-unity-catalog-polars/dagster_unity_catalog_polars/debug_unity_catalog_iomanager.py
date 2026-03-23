from __future__ import annotations

from collections.abc import Callable
from functools import wraps

from .unity_catalog_iomanager import DatabricksUnityCatalogInputManager


class DebugDatabricksUnityCatalogInputManager(DatabricksUnityCatalogInputManager):
    """Overwrites the UnityCatalogInputManager such that it adds LIMIT 10 to the original query"""

    def __init__(
        self,
        token_generator: Callable[[], str],
        server_hostname: str,
        endpoint: str,
        limit: int = 10_000,
    ):
        """Initializes the UnityCatalogInputManager.

        Args:
            token_generator (Callable): Non-cached token generator callable object, to be called when loading input
            server_hostname (str): The hostname of the databricks environment
            endpoint (str): The Databricks SQL warehouse endpoint to connect
            limit (int): Maximum number of rows to grab
        """
        super().__init__(token_generator, server_hostname, endpoint)
        self.limit = limit

    @wraps(DatabricksUnityCatalogInputManager.form_query)
    def form_query(  # type: ignore
        self,
        catalog: str,
        schema: str,
        table: str,
        columns: list[str],
        predicate: str,
        partition_predicate: str | None,
    ) -> str:
        """Adds a LIMIT to the original query"""
        return (
            super().form_query(catalog, schema, table, columns, predicate, partition_predicate)
            + f" LIMIT {self.limit}"
        )  # type: ignore
