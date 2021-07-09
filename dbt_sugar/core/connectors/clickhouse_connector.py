"""
Module clickhouse connector.

Module dependent of the base connector.
"""
from typing import Dict

import sqlalchemy

from dbt_sugar.core.connectors.base import BaseConnector
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger


class ClickhouseConnector(BaseConnector):
    """
    Connection class for clickhouse.

    Child class of base connector.
    """

    def __init__(
        self,
        connection_params: Dict[str, str],
    ) -> None:
        """
        Creates the URL and the Engine for future connections.

        Args:
            connection_params (Dict[str, str]): Dict containing database connection
                parameters and credentials.
        """
        logger.info(f"connectionparams={connection_params}")
        self.connection_url = sqlalchemy.engine.url.URL(
            drivername="clickhouse",
            host=connection_params.get("host", str()),
            username=connection_params.get("user", str()),
            password=connection_params.get("password", str()),
            database=connection_params.get("database", str()),
            port=connection_params.get("port", str()),
        )
        logger.info(f"connectionurl={self.connection_url}")
        conurl=f"clickhouse://{connection_params.get('user', str())}:{connection_params.get('password', str())}@{connection_params.get('host', str())}:{connection_params.get('port', str())}/{connection_params.get('database', str())}"
        self.connection_url=conurl
        logger.info(f"connection_url={conurl}")
        self.engine = sqlalchemy.create_engine(self.connection_url)



