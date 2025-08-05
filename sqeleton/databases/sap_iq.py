"""
Sqeleton driver for SAP IQ.

NOTE: This is a best-effort implementation and may require adjustments
for your specific SAP IQ version and configuration.

Key implementation choices:
- Hashing: Uses CHECKSUM() and BIT_XOR() for checksums.
- Connection: Uses the 'sqlanydb' library.
- Schema Reading: Uses the SYS.SYSCOLUMNS and related system catalog tables,
  as information_schema may not be available or complete.
- String Concat: Uses the '+' operator.
"""
import logging

from ..abcs import (
    ColType_BIGINT,
    ColType_BOOLEAN,
    ColType_CHAR,
    ColType_DATE,
    ColType_DECIMAL,
    ColType_FLOAT,
    ColType_INT,
    ColType_SMALLINT,
    ColType_TIMESTAMP,
    ColType_VARCHAR,
    Mixin_NormalizeValue,
    ReladiffDialect,
)
from .base import ThreadedDatabase, import_helper, Dialect

logger = logging.getLogger("sqeleton")


@import_helper("sapiq")
def import_sapiq():
    """Import the sqlanydb library, showing a nice error message if it's not installed"""
    try:
        import sqlanydb
    except ImportError:
        logger.error("Could not import sqlanydb. Please install it using: pip install sqlanydb")
        raise
    return sqlanydb


class Mixin_CHECKSUM(Dialect):
    """Hashing implementation for Sybase-like databases, using CHECKSUM()."""

    def md5_as_int(self, s: str) -> str:
        # CHECKSUM returns an INT. We cast to BIGINT for safety in BIT_XOR.
        return f"CAST(CHECKSUM({s}) AS BIGINT)"

    def aggregate_hash(self, columns: list[str]) -> str:
        # The aggregate function for bitwise XOR in SAP IQ is BIT_XOR.
        # We use CHECKSUM() on the concatenated string of column values.
        concat_expr = self.concat(columns, sep=" || '#' || ")
        return f"BIT_XOR(CAST(CHECKSUM({concat_expr}) AS BIGINT))"


class SapIQDialect(Mixin_CHECKSUM, Mixin_NormalizeValue, ReladiffDialect):
    """Dialect for SAP IQ."""
    # Sybase-like databases use + for string concatenation.
    CONCAT_OP = "+"


class SapIQ(ThreadedDatabase):
    dialect = SapIQDialect()
    CONNECT_URI_HELP = "sapiq://<user>:<password>@<host>:<port>/<db_name>?<options>"
    CONNECT_URI_PARAMS = ["db_name"]

    # Map SAP IQ's data type names to sqeleton's internal types
    TYPE_CLASSES = {
        "char": ColType_CHAR,
        "varchar": ColType_VARCHAR,
        "string": ColType_VARCHAR,
        "int": ColType_INT,
        "integer": ColType_INT,
        "smallint": ColType_SMALLINT,
        "bigint": ColType_BIGINT,
        "decimal": ColType_DECIMAL,
        "numeric": ColType_DECIMAL,
        "float": ColType_FLOAT,
        "double": ColType_FLOAT,
        "date": ColType_DATE,
        "timestamp": ColType_TIMESTAMP,
        "datetime": ColType_TIMESTAMP,
        "bit": ColType_BOOLEAN,
    }

    def create_connection(self):
        """Create a connection to SAP IQ using sqlanydb."""
        sqlanydb = import_sapiq()

        # Construct the DSN-style connection string for sqlanydb
        conn_str = f"UID={self.conn_info.user};PWD={self.conn_info.password};DBN={self.conn_info.database};ENG={self.conn_info.host}"
        if self.conn_info.port:
             conn_str += f";PORT={self.conn_info.port}"
        
        # Add any extra parameters from the URI
        if self.conn_info.extras:
            conn_str += ';' + ';'.join(f'{k}={v}' for k,v in self.conn_info.extras.items())

        return sqlanydb.connect(conn_str)

    def select_table_schema(self, path: list[str]) -> str:
        """Query SYS catalog tables to get table schema, as this is more reliable in Sybase-family DBs."""
        schema, table = self._normalize_table_path(path)

        return f"""
        SELECT
            c.column_name,
            d.domain_name,
            c.width AS datetime_precision,
            c.width AS numeric_precision,
            c.scale AS numeric_scale
        FROM
            SYS.SYSCOLUMNS c
        JOIN
            SYS.SYSDOMAIN d ON c.domain_id = d.domain_id
        JOIN
            SYS.SYSTAB t ON c.table_id = t.table_id
        JOIN
            SYS.SYSUSER u ON t.creator = u.user_id
        WHERE
            u.user_name = '{schema}' AND t.table_name = '{table}'
        ORDER BY
            c.column_id
        """

    def _normalize_table_path(self, path: list[str]) -> tuple[str, str]:
        if len(path) == 1:
            # If no schema is provided, we might need a default user/schema.
            # 'dbo' is a common default in Sybase/SQL Server.
            return self.conn_info.user or "dbo", path[0]
        if len(path) == 2:
            return path[0], path[1]

        raise ValueError(
            f"Table path in SAP IQ can be either <table_name> or <schema>.<table_name>. Got: {'.'.join(path)}"
        )
    
    @property
    def is_autocommit(self) -> bool:
        return True
