"""
Sqeleton driver for Apache Impala.

NOTE: This is a best-effort implementation and may require adjustments
for your specific Impala version and configuration.

Key implementation choices:
- Hashing: Uses FNV_HASH() and BITXOR() for checksums, as MD5/SHA1 are often not built-in.
- Connection: Uses the 'impyla' library.
- Schema Reading: Uses information_schema.columns, assuming a modern Impala version.
"""
from ..abcs import (
    ColType_BIGINT,
    ColType_BOOLEAN,
    ColType_CHAR,
    ColType_DATE,
    ColType_DECIMAL,
    ColType_DOUBLE,
    ColType_FLOAT,
    ColType_INT,
    ColType_REAL,
    ColType_SMALLINT,
    ColType_STRING,
    ColType_TIMESTAMP,
    ColType_TINYINT,
    ColType_VARCHAR,
    Mixin_NormalizeValue,
    ReladiffDialect,
)
from ..abcs.database_types import Datetime, Fractional
from .base import ThreadedDatabase, import_helper, Dialect


@import_helper("impala")
def import_impala():
    """Import the impyla library, showing a nice error message if it's not installed"""
    from impala.dbapi import connect

    return connect


class Mixin_FNV_HASH(Dialect):
    """Hashing implementation for Impala, using the built-in FNV_HASH function."""

    def md5_as_int(self, s: str) -> str:
        # FNV_HASH() returns a BIGINT, so no conversion from hex is needed.
        return f"fnv_hash({s})"

    def aggregate_hash(self, columns: list[str]) -> str:
        # Impala's aggregate bitwise XOR is BITXOR.
        # We first concatenate the columns into a single string.
        concat_expr = self.concat(columns)
        return f"bitxor(fnv_hash({concat_expr}))"


class ImpalaDialect(Mixin_FNV_HASH, Mixin_NormalizeValue, ReladiffDialect):
    """Dialect for Impala."""

    def normalize_timestamp(self, value: str, coltype: Datetime) -> str:
        # Casting to STRING is the most reliable way to get a canonical representation
        # across different DBs. More specific formatting can be added if needed.
        return f"CAST({value} AS STRING)"


class Impala(ThreadedDatabase):
    dialect = ImpalaDialect()
    CONNECT_URI_HELP = "impala://<host>:<port>/<database>"
    CONNECT_URI_PARAMS = ["database"]

    # Map Impala's data type names to sqeleton's internal types
    TYPE_CLASSES = {
        "STRING": ColType_STRING,
        "VARCHAR": ColType_VARCHAR,
        "CHAR": ColType_CHAR,
        "BOOLEAN": ColType_BOOLEAN,
        "TINYINT": ColType_TINYINT,
        "SMALLINT": ColType_SMALLINT,
        "INT": ColType_INT,
        "BIGINT": ColType_BIGINT,
        "FLOAT": ColType_FLOAT,
        "DOUBLE": ColType_DOUBLE,
        "REAL": ColType_REAL,
        "DECIMAL": ColType_DECIMAL,
        "TIMESTAMP": ColType_TIMESTAMP,
        "DATE": ColType_DATE,
    }

    def create_connection(self):
        """Create a connection to Impala using impyla."""
        connect = import_impala()

        # For advanced auth like Kerberos, you might need to pass more kwargs.
        # e.g. auth_mechanism='GSSAPI', kerberos_service_name='impala'
        # These can be passed via the DSN URI.
        return connect(
            host=self.conn_info.host,
            port=self.conn_info.port or 21050, # Default Impala port
            database=self.conn_info.database,
            user=self.conn_info.user,
            password=self.conn_info.password,
            **self.conn_info.extras,
        )

    def select_table_schema(self, path: list[str]) -> str:
        """Query information_schema to get table schema"""
        schema, table = self._normalize_table_path(path)

        return (
            "SELECT column_name, data_type, NULL as datetime_precision, numeric_precision, numeric_scale "
            f"FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}'"
        )

    def _normalize_table_path(self, path: list[str]) -> tuple[str, str]:
        if len(path) == 1:
            return "default", path[0]
        if len(path) == 2:
            return path[0], path[1]

        raise ValueError(
            f"Table path in Impala can be either <table_name> or <db_name>.<table_name>. Got: {'.'.join(path)}"
        )

    @property
    def is_autocommit(self) -> bool:
        # Impala does not support multi-statement transactions
        return True
