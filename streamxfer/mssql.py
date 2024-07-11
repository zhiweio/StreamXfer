import re
import textwrap
from dataclasses import dataclass
from urllib.parse import urlparse

import orjson
import pymssql
from pymssql import Connection
from pymssql.exceptions import ProgrammingError  # noqa

from streamxfer.format import KB
from streamxfer.format import sc, sa
from streamxfer.typing import *
from streamxfer.utils import unmask_dot


class Keywords:
    string_types = [
        "CHAR",
        "NCHAR",
        "NTEXT",
        "NVARCHAR",
        "SQL_VARIANT",
        "TEXT",
        "VARCHAR",
        "XML",
    ]
    NTEXT = "NTEXT"
    TEXT = "TEXT"
    FLOAT = "FLOAT"
    NUMERIC = "NUMERIC"
    DECIMAL = "DECIMAL"


@dataclass
class SqlCreds:
    """
    Credential object for all SQL operations.

    Attributes:
        server (str): The server address of the SQL database.
        database (str): The database name.
        username (str): The username for SQL authentication.
        password (str): The password for SQL authentication.
        port (int): The port number for the SQL server connection.
    """

    server: str
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    port: str = "1433"

    def connect(self) -> Connection:
        """
        Establishes a connection to the SQL database.

        Args:
            autocommit (bool, optional): Whether to enable autocommit mode. Defaults to False.

        Returns:
            pymssql.Connection: A connection object to the SQL database.
        """
        return pymssql.connect(
            server=self.server,
            user=self.username,
            password=self.password,
            database=self.database,
            port=self.port,
        )

    @classmethod
    def from_url(cls, url, identifier=None):
        """
        Creates a SqlCreds instance from a database URL.

        Args:
            url (str): The database URL in the format 'mssql://user:password@host/database'.
        """
        if not re.match(r"mssql://(.*?):(.*?)@(.*?)/(.*)", url):
            raise Exception(
                "Invalid db_url, must be 'mssql://user:password@host/database'"
            )

        dsn = urlparse(url)
        hostname = str(dsn.hostname)
        if identifier:
            hostname = hostname.split(".")
            hostname = identifier + "." + ".".join(hostname[1:])
        return cls(
            server=hostname,
            database=dsn.path.lstrip("/"),
            port=dsn.port or "1433",
            username=dsn.username,
            password=dsn.password,
        )

    def to_url(self):
        return f"mssql://{self.username}:{self.password}@{self.server}/{self.database}"


def table_spaceused(table, conn: Connection) -> Union[Dict[str, Any], None]:
    """
    {
        "name" : "[RISKMGT].[FACT_AccountingCheck]",
        "rows" : "0                   ",
        "reserved" : "0 KB",
        "data" : "0 KB",
        "index_size" : "0 KB",
        "unused" : "0 KB"
    }
    """
    sql = f"EXEC sp_spaceused '{table}'"
    with conn.cursor(as_dict=True) as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return row


def table_data_size(table, conn: Connection) -> int:
    res = table_spaceused(table, conn)
    if not res:
        return 0
    data = res["data"].replace("KB", "").strip()
    return int(data) * KB  # Bytes


def table_columns(table, conn: Connection) -> List[Dict]:
    sql = textwrap.dedent(
        f"""
with table_info as (select a.name as table_name, b.name as schema_name
                    from sys.tables a
                             join sys.schemas b
                                  on a.schema_id = b.schema_id
                    where a.object_id = OBJECT_ID('{table}'))
select column_name,
       UPPER(data_type) as column_type
from information_schema.columns a
         join table_info b on a.table_name = b.table_name and a.table_schema = b.schema_name
    """
    )
    with conn.cursor(as_dict=True) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [row for row in rows]
        return columns


csv_in_ft = f"{sc.SOH}{sc.STX}{sc.STX}{sc.SOH}"
csv_in_rt = f"{sc.SOH}{sc.ETX}{sc.ETX}{sc.SOH}"
csv_bin_ft = sa.SOH + sa.STX + sa.STX + sa.SOH
csv_bin_rtx = sa.SOH + sa.ETX + sa.ETX + sa.SOH
csv_out_ft = ","
csv_out_rt = sc.LN


def mssql_csv_escape(line: str) -> str:
    line = (
        line.replace(sc.LN, f"\\{sc.LN}")
        .replace(sc.TAB, f"\\{sc.TAB}")
        .replace(sc.CR, f"\\{sc.CR}")
        .replace(sc.BS, f"\\{sc.BS}")
        .replace(sc.FF, f"\\{sc.FF}")
    )
    fields = line.split(csv_in_ft)
    fields = ['"' + _.replace('"', '""') + '"' for _ in fields]
    return csv_out_ft.join(fields) + csv_out_rt


def mssql_json_escape(line: str) -> str:
    data = orjson.loads(line)
    data = unmask_dot(data)
    return orjson.dumps(data).decode("utf-8") + sc.LN


def mssql_tsv_escape(s: str) -> str:
    return (
        s.replace("\\t", "\\\\t")
        .replace("\\n", "\\\\n")
        .replace("\\r", "\\\\r")
        .replace("\\f", "\\\\f")
        .replace("\\b", "\\\\b")
    )
