import textwrap

import orjson
import sqlalchemy

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


def table_spaceused(table, conn: sqlalchemy.Connection) -> Dict[str, Any]:
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
    cur = conn.execute(sqlalchemy.text(sql))
    row = cur.fetchone()
    if row:
        return row._asdict()


def table_data_size(table, conn: sqlalchemy.Connection) -> int:
    res = table_spaceused(table, conn)
    if not res:
        return 0
    data = res["data"].replace("KB", "").strip()
    return int(data) * KB  # Bytes


def table_columns(table, conn: sqlalchemy.Connection) -> List[Dict]:
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
    cur = conn.execute(sqlalchemy.text(sql))
    rows = cur.fetchall()
    columns = [row._asdict() for row in rows]
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
