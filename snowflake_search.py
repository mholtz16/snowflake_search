#!/usr/bin/env python3

import argparse
import sys
from typing import List, Tuple
import json
from pathlib import Path

import snowflake.connector


TEXT_LIKE_TYPES = {
    "VARCHAR",
    "CHAR",
    "CHARACTER",
    "STRING",
    "TEXT",
    "DATE",
    "DATETIME",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
    "VARIANT",
    "TIMESTAMP",
    "TIMESTAMP_NTZ"
}

SCRIPT_DIR = Path(__file__).resolve().parent
json_path = SCRIPT_DIR / "creds.json"


with open(json_path) as f:
    creds = json.load(f)
ACCOUNT = creds["ACCOUNT"]
USER = creds["USER"]
WAREHOUSE = creds["WAREHOUSE"]
ROLE = creds["ROLE"]
PRIVATE_KEY_FILE = creds["PRIVATE_KEY_FILE"]

def get_schemas(conn, database: str) -> list[str]:
    """
    Return a list of schemas in a Snowflake database.

    Parameters
    ----------
    conn : snowflake.connector.connection
        An active Snowflake connection.
    database : str
        Database name.

    Returns
    -------
    list[str]
        List of schema names.
    """

    sql = f"""
        SELECT schema_name
        FROM {quote_ident(database)}.information_schema.schemata
        ORDER BY schema_name
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return [row[0] for row in rows]

def get_tables(conn, database: str, schema: str) -> list[str]:
    """
    Return a list of table names in a Snowflake database and schema.

    Parameters
    ----------
    conn : snowflake.connector.connection
        An active Snowflake connection.
    database : str
        Database name.
    schema : str
        Schema name.

    Returns
    -------
    list[str]
        List of table names.
    """

    sql = f"""
        SELECT table_name
        FROM {quote_ident(database)}.information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """

    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        rows = cur.fetchall()

    return [row[0] for row in rows]

def quote_ident(name: str) -> str:
    """Safely quote a Snowflake identifier."""
    return f'"{name}"'.replace('""','"')


def get_connection(args):
    connect_kwargs = {
        "account": ACCOUNT,
        "user": USER,
        "warehouse": WAREHOUSE,
        "role": ROLE,
        "private_key_file": PRIVATE_KEY_FILE,
        "database": args.database,
        "schema": args.schema,

    }

    # Snowflake supports an encrypted private key passphrase parameter.
    # Only include it if provided.


    return snowflake.connector.connect(**connect_kwargs)


def get_searchable_columns(conn, database: str, schema: str, table: str) -> List[Tuple[str, str]]:
    """
    Returns list of (column_name, data_type) for the target table.
    """
    print(f"getting searchable columns for {database}.{schema}.{table}")
    sql = f"""
        SELECT column_name, data_type
        FROM {quote_ident(database)}.information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """

    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        rows = cur.fetchall()

    if not rows:
        raise ValueError(f"Table not found or no visible columns: {database}.{schema}.{table}")

    searchable = [(col, dtype) for col, dtype in rows if (dtype or "").upper() in TEXT_LIKE_TYPES]
    return searchable

def get_schema_columns(
    conn,
    database: str,
    schema: str,
) -> List[Tuple[str, str, str]]:
    """
    Returns a list of:
      (table_name, column_name, data_type)
    for all searchable columns in the schema.
    """
    sql = f"""
        SELECT
            table_name,
            column_name,
            data_type
        FROM {quote_ident(database)}.information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
    """

    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        rows = cur.fetchall()

    if not rows:
        raise ValueError(f"No visible columns found in schema {database}.{schema}")


    searchable = [
        (table_name, column_name, data_type)
        for table_name, column_name, data_type in rows
        if (data_type or "").upper() in TEXT_LIKE_TYPES
    ]
    return searchable



def build_search_sql(database: str, schema: str, table: str, columns: List[Tuple[str, str]], limit: int) -> str:
    """
    Build a UNION ALL query that returns:
      matched_column, all table columns
    for each row where the search term appears in that column.
    """
    fq_table = f"{quote_ident(database)}.{quote_ident(schema)}.{quote_ident(table)}"

    branches = []
    for col_name, data_type in columns:
        qcol = quote_ident(col_name)

        predicate = f"{qcol} ILIKE %s"

        branch = f"""
            SELECT
                %s AS matched_column,
                t.*
            FROM {fq_table} t
            WHERE {predicate}
        """
        branches.append(branch.strip())

    if not branches:
        raise ValueError("No searchable columns found.")

    union_sql = "\nUNION ALL\n".join(branches)
    final_sql = f"""
        {union_sql}
        LIMIT {limit}
    """
    return final_sql.strip()


def search_table(conn, database: str, schema: str, table: str, search: str, limit: int):
    columns = get_searchable_columns(
        conn=conn,
        database=database,
        schema=schema,
        table=table,
    )

    if not columns:
        print("No searchable columns found.")
        sys.exit(0)

    sql = build_search_sql(
        database=database,
        schema=schema,
        table=table,
        columns=columns,
        limit=limit,
    )

    params = []
    like_value = f"%{search}%"

    for col_name, _ in columns:
        params.append(col_name)   # matched_column value
        params.append(like_value) # ILIKE predicate value

    with conn.cursor() as cur:
        cur.execute(sql, params)

        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    print(f"Search term: {search}")
    print(f"Table: {database}.{schema}.{table}")
    print(f"Columns searched: {', '.join(c[0] for c in columns)}")
    print(f"Matches returned: {len(rows)}")
    print()

    if not rows:
        print("No matches found.")
        return

    # Simple tab-separated output
    print("\t".join(colnames))
    for row in rows:
        printable = ["" if v is None else str(v).replace("\n", " ") for v in row]
        print("\t".join(printable))


def main():
    parser = argparse.ArgumentParser(
        description="Search a Snowflake table for a string across columns."
    )
    parser.add_argument("--search", required=True, help="String to search for")
    parser.add_argument(
        "--table-fqn",
        required=False,
        help="Fully qualified table name (DATABASE.SCHEMA.TABLE)"
    )
    parser.add_argument(
        "--database",
        required=False,
        help="Database name"
    )
    parser.add_argument(
        "--schema",
        required=False,
        help="Schema name"
    )
    parser.add_argument(
        "--table",
        required=False,
        help="Table name"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max rows to return (default: 100)",
    )

    args = parser.parse_args()
    if args.table_fqn:
        args.database, args.schema, args.table = [part.strip() for part in args.table_fqn.split(".")]
        

    try:
        print("Connecting to Snowflake...")
        conn = get_connection(args)
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        sys.exit(1)
    try:
        if not args.database:
            print("Error: Database name is required.", file=sys.stderr)
            sys.exit(1)
        if args.schema:
            s = [args.schema]
        else:
            s = get_schemas(conn, args.database)
        print(f"Schemas to search: {', '.join(s)}")
        for schema in s:
             print(f"Processing schema: {schema}")
             if args.table:
                t = [args.table]
             else:
                t = get_tables(conn, args.database, schema)
             print(f"Tables to search: {', '.join(t)}")
             for table in t:
                 print(f"Searching {args.database}.{schema}.{table}...")
                 search_table(
                    conn=conn,
                    database=args.database,
                    schema=schema,
                    table=table,
                    search=args.search,
                    limit=args.limit,
                    )
                
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()