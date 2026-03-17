#!/usr/bin/env python3

import json
from pathlib import Path
from typing import List, Tuple, Dict, Any

from flask import Flask, request, render_template_string, jsonify
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
    "TIMESTAMP_NTZ",
}

SCRIPT_DIR = Path(__file__).resolve().parent
JSON_PATH = SCRIPT_DIR / "creds.json"

with open(JSON_PATH) as f:
    creds = json.load(f)

ACCOUNT = creds["ACCOUNT"]
USER = creds["USER"]
WAREHOUSE = creds["WAREHOUSE"]
ROLE = creds["ROLE"]
PRIVATE_KEY_FILE = creds["PRIVATE_KEY_FILE"]
PRIVATE_KEY_PASSPHRASE = creds.get("PRIVATE_KEY_PASSPHRASE")
DEFAULT_DATABASE = creds.get("DATABASE", "")
DEFAULT_SCHEMA = creds.get("SCHEMA", "")

app = Flask(__name__)


def quote_ident(name: str) -> str:
    """Safely quote a Snowflake identifier, preserving case-sensitive names."""
    return '"' + name.replace('"', '""') + '"'


def get_connection(database: str | None = None, schema: str | None = None):
    """Return a Snowflake connection using credentials from creds.json.
    
    If database or schema are provided, they will be included in the connection parameters.
    Parameters
    ----------
    database : str | None
        Optional database name to connect to.
    schema : str | None
        Optional schema name to connect to. 
    """
    connect_kwargs = {
        "account": ACCOUNT,
        "user": USER,
        "warehouse": WAREHOUSE,
        "role": ROLE,
        "private_key_file": PRIVATE_KEY_FILE,
    }

    if PRIVATE_KEY_PASSPHRASE:
        connect_kwargs["private_key_file_pwd"] = PRIVATE_KEY_PASSPHRASE
    if database:
        connect_kwargs["database"] = database
    if schema:
        connect_kwargs["schema"] = schema

    return snowflake.connector.connect(**connect_kwargs)


def get_databases(conn) -> list[str]:
    """Return a list of database names in the Snowflake account.
    Parameters
    ----------
    conn : snowflake.connector.connection
        An active Snowflake connection.
    Returns
    -------
    list[str]
        List of database names."""
    sql = "SHOW DATABASES"
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        description = [col[0] for col in cur.description]

    try:
        name_idx = description.index("name")
    except ValueError:
        name_idx = 1

    return [row[name_idx] for row in rows]


def get_schemas(conn, database: str) -> list[str]:
    """Return a list of schema names in a Snowflake database.
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
        return [row[0] for row in cur.fetchall()]


def get_tables(conn, database: str, schema: str) -> list[str]:
    """Return a list of table names in a Snowflake database and schema.
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
        List of table names."""
    sql = f"""
        SELECT table_name
        FROM {quote_ident(database)}.information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        return [row[0] for row in cur.fetchall()]


def get_searchable_columns(conn, database: str, schema: str, table: str) -> List[Tuple[str, str]]:
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

    return [(col, dtype) for col, dtype in rows if (dtype or "").upper() in TEXT_LIKE_TYPES]


def build_search_sql(database: str, schema: str, table: str, columns: List[Tuple[str, str]], limit: int) -> str:
    fq_table = f"{quote_ident(database)}.{quote_ident(schema)}.{quote_ident(table)}"

    branches = []
    for col_name, _data_type in columns:
        qcol = quote_ident(col_name)
        branch = f"""
            SELECT
                %s AS matched_column,
                t.*
            FROM {fq_table} t
            WHERE {qcol} ILIKE %s
        """
        branches.append(branch.strip())

    if not branches:
        raise ValueError("No searchable columns found.")

    union_sql = "\nUNION ALL\n".join(branches)
    return f"""
        {union_sql}
        LIMIT {limit}
    """.strip()


def search_table(conn, database: str, schema: str, table: str, search: str, limit: int) -> Dict[str, Any]:
    columns = get_searchable_columns(conn, database, schema, table)
    if not columns:
        return {
            "database": database,
            "schema": schema,
            "table": table,
            "columns_searched": [],
            "column_count": 0,
            "match_count": 0,
            "colnames": [],
            "rows": [],
        }

    sql = build_search_sql(database, schema, table, columns, limit)
    like_value = f"%{search}%"
    params = []
    for col_name, _ in columns:
        params.append(col_name)
        params.append(like_value)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    return {
        "database": database,
        "schema": schema,
        "table": table,
        "columns_searched": [c[0] for c in columns],
        "column_count": len(columns),
        "match_count": len(rows),
        "colnames": colnames,
        "rows": rows,
    }


def parse_scope(database: str, schema: str | None, table: str | None) -> tuple[str, list[str], dict[str, list[str]]]:
    conn = get_connection(database=database, schema=schema)
    try:
        if schema:
            schemas = [schema]
        else:
            schemas = get_schemas(conn, database)

        tables_by_schema: dict[str, list[str]] = {}
        for s in schemas:
            if table:
                tables_by_schema[s] = [table]
            else:
                tables_by_schema[s] = get_tables(conn, database, s)

        return database, schemas, tables_by_schema
    finally:
        conn.close()


HTML = """
<script>
  const appBase = "{{ request.script_root }}";
  const databaseSelect = document.getElementById('database');
  const schemaSelect = document.getElementById('schema');
  const tableSelect = document.getElementById('table');
  const loadingIndicator = document.getElementById('loading-indicator');
  const reloadDbButton = document.getElementById('reload-db');

  function setLoading(isLoading, message = 'Loading metadata...') {
    loadingIndicator.style.display = isLoading ? 'inline' : 'none';
    loadingIndicator.textContent = message;
  }

  function fillSelect(selectEl, values, blankLabel, selectedValue = '') {
    selectEl.innerHTML = '';
    const blankOption = document.createElement('option');
    blankOption.value = '';
    blankOption.textContent = blankLabel;
    selectEl.appendChild(blankOption);

    values.forEach(value => {
      const option = document.createElement('option');
      option.value = value;
      option.textContent = value;
      if (value === selectedValue) {
        option.selected = true;
      }
      selectEl.appendChild(option);
    });
  }

  async function loadDatabases(selectedValue = '') {
    setLoading(true, 'Loading databases...');
    try {
      const response = await fetch(`${appBase}/api/databases`);
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to load databases');
      }
      fillSelect(databaseSelect, data.databases, 'Select a database...', selectedValue || databaseSelect.value);
    } catch (error) {
      console.error(error);
      alert(error.message);
    } finally {
      setLoading(false);
    }
  }

  async function loadSchemas(database, selectedValue = '') {
    fillSelect(schemaSelect, [], 'All schemas');
    fillSelect(tableSelect, [], 'All tables');
    if (!database) return;

    setLoading(true, 'Loading schemas...');
    try {
      const response = await fetch(`${appBase}/api/schemas?database=${encodeURIComponent(database)}`);
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to load schemas');
      }
      fillSelect(schemaSelect, data.schemas, 'All schemas', selectedValue);
    } catch (error) {
      console.error(error);
      alert(error.message);
    } finally {
      setLoading(false);
    }
  }

  async function loadTables(database, schema, selectedValue = '') {
    fillSelect(tableSelect, [], 'All tables');
    if (!database || !schema) return;

    setLoading(true, 'Loading tables...');
    try {
      const response = await fetch(`${appBase}/api/tables?database=${encodeURIComponent(database)}&schema=${encodeURIComponent(schema)}`);
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to load tables');
      }
      fillSelect(tableSelect, data.tables, 'All tables', selectedValue);
    } catch (error) {
      console.error(error);
      alert(error.message);
    } finally {
      setLoading(false);
    }
  }

  databaseSelect.addEventListener('change', async () => {
    await loadSchemas(databaseSelect.value);
  });

  schemaSelect.addEventListener('change', async () => {
    await loadTables(databaseSelect.value, schemaSelect.value);
  });

  reloadDbButton.addEventListener('click', async () => {
    await loadDatabases(databaseSelect.value);
    if (databaseSelect.value) {
      await loadSchemas(databaseSelect.value, schemaSelect.value);
      if (schemaSelect.value) {
        await loadTables(databaseSelect.value, schemaSelect.value, tableSelect.value);
      }
    }
  });

  window.addEventListener('load', async () => {
    if (databaseSelect.options.length <= 1) {
      await loadDatabases('{{ form.database }}');
    }
    if ('{{ form.database }}') {
      await loadSchemas('{{ form.database }}', '{{ form.schema }}');
    }
    if ('{{ form.database }}' && '{{ form.schema }}') {
      await loadTables('{{ form.database }}', '{{ form.schema }}', '{{ form.table }}');
    }
  });
</script>
"""


def render_page(form=None, results=None, summary=None, error=None, databases=None, schemas=None, tables=None):
    return render_template_string(
        HTML,
        form=form or {"database": DEFAULT_DATABASE, "schema": DEFAULT_SCHEMA, "table": "", "search": "", "limit": 100},
        results=results or [],
        summary=summary,
        error=error,
        databases=databases or [],
        schemas=schemas or [],
        tables=tables or [],
        user=USER,
        warehouse=WAREHOUSE,
        role=ROLE,
    )


@app.get("/")
def index():
    databases = []
    schemas = []
    tables = []
    try:
        conn = get_connection()
        try:
            databases = get_databases(conn)
            if DEFAULT_DATABASE:
                schemas = get_schemas(conn, DEFAULT_DATABASE)
                if DEFAULT_SCHEMA:
                    tables = get_tables(conn, DEFAULT_DATABASE, DEFAULT_SCHEMA)
        finally:
            conn.close()
    except Exception:
        pass

    return render_page(
        form={"database": DEFAULT_DATABASE, "schema": DEFAULT_SCHEMA, "table": "", "search": "", "limit": 100},
        databases=databases,
        schemas=schemas,
        tables=tables,
    )


@app.get("/api/databases")
def api_databases():
    try:
        conn = get_connection()
        try:
            return jsonify({"databases": get_databases(conn)})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/api/schemas")
def api_schemas():
    database = (request.args.get("database") or "").strip()
    if not database:
        return jsonify({"error": "database is required"}), 400

    try:
        conn = get_connection(database=database)
        try:
            return jsonify({"schemas": get_schemas(conn, database)})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/api/tables")
def api_tables():
    database = (request.args.get("database") or "").strip()
    schema = (request.args.get("schema") or "").strip()
    if not database or not schema:
        return jsonify({"error": "database and schema are required"}), 400

    try:
        conn = get_connection(database=database, schema=schema)
        try:
            return jsonify({"tables": get_tables(conn, database, schema)})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/search")
def search():
    database = (request.form.get("database") or "").strip()
    schema = (request.form.get("schema") or "").strip() or None
    table = (request.form.get("table") or "").strip() or None
    search_value = (request.form.get("search") or "").strip()

    try:
        limit = int(request.form.get("limit") or "100")
    except ValueError:
        limit = 100

    form = {
        "database": database,
        "schema": schema or "",
        "table": table or "",
        "search": search_value,
        "limit": limit,
    }

    databases = []
    schemas = []
    tables = []

    try:
        conn_meta = get_connection(database=database or None, schema=schema)
        try:
            databases = get_databases(conn_meta)
            if database:
                schemas = get_schemas(conn_meta, database)
            if database and schema:
                tables = get_tables(conn_meta, database, schema)
        finally:
            conn_meta.close()
    except Exception:
        pass

    if not database:
        return render_page(form=form, results=[], summary=None, error="Database is required.", databases=databases, schemas=schemas, tables=tables)
    if not search_value:
        return render_page(form=form, results=[], summary=None, error="Search string is required.", databases=databases, schemas=schemas, tables=tables)

    results = []
    try:
        _database, searched_schemas, tables_by_schema = parse_scope(database, schema, table)
        conn = get_connection(database=database, schema=schema)
        try:
            for s in searched_schemas:
                for t in tables_by_schema.get(s, []):
                    results.append(search_table(conn, database, s, t, search_value, limit))
        finally:
            conn.close()
    except Exception as e:
        return render_page(form=form, results=[], summary=None, error=str(e), databases=databases, schemas=schemas, tables=tables)

    tables_with_matches = sum(1 for r in results if r["match_count"] > 0)
    total_matches = sum(r["match_count"] for r in results)
    summary = {
        "database": database,
        "schema_count": len(searched_schemas),
        "table_count": sum(len(v) for v in tables_by_schema.values()),
        "tables_with_matches": tables_with_matches,
        "total_matches": total_matches,
    }

    return render_page(form=form, results=results, summary=summary, error=None, databases=databases, schemas=schemas, tables=tables)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
