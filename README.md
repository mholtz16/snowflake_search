# Snowflake Schema Search Tool

A command-line Python utility that searches for a string across one or more Snowflake tables.
It can search:

* a **single table**
* an **entire schema**
* or an **entire database** (by iterating schemas)

The script introspects Snowflake metadata to identify searchable columns and builds a dynamic query that searches those columns for the provided string.

---

# Features

* Connects to **Snowflake using key-pair authentication**
* Searches **multiple column types**, including:

  * VARCHAR / TEXT
  * DATE / TIMESTAMP
  * VARIANT / ARRAY
* Automatically discovers:

  * schemas
  * tables
  * columns
* Works with **case-sensitive (quoted) identifiers**
* Returns the **matching column and row data**
* Supports **fully qualified table names**
* Limits result size to prevent runaway queries

---

# Requirements

* Python **3.9+**
* Snowflake account with access to the target database
* A Snowflake user configured for **key-pair authentication**

Python dependency:

```
snowflake-connector-python
```

Install with:

```
pip install snowflake-connector-python
```

---

# Setup

Clone the repository:

```
git clone <repo-url>
cd snowflake-schema-search
```

Create a virtual environment:

```
python -m venv venv
```

Activate it:

**macOS / Linux**

```
source venv/bin/activate
```

Install dependencies:

```
pip install snowflake-connector-python
```

---

# Configuration

Connection settings are currently defined in creds.json.  See the example for details.

Your Snowflake user must already have the public key registered.

Snowflake documentation:
https://docs.snowflake.com/en/user-guide/key-pair-auth

---

# Usage

```
python search.py --search <string> [options]
```

---

# Search a Single Table

```
python search.py \
  --database MYDB \
  --schema PUBLIC \
  --table CUSTOMERS \
  --search gmail.com
```

---

# Search an Entire Schema

```
python search.py \
  --database MYDB \
  --schema PUBLIC \
  --search gmail.com
```

The script will automatically:

1. list tables in the schema
2. inspect their columns
3. search each table

---

# Search an Entire Database

```
python search.py \
  --database MYDB \
  --search gmail.com
```

The script will:

1. list schemas
2. list tables in each schema
3. search every table

---

# Using a Fully Qualified Table Name

Instead of specifying database/schema/table separately:

```
python search.py \
  --table-fqn MYDB.PUBLIC.CUSTOMERS \
  --search gmail.com
```

Quoted identifiers are also supported:

```
python search.py \
  --table-fqn '"my_db"."my_schema"."orders"' \
  --search gmail.com
```

---

# Limiting Results

To prevent very large result sets:

```
--limit 100
```

Example:

```
python search.py \
  --database MYDB \
  --schema PUBLIC \
  --search gmail.com \
  --limit 50
```

---

# Example Output

```
Search term: gmail.com
Table: MYDB.PUBLIC.CUSTOMERS
Columns searched: EMAIL, NOTES
Matches returned: 2

matched_column  CUSTOMER_ID  EMAIL                NAME
EMAIL           1045         user@gmail.com       Jane Doe
EMAIL           2093         example@gmail.com    John Smith
```

---

# Supported Column Types

The script currently searches these Snowflake column types:

```
VARCHAR
CHAR
TEXT
STRING
DATE
DATETIME
TIMESTAMP
TIMESTAMP_LTZ
TIMESTAMP_TZ
TIMESTAMP_NTZ
VARIANT
```

---

# Notes

### Performance

Searching very large schemas may generate large SQL queries and can be expensive.

Recommended usage:

* search **specific schemas**
* use a **limit**
* run with a **small warehouse** if exploratory

---

### Case-Sensitive Object Names

Snowflake objects created with quotes preserve case:

```
CREATE TABLE "orders" (...)
```

This script handles those safely by quoting identifiers internally.

---

# Security

Do **not commit private keys** to the repository.

The included `.gitignore` ignores common key formats:

```
*.pem
*.key
*.p8
```

---

# Future Improvements

Possible enhancements:

* search entire **Snowflake account**
* optional **JSON row output**
* export results to **CSV**
* parallel table scanning
* filter tables by name pattern
* configurable column type list

---

# License

GPLv2 License
