# Gemini Instructional Context - DopeDB

## Project Overview
DopeDB is a lightweight, relational database management system (RDBMS) implemented in Python. It features a custom engine capable of handling core database operations, a SQL-like query parser, and a persistent storage mechanism using JSON.

### Key Technologies
- **Python 3**: Core language for the engine, REPL, and tests.
- **Flask**: Used to build the demonstration web application.
- **Regex (re)**: Utilized for SQL query parsing.
- **JSON**: Used as the underlying file storage format.

### Architecture
- **`dopedb/engine.py`**: The heart of the system. Contains the `Table` and `Database` classes.
    - **`Table` class**: Manages row-level data, schema validation, and in-memory indexing for `PRIMARY KEY` and `UNIQUE` constraints.
    - **`Database` class**: Manages multiple tables, metadata persistence, and translates SQL-like strings into engine method calls.
- **`data/`**: Directory where table data (`.json`) and metadata (`metadata.json`) are persisted.
- **SQL Parser**: A regex-based parser within `Database.execute()` that supports `CREATE TABLE`, `INSERT`, `SELECT` (including `JOIN`), `UPDATE`, and `DELETE`.

## Building and Running

### Prerequisites
- Python 3.x
- Flask (for the web app)

### Key Commands
- **Run REPL**: `python3 repl.py` (Interactive SQL-like interface)
- **Run Web App**: `python3 app.py` (Starts Flask server on port 5000)
- **Run Tests**: `python3 test_db.py` (Verifies CRUD, Joins, and Constraints)

## Development Conventions

### Data Types
Supported types in `CREATE TABLE`:
- `INT`: Integers (automatically cast during insertion and querying).
- `TEXT`: Strings.

### Constraints
- `PRIMARY KEY`: Unique, non-null, and indexed.
- `UNIQUE`: Unique values, indexed.

### Query Limitations
- `JOIN`: Supports simple inner joins on a single column equality (`ON t1.c1 = t2.c2`).
- `WHERE`: Supports single column equality matches (`WHERE col = val`).
- `INSERT`: Handles commas within quoted strings using `csv.reader` logic.

### Persistence
The engine automatically saves to the `data/` directory after every modifying operation (`INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`).
