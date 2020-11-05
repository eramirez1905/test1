from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
SQL_DIR = CURRENT_DIR / "sql"

UPDATE_TABLE_SQL = SQL_DIR / "update_table.sql"
