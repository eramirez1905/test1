from glob import glob
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
SQL_DIR = CURRENT_DIR / "sql"
UDF_FILEPATHS = glob(f"{SQL_DIR}/*.sql")
