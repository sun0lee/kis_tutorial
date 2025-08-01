import os

current_dir = os.path.dirname(os.path.abspath(__file__))  # src/core
BASE_DIR = os.path.abspath(os.path.join(current_dir, "../../"))
DATA_PATH = os.path.join(BASE_DIR, "db","DB_SQLite")
SQL_DIR = os.path.join(BASE_DIR, "sql")