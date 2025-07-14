import sqlite3
import json
from datetime import datetime
from core import DATA_PATH

RAW_API_RESPONSES_TABLE = "rst_raw_api"
RAW_API_RESPONSES_SCHEMA = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "response_type": "TEXT NOT NULL",
    "symbol": "TEXT",
    "data": "TEXT NOT NULL",
    "created_at": "TEXT NOT NULL"
}

class DatabaseManager:

    def __init__(self):
        self.db_path = DATA_PATH

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def insert(self, response_type: str, request_symbol: str, raw_data: dict) -> int:

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            timestamp = datetime.now().isoformat()
            json_string = json.dumps(raw_data, ensure_ascii=False, indent=2)

            cursor.execute(f'''
                INSERT INTO {RAW_API_RESPONSES_TABLE} (response_type, symbol, data, created_at)
                VALUES (?, ?, ?, ?)
            ''', (response_type, request_symbol, json_string, timestamp))
            conn.commit()
            print(
                f"Raw JSON data for '{request_symbol}' ({response_type}) inserted into '{RAW_API_RESPONSES_TABLE}' successfully.")
            return cursor.lastrowid
        except sqlite3.Error as e:
            print(f"Error inserting raw JSON data into DB: {e}")
            conn.rollback()
            raise
        except TypeError as e:
            print(f"Error converting raw_data_object to JSON string: {e}")
            print(f"Type of raw_data_object: {type(raw_data)}")
            conn.rollback()
            raise
        finally:
            conn.close()