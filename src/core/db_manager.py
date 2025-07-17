import sqlite3
import json
import os
from typing  import List, Dict, Any
from datetime import datetime
from core import DATA_PATH, SQL_DIR


RAW_API_TABLE = "rst_raw_api"
# RAW_API_SCHEMA = {
#     "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
#     "response_type": "TEXT NOT NULL",
#     "symbol": "TEXT",
#     "param": "TEXT NOT NULL",
#     "data": "TEXT NOT NULL",
#     "created_at": "TEXT NOT NULL"
# }

class DatabaseManager:

    def __init__(self):
        self.db_path = DATA_PATH
        self.sql_dir = SQL_DIR

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # 컬럼 이름으로 접근 가능하게 설정
        # print(f"[DEBUG] _get_connection: 연결 객체 생성됨: {conn}")
        return conn

    def get_inst_list(self) -> List[Dict[str, Any]]:
        insts = []
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            # print(f"[DEBUG] get_inst_list: 커서 객체 생성됨: {cursor}")

            query = "SELECT shrn_iscd, mrkt_div, kor_name FROM mst_inst WHERE use_yn = 'Y'"
            cursor.execute(query)

            rows = cursor.fetchall()
            # print(f"[DEBUG] get_inst_list: 조회된 행의 수: {len(rows)}")

            if not rows:
                print("경고: 'mst_inst' 테이블에 'use_yn = 'Y''인 종목이 없습니다. API 호출 대상 없음.")
                return []

            column_names = rows[0].keys()  # 첫 번째 행에서 컬럼 이름을 가져옵니다.
            # print(f"[DEBUG] get_inst_list: 컬럼 이름: {column_names}")

            for row in rows:
                item = {col_name: row[col_name] for col_name in column_names}
                insts.append(item)

        except Exception as e:
            print(f"ERROR: mst_inst 테이블에서 종목 조회 중 오류 발생: {e}")
            insts = []  # 오류 발생 시 빈 리스트 반환
        finally:
            if conn:
                conn.close()
        return insts

    def insert(self, response_type: str, symbol: str, params: dict, raw_data: dict) -> int:

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            timestamp = datetime.now().isoformat()
            json_str = json.dumps(raw_data, ensure_ascii=False, indent=2)
            json_str_params = json.dumps(params, ensure_ascii=False, indent=2)

            cursor.execute(f'''
                INSERT INTO {RAW_API_TABLE} (response_type, symbol, param, data, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (response_type, symbol, json_str_params, json_str, timestamp))
            conn.commit()
            print(
                f"Raw JSON data for '{symbol}' ({response_type}) inserted into '{RAW_API_TABLE}' successfully.")
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

    def execute_sql (self, file_name: str):
        sql_file_path = os.path.join(self.sql_dir,file_name)

        if not os.path.exists(sql_file_path):
            print(f"ERROR: SQL 파일 '{sql_file_path}'를 찾을 수 없습니다.")
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_query = f.read()

            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql_query)
            conn.commit()
            print(f"SQL 파일 '{file_name}' 실행 및 커밋 완료.")
            return cursor.rowcount  # 영향을 받은 행의 수 반환

        except sqlite3.Error as e:
            print(f"ERROR: SQL 파일 '{file_name}' 실행 중 데이터베이스 오류 발생: {e}")
            conn.rollback()
            raise
        except Exception as e:
            print(f"ERROR: SQL 파일 '{file_name}' 처리 중 알 수 없는 오류 발생: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def transform (self, tr_id:str, idx: int = 1):
        file_name = f"{tr_id}_{idx:02d}.sql"
        print(f"\n---원본 데이터 변환 : TR ID '{tr_id}', output '{idx}'---")
        try:
            self.execute_sql (file_name)
            print(f"원본 데이터 변환 완료: {file_name}")
        except Exception as e:
            print(f"원본 데이터 변환 실패: {file_name} - {e}")
