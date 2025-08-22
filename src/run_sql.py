# main.py

import os
from core.db_manager import DatabaseManager


if __name__ == "__main__":

    # sql_file_name = "trn_inst.sql"
    # sql_file_name = "update mst_pf_pos.sql"
    sql_file_name = "trn_pf_pos_val.sql"
    db_manager = DatabaseManager()

    # 3. SQL 파일 실행 로직
    try:
        print(f"작업시작: {sql_file_name}")
        rows_affected = db_manager.execute_sql(sql_file_name)
        print(f"영향을 받은 행의 수: {rows_affected}")

    except Exception as e:
        print(f"\n스크립트 실행 실패: {e}")



