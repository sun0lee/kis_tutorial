from typing import Optional, List, Dict, Any

from core.db_manager import DatabaseManager
from core.api_client import KisClient

class MarketDataManager:
    def __init__(self, kis_client: KisClient
                    , db_manager: DatabaseManager
                    , api_configs:List[Dict[str, Any]]
                    , code_list: List[str]):
        self.kis_client = kis_client
        self.db_manager = db_manager
        self.code_list = code_list
        self.api_call_configs = api_configs

    def save_data(self, job_list: Optional[List[str]] = None
                  , dynamic_params: Optional[Dict[str, Any]] = None):

        configs_to_run = self.api_call_configs
        if job_list:
            configs_to_run = [config for config in self.api_call_configs if config["job"] in job_list]

            if not configs_to_run:
                print(f"경고: 선택된 API 작업 ({job_list}) 중 실행할 대상이 없습니다. 작업 이름을 정확히 확인해주세요.")
                return  # 실행할 설정이 없으면 함수 종료

        for config in configs_to_run:
            print(f"\n============================================================")
            print(f"=== API 호출 설정: '{config['job']}' : '{config['name']}' 처리 시작 ===")
            print(f"============================================================\n")

            current_endpoint = config["endpoint"]
            current_tr_id = config["tr_id"]
            current_fixed_params = config["fixed_params"]

            for code in self.code_list:
                print(f"  --- 종목 코드: {code} (작업: {config['name']}) 데이터 처리 시작 ---")

                params_for_call = current_fixed_params.copy()
                params_for_call["FID_INPUT_ISCD"] = code

                data = self.kis_client._call_api(current_endpoint, current_tr_id, params_for_call)

                if data:
                    print(data)

                    print(f" [DatabaseManager] 응답 데이터를 SQLite 데이터베이스에 적재합니다...")
                    rowid = self.db_manager.insert(current_tr_id, code, data)
                    print(f"  데이터베이스에 rowid = {rowid}의 데이터가 적재되었습니다.")
                else:
                    print(f" API 호출이 실패하여 종목코드 {code} (작업: {config['name']}) 데이터를 적재하지 않습니다.")

                print(f"  --- 종목 코드: {code} (작업: {config['name']}) 데이터 처리 완료 ---\n")

            print(f"\n============================================================")
            print(f"=== API 호출 설정: '{config['name']}' 처리 완료 ===")
            print(f"============================================================\n")