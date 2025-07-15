from typing import Optional, List, Dict, Any

from core.db_manager import DatabaseManager
from core.api_client import KisClient

class MarketDataManager:
    def __init__(self, kis_client: KisClient
                    , db_manager: DatabaseManager
                    , api_configs:List[Dict[str, Any]]
                    , inst_list: List[str]):
        self.kis_client = kis_client
        self.db_manager = db_manager
        self.inst_list = inst_list
        self.api_call_configs = api_configs

    def save_data(self, job_list: Optional[List[str]] = None):

        configs_to_run = self.api_call_configs
        if job_list:
            configs_to_run = [config for config in self.api_call_configs if config["job"] in job_list]

            if not configs_to_run:
                print(f"경고: 선택된 API 작업 ({job_list}) 중 실행할 대상이 없습니다. 작업 이름을 정확히 확인해주세요.")
                return  # 실행할 설정이 없으면 함수 종료

        if not self.inst_list:
            print("처리할 종목 코드가 없습니다 (mst_inst에서 use_yn='Y'인 종목이 없거나 조회가 실패했습니다).")
            return

        for config in configs_to_run:
            print(f"\n============================================================")
            print(f"=== API 호출 설정: '{config['job']}' : '{config['name']}' 처리 시작 ===")
            print(f"============================================================\n")

            cur_endpoint = config["endpoint"]
            cur_tr_id = config["tr_id"]
            cur_params = config["params"]

            for inst in self.inst_list:
                cur_code = inst['shrn_iscd']
                cur_mrkt_div = inst['mrkt_div']
                cur_kor_name = inst['kor_name']

                print(f"  --- 종목: {cur_kor_name} ({cur_code}, 시장 구분: {cur_mrkt_div}) (작업: {config['name']}) 데이터 처리 시작 ---")

                params = cur_params.copy()
                params["FID_INPUT_ISCD"] = cur_code
                params["FID_COND_MRKT_DIV_CODE"]= cur_mrkt_div

                data = self.kis_client._call_api(cur_endpoint, cur_tr_id, params)

                if data:
                    print(data)

                    print(f" [DatabaseManager] 응답 데이터를 SQLite 데이터베이스에 적재합니다...")
                    rowid = self.db_manager.insert(cur_tr_id, cur_code, params, data)
                    print(f"  데이터베이스에 rowid = {rowid}의 데이터가 적재되었습니다.")
                else:
                    print(f" API 호출이 실패하여 종목코드 {cur_code} (작업: {config['name']}) 데이터를 적재하지 않습니다.")

                print(f"  --- 종목 코드: {cur_code} (작업: {config['name']}) 데이터 처리 완료 ---\n")

            print(f"\n============================================================")
            print(f"=== API 호출 설정: '{config['name']}' 처리 완료 ===")
            print(f"============================================================\n")