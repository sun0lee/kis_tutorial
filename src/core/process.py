from typing import Optional, List, Dict, Any
from core.db_manager import DatabaseManager
from core.api_client import KisClient
import json

class MarketDataManager:
    def __init__(self, kis_client: KisClient
                    , db_manager: DatabaseManager):
        self.kis_client = kis_client
        self.db_manager = db_manager

    def per_symbol_jobs(self):

        per_symbol_jobs = self.db_manager.get_job_list(job_type='PER_SYMBOL')
        inst_list = self.db_manager.get_inst_list()

        if not per_symbol_jobs:
            print("\n 실행가능한 'PER_SYMBOL' (종목별) 작업이 없습니다. mst_api_job의 use_yn를 확인하세요.")
            return

        if not inst_list:
            print("\n경고: 'PER_SYMBOL' 타입 작업을 위해 'inst_list'가 제공되지 않았습니다. mst_api_inst의 use_yn를 확인하세요.")
            return

        print("\n--- Processing 'PER_SYMBOL' (Symbol-specific) Jobs ---")
        for config in per_symbol_jobs:
            print(f"\n============================================================")
            print(f"=== API Job '{config['job_id']}' : '{config['job_name']}' 처리 시작 ===")
            print(f"============================================================\n")

            base_url = config["base_url"]
            endpoint = config["endpoint"]
            tr_id = config["tr_id"]
            params=config["params"]

            if "FID_INPUT_ISCD" not in params:
                params["FID_INPUT_ISCD"] = ""  # 임시 값, 아래 루프에서 채워짐

            for inst in inst_list:
                cur_code = inst['shrn_iscd']
                cur_mrkt_div = inst['mrkt_div']
                cur_kor_name = inst['kor_name']

                params_for_call = params.copy()
                params_for_call["FID_INPUT_ISCD"] = cur_code
                params_for_call["FID_COND_MRKT_DIV_CODE"] = cur_mrkt_div

                print(
                    f"  --- 종목: {cur_kor_name} ({cur_code}, 시장 구분: {cur_mrkt_div}) (작업: {config['job_name']}) 데이터 처리 중 ---")

                data = self.kis_client._call_api(base_url,endpoint,tr_id,params_for_call)

                if data:
                    print(f"    [API 응답]: {data.get('msg1') if 'msg1' in data else '데이터 수신 완료'}")
                    rowid = self.db_manager.insert(tr_id, cur_code, params_for_call, data)
                    print(f"    데이터베이스에 rowid = {rowid}의 데이터가 적재되었습니다.")
                else:
                    print(f"    API 호출 실패: 종목코드 {cur_code} (작업: {config['job_name']}) 데이터를 적재하지 않습니다.")

                print(f"  --- 종목 코드: {cur_code} (작업: {config['job_name']}) 데이터 처리 완료 ---\n")

            print(f"\n============================================================")
            print(f"=== API Job '{config['job_name']}' 처리 완료 ===")
            print(f"============================================================\n")

    def board_all_jobs(self):
        board_all_jobs = self.db_manager.get_job_list(job_type='BOARD_ALL')

        if not board_all_jobs:
            print("\n 실행가능한 'BOARD_ALL' (전체 시장 전광판) 작업이 없습니다. mst_api_job의 use_yn를 확인하세요.")
            return

        print("\n--- Processing 'BOARD_ALL' (Market-Wide) Jobs ---")
        for config in board_all_jobs:
            print(f"\n============================================================")
            print(f"=== API Job '{config['job_id']}' : '{config['job_name']}' 처리 시작 ===")
            print(f"============================================================\n")

            base_url = config["base_url"]
            endpoint = config["endpoint"]
            tr_id = config["tr_id"]
            params = config["params"]


            print(f"  --- 전체 시장 현황 (작업: {config['job_name']}) 데이터 처리 중 'BOARD_ALL' 유형 (TR_ID: {tr_id})  ---")
            data = self.kis_client._call_api(base_url,endpoint,tr_id,params)

            if data:
                print(f"      [API 응답]: {data.get('msg1') if 'msg1' in data else '데이터 수신 완료'}")
                rowid = self.db_manager.insert(tr_id, 'ALL', params, data)
                print(f"      데이터베이스에 rowid = {rowid}의 데이터가 적재되었습니다.")
            else:
                print(f"      API 호출 실패: 일반 'BOARD_ALL' 유형 (작업: {config['job_name']}) 데이터를 적재하지 않습니다.")

            print(f"  --- 전체 시장 현황 (작업: {config['job_name']}) 데이터 처리 완료 ---\n")


    def transform_data(self):

        configs_to_run = self.db_manager.get_job_list()

        for config in configs_to_run:
            print(f"\n============================================================")
            print(f"=== API 호출 설정: '{config['job_id']}' : '{config['job_name']}' Raw data 변환 처리 시작 ===")
            print(f"============================================================\n")

            cur_tr_id = config["tr_id"]
            cur_outputs = json.loads(config["outputs"])

            for idx in cur_outputs:
                self.db_manager.transform(cur_tr_id, idx)

            print("\n============================================================")
            print(f"=== API 호출 설정: '{config['job_id']}' : '{config['job_name']}' Raw data 변환 작업 완료 ===")
            print("============================================================\n")