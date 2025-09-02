from typing import Optional, List, Dict, Any
from core.db_manager import DatabaseManager
from core.api_client import KisClient
import json
import datetime
from dateutil.relativedelta import relativedelta

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
            fixed_params = {
                param_meta['param_key']: param_meta['param_value']
                for param_meta in config["params"] if not param_meta['is_dynamic']
            }
            # params=config["params"]

            for inst in inst_list:
                cur_code = inst['shrn_iscd']
                cur_mrkt_div = inst['mrkt_div']
                cur_kor_name = inst['kor_name']

                params_for_call = {
                    'FID_INPUT_ISCD': cur_code,
                    'FID_COND_MRKT_DIV_CODE': cur_mrkt_div,
                    **fixed_params,
                }
                # 20250902 과거 데이터 가져올때 조회기간 옵션만기에 따라 동적으로 처리하기
                if tr_id == 'FHKIF03020100' :
                    try:
                        # 종목명에서 만기 년월 추출 (예: 'C 202501 332.5' -> '202501')
                        expiry_ym = cur_kor_name.split(' ')[1]

                        # 만기일 계산 로직: 해당 월의 셋째 목요일
                        year = int(expiry_ym[:4])
                        month = int(expiry_ym[4:])
                        # 만기일이 포함된 달의 첫 번째 날짜
                        first_day_of_month = datetime.date(year, month, 1)
                        # 만기일인 목요일을 찾음
                        if first_day_of_month.weekday() <= 3:  # 0=월, 1=화, 2=수, 3=목
                            expiry_date = first_day_of_month + datetime.timedelta(
                                days=(3 - first_day_of_month.weekday()) + 14)
                        else:
                            expiry_date = first_day_of_month + datetime.timedelta(
                                days=(10 - first_day_of_month.weekday()) + 14)

                        expiry_date_str = expiry_date.strftime('%Y%m%d')

                        # 조회 시작 날짜 계산 (만기일 2개월 전)
                        start_date = expiry_date - relativedelta(months=2)
                        start_date_str = start_date.strftime('%Y%m%d')

                        params_for_call['FID_INPUT_DATE_1'] = start_date_str
                        params_for_call['FID_INPUT_DATE_2'] = expiry_date_str

                    except (ValueError, IndexError):
                        print(f"  --- 경고: 종목명 '{cur_kor_name}'에서 만기일 정보를 파싱할 수 없습니다. 기존 고정 기간을 사용합니다.")
                        # 동적 파라미터가 존재한다면 고정값으로 설정
                        for param_meta in config["params"]:
                            if param_meta['is_dynamic']:
                                params_for_call[param_meta['param_key']] = param_meta['param_value']
                else:
                    # 옵션이 아닌 다른 종목이거나, 다른 TR_ID인 경우 고정 파라미터 사용
                    for param_meta in config["params"]:
                        if param_meta['is_dynamic']:
                            params_for_call[param_meta['param_key']] = param_meta['param_value']

                print(f"  --- 종목: {cur_kor_name} ({cur_code}, 시장 구분: {cur_mrkt_div}) (작업: {config['job_name']}) 데이터 처리 중 ---")
                print(f"  --- 조회 기간: {params_for_call.get('FID_INPUT_DATE_1')} ~ {params_for_call.get('FID_INPUT_DATE_2')} ---")

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

            fixed_params = {}
            dynamic_param_key = None
            dynamic_param_values = [None]

            for param_info in config["params"]:
                param_key = param_info['param_key']
                param_value = param_info['param_value']
                param_type = param_info['param_type']
                is_dynamic = param_info['is_dynamic']

                if is_dynamic == 1:
                    dynamic_param_key = param_key
                    try:
                        parsed_value = json.loads(param_value)
                        if isinstance(parsed_value, list):
                            dynamic_param_values = parsed_value
                        else:
                            dynamic_param_values = [param_value]
                    except json.JSONDecodeError:
                        dynamic_param_values = [param_value]
                else:
                    fixed_params[param_key] = param_value

            for dynamic_value in dynamic_param_values:
                params_for_call = fixed_params.copy()
                if dynamic_param_key:
                    params_for_call[dynamic_param_key] = dynamic_value

                print(f"  --- 전체 시장 현황 (작업: {config['job_name']}) 데이터 처리 중 'BOARD_ALL' 유형 (TR_ID: {tr_id})  ---")
                if dynamic_param_key:
                    print(f"    동적 파라미터 '{dynamic_param_key}': '{dynamic_value}' 적용")

                data = self.kis_client._call_api(base_url, endpoint, tr_id, params_for_call)

                if data:
                    print(f"      [API 응답]: {data.get('msg1') if 'msg1' in data else '데이터 수신 완료'}")
                    rowid = self.db_manager.insert(tr_id, 'ALL', params_for_call, data)
                    print(f"      데이터베이스에 rowid = {rowid}의 데이터가 적재되었습니다.")
                else:
                    print(f"      API 호출 실패: 일반 'BOARD_ALL' 유형 (작업: {config['job_name']}) 데이터를 적재하지 않습니다.")

                print(f"  --- 전체 시장 현황 (작업: {config['job_name']}) 데이터 처리 완료 ---\n")

            print(f"\n============================================================")
            print(f"=== API Job '{config['job_name']}' 처리 완료 ===")
            print("============================================================\n")

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