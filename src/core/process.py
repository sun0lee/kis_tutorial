from typing import Optional, List, Dict, Any
from core.db_manager import DatabaseManager
from core.api_client import KisClient
import json
import datetime
import time
from dateutil.relativedelta import relativedelta

class MarketDataManager:
    API_CALL_INTERVAL = 0.4  # 신규 App Key(3 TPS)
    # API_CALL_INTERVAL = 0.06  # 기본(18 TPS)

    def __init__(self, kis_client: KisClient
                    , db_manager: DatabaseManager):
        self.kis_client = kis_client
        self.db_manager = db_manager

    # 처리 간격 조정 
    def throttle(self):
        now = time.time()
        if not hasattr(self, "last_call_time"):
            self.last_call_time = 0
        wait = self.API_CALL_INTERVAL - (now - self.last_call_time)
        if wait > 0:
            time.sleep(wait)
        self.last_call_time = time.time()

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

            # for inst in inst_list:
            for idx, inst in enumerate(inst_list, start=1):
                cur_code = inst['shrn_iscd']
                cur_mrkt_div = inst['mrkt_div']
                cur_kor_name = inst['kor_name']

                params_for_call = {
                    'FID_INPUT_ISCD': cur_code,
                    'FID_COND_MRKT_DIV_CODE': cur_mrkt_div,
                    **fixed_params,
                }

                if tr_id == 'FHKIF03020100':
                    try:
                        option_type = inst['info_type']

                        start_date = None
                        expiry_date = None

                        if option_type in ('5', '6'):  # 월물 옵션
                            # 종목명: "C 200703 197.5"
                            # 두 번째 요소가 만기 년월 정보
                            expiry_info = cur_kor_name.split(' ')[1]
                            year = int(expiry_info[:4])
                            month = int(expiry_info[4:])

                            # 만기일 계산 로직 (셋째 목요일)
                            first_day_of_month = datetime.date(year, month, 1)
                            # 목요일(3)을 찾기 위해 요일 차이 계산
                            days_to_thursday = (3 - first_day_of_month.weekday() + 7) % 7
                            expiry_date = first_day_of_month + datetime.timedelta(days=days_to_thursday + 14)

                            # 조회 시작일 계산 (2개월 전)
                            start_date = expiry_date - relativedelta(months=2)

                        elif option_type in ('L', 'M'):  # 위클리 옵션
                            # 종목명: "위클리 C 2406W3 357.5"
                            # 세 번째 요소가 만기 주차 정보
                            expiry_info = cur_kor_name.split(' ')[2]
                            year = int('20' + expiry_info[0:2])  # '24' -> 2024
                            month = int(expiry_info[2:4])  # '06' -> 6
                            week_number = int(expiry_info[5])  # 'W3' -> 3

                            # 만기 요일 설정
                            if '(월)' in cur_kor_name:
                                target_weekday = 0  # 월요일
                            elif '(목)' in cur_kor_name:
                                target_weekday = 3  # 목요일
                            else:
                                # 괄호 안의 요일 정보가 없는 경우 예외 처리
                                raise ValueError("종목명에 위클리 옵션 만기 요일 정보가 없습니다.")

                            # 해당 월의 n번째 만기 요일 계산
                            first_day_of_month = datetime.date(year, month, 1)
                            # 해당 월의 첫 번째 만기 요일을 찾음
                            days_to_target_day = (target_weekday - first_day_of_month.weekday() + 7) % 7
                            # n번째 만기일 계산
                            expiry_date = first_day_of_month + datetime.timedelta(
                                days=days_to_target_day + (week_number - 1) * 7)

                            # 조회 시작일 계산 (만기일 2주일 전)
                            start_date = expiry_date - relativedelta(weeks=2)

                        # 조회 기간 파라미터 설정
                        if start_date and expiry_date:
                            params_for_call['FID_INPUT_DATE_1'] = start_date.strftime('%Y%m%d')
                            params_for_call['FID_INPUT_DATE_2'] = expiry_date.strftime('%Y%m%d')
                        else:
                            raise ValueError("만기일 또는 시작일 계산 오류")

                    except (ValueError, IndexError):
                        print(f"  --- 경고: 종목명 '{cur_kor_name}'에서 만기일 정보를 파싱할 수 없거나, 유효하지 않은 옵션 타입입니다. 고정 기간을 사용합니다.")
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

                # time.sleep(self.API_CALL_INTERVAL)
                self.throttle()
                data = self.kis_client._call_api(base_url,endpoint,tr_id,params_for_call)
                time.sleep(0.1)

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

                self.throttle()
                data = self.kis_client._call_api(base_url, endpoint, tr_id, params_for_call)
                time.sleep(0.1)

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