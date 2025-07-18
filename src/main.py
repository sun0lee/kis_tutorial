import json
from core.api_client import KisClient
from core.db_manager import DatabaseManager
from core.process import MarketDataManager
from core import API_CONFIG_PATH

kis_client = KisClient()
db = DatabaseManager()

# 종목코드 : mst_inst 에서 use_yn ='Y' 인 대상 추출
inst_list = db.get_inst_list()

# api 설정 : json file에서 관리 : config > api_configs.json
with open(API_CONFIG_PATH, 'r', encoding='utf-8') as f:
    api_configs = json.load(f)

# 실행 객체
manager = MarketDataManager(kis_client, db, api_configs, inst_list)


if __name__ == "__main__":

    # 1 : 선물옵션 시세
    # 2 : 선물옵션기간별시세(일/주/월/년)
    # 3 : 선물옵션 분봉조회
    # 4 : 국내선물 기초자산 시세
    # 공백 : 1~4 실행

    manager.save_data()
    # manager.save_data(job_list=["1"])
    # manager.save_data(job_list=["2","3"])
    # manager.save_data(job_list=["4"])

    manager.transform_data()