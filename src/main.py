import json
from core.api_client import KisClient
from core.db_manager import DatabaseManager
from core.process import MarketDataManager
from core import API_CONFIG_PATH

kis_client = KisClient()
db = DatabaseManager()

# 종목코드
code_list = ["101K09", "101K12"]

# api 설정 : json file에서 관리 : config > api_configs.json
with open(API_CONFIG_PATH, 'r', encoding='utf-8') as f:
    api_configs = json.load(f)

# 실행 객체
manager = MarketDataManager(kis_client, db, api_configs, code_list)


if __name__ == "__main__":
    manager.save_data(job_list=["2"])

