from core.api_client import KisClient
from core.db_manager import DatabaseManager
from core.process import MarketDataManager

kis_client = KisClient()
db = DatabaseManager()

# 실행 객체
manager = MarketDataManager(kis_client, db)

if __name__ == "__main__":

# 종목코드별 실행
    # 1 : 선물옵션 시세
    # 2 : 선물옵션기간별시세(일/주/월/년)
    # 3 : 선물옵션 분봉조회
    # 4 : 국내선물 기초자산 시세
    manager.per_symbol_jobs()

# 종목코드 없이 전체 대상 실행
    # 5 : 옵션 전광판
  #   manager.board_all_jobs()

# 데이터 가공
    # manager.transform_data()