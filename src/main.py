from core.api_client import KisClient
from core.db_manager import DatabaseManager
from core.process import MarketDataManager
import time
import datetime
import sys
kis_client = KisClient()
db = DatabaseManager()
manager = MarketDataManager(kis_client, db)

# 10분 = 600초
INTERVAL_SECONDS = 600


def is_market_open():
    """
    현재 시간이 KOSPI200 선물/옵션 정규 거래 시간 내인지 확인합니다.
    최종 거래일 여부도 고려해야 하지만, 여기서는 일반적인 시간만 반영합니다.
    """
    now = datetime.datetime.now()

    # 주말 (토요일=5, 일요일=6)에는 시장이 열리지 않습니다.
    if now.weekday() >= 5:  # Monday is 0, Sunday is 6
        return False

    # 정규 시장 시간: 08:45 ~ 15:45
    # 동시호가 시간 포함하여 08:30 ~ 15:45 사이에만 데이터 수집 (안전하게)
    market_open_time = now.replace(hour=8, minute=30, second=0, microsecond=0)
    market_close_time = now.replace(hour=15, minute=45, second=0, microsecond=0)

    # 최종 거래일 (만기일)에는 15:20에 마감
    # TODO: 실제 최종 거래일인지 여부를 판단하는 로직 추가 필요 (예: DB에서 만기일 조회)
    # 현재는 간단화를 위해 일반적인 마감 시간만 사용
    # if is_expiration_day():
    #     market_close_time = now.replace(hour=15, minute=20, second=0, microsecond=0)

    return market_open_time <= now <= market_close_time


def main():
    print("데이터 수집 스케줄러를 시작합니다. (10분 간격)")
    print("종료하려면 Ctrl+C를 누르세요.")


    while True:
        if is_market_open():
            print(f"[{datetime.datetime.now()}] 시장 개장 중, 데이터 수집 시작...")
            try:
                # 종목코드별 실행
                # 1 : 선물옵션 시세
                manager.per_symbol_jobs()

                # 종목코드 없이 전체 대상 실행
                # 5 : 옵션 전광판
                manager.board_all_jobs()

                # 데이터 가공
                manager.transform_data()

                print(f"[{datetime.datetime.now()}] 데이터 수집 완료.")
            except Exception as e:
                print(f"[{datetime.datetime.now()}] 데이터 수집 중 오류 발생: {e}")
        else:
            print(f"[{datetime.datetime.now()}] 시장 마감 또는 주말입니다. 종료합니다.")
            sys.exit(0)  # 프로그램 종료

        print(f"다음 확인까지 {INTERVAL_SECONDS / 60}분 대기...")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":

    main()
