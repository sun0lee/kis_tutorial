import datetime
import time
import sys

from core.utils import is_market_open, get_next_scheduled_time, data_collecting
from core.auth import initialize_application

INTERVAL_SECONDS = 600
INTERVAL_MINUTES = INTERVAL_SECONDS // 60

def main():
    print("데이터 수집 스케줄러를 시작합니다. (10분 간격)")
    print("종료하려면 Ctrl+C를 누르세요.")

    kis_client, db, manager = initialize_application()

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 스케줄러 시작 시 초기 데이터 수집 시작...")
    try:
        manager.per_symbol_jobs()
        manager.board_all_jobs()
        # manager.transform_data()

        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 초기 데이터 수집 완료.")
    except Exception as e:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 초기 데이터 수집 중 오류 발생: {e}")

    market_open_time_obj = datetime.time(8, 30, 0)
    market_close_time_obj = datetime.time(15, 45, 0)
    now = datetime.datetime.now()
    market_open_dt_today = now.replace(hour=market_open_time_obj.hour, minute=market_open_time_obj.minute, second=0, microsecond=0)
    market_close_dt_today = now.replace(hour=market_close_time_obj.hour, minute=market_close_time_obj.minute, second=0, microsecond=0)

    if now.weekday() < 5 and now < market_open_dt_today:
        sleep_duration = (market_open_dt_today - now).total_seconds()
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 시장 개장 시간인 08:30까지 대기합니다. 남은 시간: {sleep_duration:.1f}초")
        time.sleep(sleep_duration)

    elif now.weekday() >= 5 or now >= market_close_dt_today:
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 시장 마감 또는 주말입니다. 스케줄러를 종료합니다.")
        sys.exit(0)

    data_collecting(manager, INTERVAL_MINUTES, market_open_dt_today, market_close_dt_today)


if __name__ == "__main__":
    main()
