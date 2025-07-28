import time
import datetime
import sys
from core.api_client import KisClient
from core.db_manager import DatabaseManager
from core.process import MarketDataManager

kis_client = KisClient()
db = DatabaseManager()
manager = MarketDataManager(kis_client, db)

INTERVAL_SECONDS = 600
INTERVAL_MINUTES = INTERVAL_SECONDS // 60


def is_market_open():

    now = datetime.datetime.now()
    if now.weekday() >= 5:  # 월요일은 0, 일요일은 6
        return False

    market_open_time = now.replace(hour=8, minute=30, second=0, microsecond=0)
    market_close_time = now.replace(hour=15, minute=45, second=0, microsecond=0)

    return market_open_time <= now < market_close_time


def get_next_scheduled_time(current_time, interval_minutes):
    """
    현재 시간을 기준으로 다음 정각(interval_minutes 단위)을 계산합니다.
    예: current_time 08:32, interval_minutes 10 -> 08:40:00
    예: current_time 08:30, interval_minutes 10 -> 08:40:00 (다음 실행 시간)
    """
    next_minute_target = (current_time.minute // interval_minutes) * interval_minutes + interval_minutes

    next_hour = current_time.hour
    if next_minute_target >= 60:
        next_hour += (next_minute_target // 60)
        next_minute_target %= 60

    next_time = current_time.replace(hour=next_hour, minute=next_minute_target, second=0, microsecond=0)

    while next_time <= current_time:
        next_time += datetime.timedelta(minutes=interval_minutes)

    return next_time


def main():
    print("데이터 수집 스케줄러를 시작합니다. (정확히 매 10분 간격)")
    print("종료하려면 Ctrl+C를 누르세요.")

    initial_now = datetime.datetime.now()
    next_run_time = get_next_scheduled_time(initial_now, INTERVAL_MINUTES)

    market_open_dt_today = initial_now.replace(hour=8, minute=30, second=0, microsecond=0)

    if next_run_time < market_open_dt_today:
        next_run_time = market_open_dt_today

    print(f"초기 목표 실행 시간: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        now = datetime.datetime.now()

        if not is_market_open():
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 시장 마감 또는 주말입니다. 스케줄러를 종료합니다.")
            sys.exit(0)  # 프로그램 종료

        while now >= next_run_time:
            next_run_time += datetime.timedelta(minutes=INTERVAL_MINUTES)

            market_close_time_obj = datetime.time(15, 45, 0)  # is_market_open과 동일한 마감 시간 사용
            market_close_dt_today = now.replace(hour=market_close_time_obj.hour,
                                                minute=market_close_time_obj.minute,
                                                second=0, microsecond=0)

            if next_run_time >= market_close_dt_today:
                print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 다음 목표 실행 시간이 시장 마감 이후입니다. 더 이상 스케줄링하지 않습니다.")
                break  # 이 내부 while 루프를 벗어납니다.

            print(
                f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 스케줄 지연 감지. 다음 목표 실행 시간 조정: {next_run_time.strftime('%H:%M:%S')}")

        sleep_duration = (next_run_time - now).total_seconds()

        if sleep_duration > 0:
            print(
                f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 다음 실행까지 {sleep_duration:.1f}초 ({sleep_duration / 60:.1f}분) 대기...")
            time.sleep(sleep_duration)

        actual_run_time = datetime.datetime.now()
        print(f"[{actual_run_time.strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 시작...")

        try:
            manager.per_symbol_jobs()
            manager.board_all_jobs()
            manager.transform_data()

            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 완료.")
        except Exception as e:
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 중 오류 발생: {e}")

        next_run_time += datetime.timedelta(minutes=INTERVAL_MINUTES)


if __name__ == "__main__":
    main()
