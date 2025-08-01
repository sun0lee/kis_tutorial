import time
import datetime
import sys

from core.utils import is_market_open, get_next_scheduled_time
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
        manager.transform_data()

        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 초기 데이터 수집 완료.")

    except Exception as e:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 초기 데이터 수집 중 오류 발생: {e}")


    initial_now = datetime.datetime.now()
    next_run_time = get_next_scheduled_time(initial_now, INTERVAL_MINUTES)

    market_open_dt_today = initial_now.replace(hour=8, minute=20, second=0, microsecond=0)

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

            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 완료.")
        except Exception as e:
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 중 오류 발생: {e}")


if __name__ == "__main__":
    main()
