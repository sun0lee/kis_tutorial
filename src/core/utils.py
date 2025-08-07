import datetime
import time
import sys


def is_market_open() -> bool:
    now = datetime.datetime.now()

    # 주말 (토요일: 5, 일요일: 6) 확인
    if now.weekday() >= 5:
        return False

    market_open_time = now.replace(hour=8, minute=30, second=0, microsecond=0)
    market_close_time = now.replace(hour=15, minute=45, second=0, microsecond=0)

    return market_open_time <= now < market_close_time


def get_next_scheduled_time(current_time: datetime.datetime, interval_minutes: int) -> datetime.datetime:
    next_minute_target = (current_time.minute // interval_minutes) * interval_minutes + interval_minutes

    next_hour = current_time.hour
    if next_minute_target >= 60:
        next_hour += (next_minute_target // 60)
        next_minute_target %= 60

    next_time = current_time.replace(hour=next_hour, minute=next_minute_target, second=0, microsecond=0)

    while next_time <= current_time:
        next_time += datetime.timedelta(minutes=interval_minutes)

    return next_time


def data_collecting(manager, interval_minutes: int):
    market_close_time_obj = datetime.time(15, 45, 0)
    market_close_dt_today = datetime.datetime.now().replace(hour=market_close_time_obj.hour,
                                                            minute=market_close_time_obj.minute, second=0,
                                                            microsecond=0)

    initial_now = datetime.datetime.now()
    next_run_time = get_next_scheduled_time(initial_now, interval_minutes)

    market_open_dt_today = initial_now.replace(hour=8, minute=30, second=0, microsecond=0)

    if next_run_time < market_open_dt_today:
        next_run_time = market_open_dt_today

    print(f"초기 목표 실행 시간: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        now = datetime.datetime.now()

        if now.weekday() >= 5 or now.time() >= market_close_time_obj:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 시장 마감 또는 주말입니다. 스케줄러를 종료합니다.")
            sys.exit(0)

        while now >= next_run_time:
            next_run_time += datetime.timedelta(minutes=interval_minutes)

            if next_run_time >= market_close_dt_today:
                print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 다음 목표 실행 시간이 시장 마감 이후입니다. 더 이상 스케줄링하지 않습니다.")
                sys.exit(0)

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
