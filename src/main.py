import time
import datetime
import sys
import pytz # pytz 임포트 추가

# 유틸리티 함수들을 임포트합니다.
from core.utils import is_market_open, get_next_scheduled_time, get_kst_market_boundaries_for_today # get_kst_market_boundaries_for_today 추가
# 초기화 함수를 임포트합니다.
from core.auth import initialize_application

# 데이터 수집 간격 설정 (10분)
INTERVAL_SECONDS = 600
INTERVAL_MINUTES = INTERVAL_SECONDS // 60

# KST 시간대 정의 (main.py에서도 필요할 수 있음)
KST = pytz.timezone('Asia/Seoul')

def main():
    print("데이터 수집 스케줄러를 시작합니다. (10분 간격)")
    print("종료하려면 Ctrl+C를 누르세요.")

    # 애플리케이션의 핵심 구성 요소들을 초기화합니다.
    kis_client, db, manager = initialize_application()

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 스케줄러 시작 시 초기 데이터 수집 시작...")
    try:
        manager.per_symbol_jobs()
        manager.board_all_jobs()
        # manager.transform_data()
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 초기 데이터 수집 완료.")
    except Exception as e:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 초기 데이터 수집 중 오류 발생: {e}")

    initial_now_utc = datetime.datetime.utcnow() # UTC 시간으로 초기화

    # 다음 목표 실행 시간 계산 (UTC 기준)
    next_run_time_utc = get_next_scheduled_time(initial_now_utc, INTERVAL_MINUTES)

    # KST 시장 개장 시간 (08:30)을 UTC로 변환하여 비교
    market_open_kst, market_close_kst = get_kst_market_boundaries_for_today(initial_now_utc)
    market_open_utc = market_open_kst.astimezone(pytz.utc)

    # 만약 계산된 첫 실행 시간(UTC)이 시장 개장 시간(UTC)보다 이르다면, 시장 개장 시간(UTC)으로 조정합니다.
    if next_run_time_utc < market_open_utc:
        next_run_time_utc = market_open_utc

    print(f"다음 스케줄링 목표 실행 시간 (UTC): {next_run_time_utc.strftime('%Y-%m-%d %H:%M:%S')}")

    # 메인 스케줄링 루프
    while True:
        now_utc = datetime.datetime.now(pytz.utc)
        current_kst_datetime = now_utc.astimezone(KST) # 현재 KST 시간

        # 1. 시장 개장 여부 확인 및 종료 조건
        # is_market_open 함수에 현재 UTC 시간을 전달하여 KST 시장 개장 여부 확인
        if not is_market_open(now_utc):
            print(f"[{current_kst_datetime.strftime('%Y-%m-%d %H:%M:%S')} KST] 시장 마감 또는 주말입니다. 스케줄러를 종료합니다.")
            # if db: db.close() # 프로그램 종료 전 DB 연결을 안전하게 닫습니다.
            sys.exit(0)  # 프로그램 종료

        # 2. 다음 실행 시간까지 대기 또는 스케줄 조정
        # 현재 UTC 시간이 다음 목표 실행 시간(UTC)을 넘어섰다면 스케줄 재정렬
        while now_utc >= next_run_time_utc:
            next_run_time_utc += datetime.timedelta(minutes=INTERVAL_MINUTES)

            # KST 시장 마감 시간을 UTC로 변환하여 비교
            # 매 루프마다 현재 now_utc를 기준으로 market_close_kst를 다시 계산하여 날짜 경계 문제 방지
            _, market_close_kst = get_kst_market_boundaries_for_today(now_utc)
            market_close_utc = market_close_kst.astimezone(pytz.utc)

            # 만약 다음 목표 시간(UTC)이 시장 마감 시간(UTC)과 같거나 그 이후라면, 더 이상 스케줄을 진행하지 않습니다.
            if next_run_time_utc >= market_close_utc:
                print(f"[{current_kst_datetime.strftime('%Y-%m-%d %H:%M:%S')} KST] 다음 목표 실행 시간이 시장 마감 이후입니다. 더 이상 스케줄링하지 않습니다.")
                break # 이 내부 while 루프를 벗어납니다.

            print(f"[{current_kst_datetime.strftime('%Y-%m-%d %H:%M:%S')} KST] 스케줄 지연 감지. 다음 목표 실행 시간 조정 (UTC): {next_run_time_utc.strftime('%H:%M:%S')}")

        # 계산된 대기 시간만큼 sleep 합니다. (UTC 시간 기준으로 계산)
        sleep_duration = (next_run_time_utc - now_utc).total_seconds()

        if sleep_duration > 0:
            print(
                f"[{current_kst_datetime.strftime('%Y-%m-%d %H:%M:%S')} KST] 다음 실행까지 {sleep_duration:.1f}초 ({sleep_duration / 60:.1f}분) 대기...")
            time.sleep(sleep_duration)

        # 3. 작업 실행 (정확히 목표 시간에 도달했으므로)
        actual_run_time_utc = datetime.datetime.utcnow()
        actual_run_time_kst = actual_run_time_utc.astimezone(KST)
        print(f"[{actual_run_time_kst.strftime('%Y-%m-%d %H:%M:%S')} KST] 데이터 수집 시작...")

        try:
            manager.per_symbol_jobs()
            manager.board_all_jobs()
            # manager.transform_data()

            print(f"[{actual_run_time_kst.strftime('%Y-%m-%d %H:%M:%S')} KST] 데이터 수집 완료.")
        except Exception as e:
            print(f"[{actual_run_time_kst.strftime('%Y-%m-%d %H:%M:%S')} KST] 데이터 수집 중 오류 발생: {e}")

        # 다음 목표 실행 시간은 내부 while 루프에서 이미 조정되었거나,
        # 다음 외부 루프 반복 시 `now_utc >= next_run_time_utc` 조건에 따라 다시 계산됩니다.
        # 따라서 여기에 추가적인 `next_run_time_utc += ...`는 필요하지 않습니다.


if __name__ == "__main__":
    main()
