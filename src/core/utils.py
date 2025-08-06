import datetime
import pytz

# KST 시간대 정의
KST = pytz.timezone('Asia/Seoul')

def get_kst_market_boundaries_for_today(current_utc_datetime: datetime.datetime):
    """
    주어진 UTC datetime 객체를 기준으로 한국 시장의 개장 및 마감 시간을
    KST datetime 객체로 반환합니다.
    이 함수는 KST 시간대를 고려하여 정확한 날짜/시간을 계산합니다.

    Args:
        current_utc_datetime (datetime): PythonAnywhere 서버에서 얻은 현재 UTC datetime 객체.
                                         (예: datetime.now() 또는 datetime.utcnow())

    Returns:
        tuple[datetime, datetime]: KST 시장 개장 시간 datetime 객체, KST 시장 마감 시간 datetime 객체
    """
    # 1. 입력된 UTC datetime 객체를 KST로 변환하여 '오늘'의 KST 날짜를 얻습니다.
    #    만약 current_utc_datetime가 naive하다면 UTC로 간주하고 timezone-aware로 만듭니다.
    if current_utc_datetime.tzinfo is None:
        current_utc_datetime = current_utc_datetime.replace(tzinfo=pytz.utc)
    elif current_utc_datetime.tzinfo != pytz.utc:
        current_utc_datetime = current_utc_datetime.astimezone(pytz.utc)

    now_kst = current_utc_datetime.astimezone(KST)

    # 2. KST 기준 시장 개장 및 마감 시간 설정 (08:30 ~ 15:45 반영)
    market_open_hour_kst = 8
    market_open_minute_kst = 30
    market_close_hour_kst = 15
    market_close_minute_kst = 45

    # 3. 현재 KST 날짜에 시장 개장/마감 시간을 적용하여 datetime 객체 생성
    market_open_datetime_kst = now_kst.replace(hour=market_open_hour_kst, minute=market_open_minute_kst, second=0, microsecond=0)
    market_close_datetime_kst = now_kst.replace(hour=market_close_hour_kst, minute=market_close_minute_kst, second=0, microsecond=0)

    return market_open_datetime_kst, market_close_datetime_kst

def is_market_open(current_utc_datetime: datetime.datetime) -> bool:
    """
    주어진 UTC datetime을 기준으로 현재 KST 시장이 개장 시간(평일 08:30 ~ 15:45) 내에 있는지 확인합니다.
    """
    # 현재 UTC 시간을 KST로 변환
    if current_utc_datetime.tzinfo is None:
        current_utc_datetime = current_utc_datetime.replace(tzinfo=pytz.utc)
    current_kst_datetime = current_utc_datetime.astimezone(KST)

    # 주말 (토요일: 5, 일요일: 6) 확인
    if current_kst_datetime.weekday() >= 5:
        return False

    # KST 시장 개장 및 마감 시간 가져오기
    market_open_kst, market_close_kst = get_kst_market_boundaries_for_today(current_utc_datetime)

    # 현재 KST 시간이 개장 시간과 마감 시간 사이에 있는지 확인
    return market_open_kst <= current_kst_datetime < market_close_kst

def get_next_scheduled_time(current_utc_datetime: datetime.datetime, interval_minutes: int) -> datetime.datetime:
    """
    주어진 UTC datetime을 기준으로 다음 정각(interval_minutes 단위)의 UTC 시간을 계산합니다.
    이 함수는 KST 시장 개장/마감 시간을 직접 고려하지 않고, 순수하게 다음 스케줄링 포인트를 계산합니다.
    시장 시간 내에 있는지 여부는 is_market_open 함수로 별도 확인해야 합니다.

    Args:
        current_utc_datetime (datetime.datetime): 현재 UTC 시간.
        interval_minutes (int): 스케줄링 간격 (분).

    Returns:
        datetime.datetime: 다음 스케줄링 목표 UTC 시간.
    """
    # **수정된 부분:** 입력된 current_utc_datetime이 naive하다면 UTC로 간주하고 timezone-aware로 만듭니다.
    if current_utc_datetime.tzinfo is None:
        current_utc_datetime = current_utc_datetime.replace(tzinfo=pytz.utc)

    # 현재 시간의 분을 interval_minutes의 배수로 올림
    minutes_to_add = interval_minutes - (current_utc_datetime.minute % interval_minutes)
    if minutes_to_add == interval_minutes: # 이미 정각인 경우 (예: 08:30) 다음 간격으로
        minutes_to_add = 0

    # 현재 시간의 초와 마이크로초를 0으로 설정
    temp_time = current_utc_datetime.replace(second=0, microsecond=0)

    # 다음 스케줄 시간 계산
    next_time = temp_time + datetime.timedelta(minutes=minutes_to_add)

    # 만약 계산된 next_time이 current_utc_datetime보다 같거나 이전이라면, interval_minutes를 한 번 더 더합니다.
    # 이는 current_utc_datetime이 정확히 정각일 때 (예: 08:30) next_time이 08:30이 되는 것을 방지하고 08:40으로 만듭니다.
    while next_time <= current_utc_datetime:
        next_time += datetime.timedelta(minutes=interval_minutes)

    return next_time
