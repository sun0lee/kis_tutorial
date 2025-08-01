import datetime


def is_market_open() -> bool:
    """
    현재 시간이 시장 개장 시간(평일 08:30 ~ 15:45) 내에 있는지 확인합니다.
    """
    now = datetime.datetime.now()

    # 주말 (토요일: 5, 일요일: 6) 확인
    if now.weekday() >= 5:
        return False

    # 시장 개장 시간 (08:30) 및 마감 시간 (15:45) 설정
    market_open_time = now.replace(hour=8, minute=30, second=0, microsecond=0)
    market_close_time = now.replace(hour=15, minute=45, second=0, microsecond=0)

    # 현재 시간이 개장 시간과 마감 시간 사이에 있는지 확인
    return market_open_time <= now < market_close_time


def get_next_scheduled_time(current_time: datetime.datetime, interval_minutes: int) -> datetime.datetime:
    """
    현재 시간을 기준으로 다음 정각(interval_minutes 단위)을 계산합니다.
    예: current_time 08:32, interval_minutes 10 -> 08:40:00
    예: current_time 08:30, interval_minutes 10 -> 08:40:00 (다음 실행 시간)
    """
    # 다음 정각 분 계산
    next_minute_target = (current_time.minute // interval_minutes) * interval_minutes + interval_minutes

    next_hour = current_time.hour
    if next_minute_target >= 60:
        next_hour += (next_minute_target // 60)
        next_minute_target %= 60

    # 다음 목표 시간 설정
    next_time = current_time.replace(hour=next_hour, minute=next_minute_target, second=0, microsecond=0)

    # 만약 계산된 다음 시간이 현재 시간보다 같거나 이전이라면, interval_minutes를 더하여 미래 시간으로 만듭니다.
    # 이는 특히 current_time이 정확히 정각일 경우 (예: 08:30) 다음 실행 시간을 09:40으로 만드는 것을 방지합니다.
    while next_time <= current_time:
        next_time += datetime.timedelta(minutes=interval_minutes)

    return next_time
