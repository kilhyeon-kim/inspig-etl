"""
한국 시간대(KST) 유틸리티

서버가 UTC 시간대로 운영되더라도 비즈니스 로직은 한국 시간 기준으로 처리.
- 비즈니스 로직: now_kst() 사용 (기준일 계산, TOKEN_EXPIRE_DT 등)
- 로그/측정: datetime.now() 그대로 사용 (UTC)
"""
from datetime import datetime, timezone, timedelta

# 한국 시간대 (UTC+9)
KST = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    """현재 한국 시간 반환

    Returns:
        datetime: 한국 시간대(KST) 기준 현재 시각
    """
    return datetime.now(KST)


def today_kst() -> str:
    """오늘 날짜 (한국 시간 기준) YYYYMMDD 형식

    Returns:
        str: YYYYMMDD 형식의 오늘 날짜
    """
    return now_kst().strftime('%Y%m%d')