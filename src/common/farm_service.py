"""
서비스 대상 농장 조회 유틸리티

농장 목록 조회 SQL을 한 곳에서 관리하여 일관성을 보장합니다.
ProductivityCollector, WeatherCollector, orchestrator 등에서 공통으로 사용합니다.
"""
from typing import List, Dict, Optional


# 서비스 대상 농장 조회 SQL (공통)
# TS_INS_SERVICE 필터링 조건:
# - INSPIG_YN = 'Y': 인사이트피그 서비스 사용
# - INSPIG_FROM_DT <= SYSDATE: 서비스 시작일 이후
# - SYSDATE <= LEAST(INSPIG_TO_DT, INSPIG_STOP_DT): 종료일/중지일 중 빠른 날짜 이전
# - INSPIG_STOP_DT 기본값: 9999-12-31 (NULL이면 중지 안됨)
# 주의: DB가 UTC이므로 SYSDATE + 9/24로 KST 변환
SERVICE_FARM_SQL = """
    SELECT DISTINCT F.FARM_NO, F.FARM_NM, F.PRINCIPAL_NM, F.SIGUN_CD,
           NVL(F.COUNTRY_CODE, 'KOR') AS LOCALE
    FROM TA_FARM F
    INNER JOIN TS_INS_SERVICE S ON F.FARM_NO = S.FARM_NO
    WHERE F.USE_YN = 'Y'
      AND S.INSPIG_YN = 'Y'
      AND S.USE_YN = 'Y'
      AND S.INSPIG_FROM_DT IS NOT NULL
      AND S.INSPIG_TO_DT IS NOT NULL
      AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') >= S.INSPIG_FROM_DT
      AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') <= LEAST(
          S.INSPIG_TO_DT,
          NVL(S.INSPIG_STOP_DT, '99991231')
      )
    ORDER BY F.FARM_NO
"""

# 농장번호만 조회하는 SQL (ProductivityCollector 등에서 사용)
# 주의: DB가 UTC이므로 SYSDATE + 9/24로 KST 변환
SERVICE_FARM_NO_SQL = """
    SELECT DISTINCT F.FARM_NO
    FROM TA_FARM F
    INNER JOIN TS_INS_SERVICE S ON F.FARM_NO = S.FARM_NO
    WHERE F.USE_YN = 'Y'
      AND S.INSPIG_YN = 'Y'
      AND S.USE_YN = 'Y'
      AND S.INSPIG_FROM_DT IS NOT NULL
      AND S.INSPIG_TO_DT IS NOT NULL
      AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') >= S.INSPIG_FROM_DT
      AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') <= LEAST(
          S.INSPIG_TO_DT,
          NVL(S.INSPIG_STOP_DT, '99991231')
      )
    ORDER BY F.FARM_NO
"""


def get_service_farms(db, farm_list: Optional[str] = None) -> List[Dict]:
    """서비스 대상 농장 목록 조회 (상세 정보 포함)

    Args:
        db: Database 인스턴스
        farm_list: 특정 농장만 필터링 (콤마 구분, 예: "1387,2807")

    Returns:
        농장 정보 리스트
        [{'FARM_NO': 1387, 'FARM_NM': '바른양돈', 'LOCALE': 'KOR', ...}, ...]
    """
    sql = SERVICE_FARM_SQL

    if farm_list:
        farm_nos = [int(f.strip()) for f in farm_list.split(',') if f.strip()]
        placeholders = ', '.join([f':f{i}' for i in range(len(farm_nos))])
        sql = sql.replace('ORDER BY F.FARM_NO', f'AND F.FARM_NO IN ({placeholders})\n    ORDER BY F.FARM_NO')
        params = {f'f{i}': f for i, f in enumerate(farm_nos)}
        return db.fetch_dict(sql, params)

    return db.fetch_dict(sql)


def get_service_farm_nos(db) -> List[Dict]:
    """서비스 대상 농장번호 목록 조회 (농장번호만)

    Args:
        db: Database 인스턴스

    Returns:
        농장번호 리스트
        [{'FARM_NO': 1387}, {'FARM_NO': 2807}, ...]
    """
    return db.fetch_dict(SERVICE_FARM_NO_SQL)
