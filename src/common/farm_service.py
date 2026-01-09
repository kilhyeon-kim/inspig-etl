"""
서비스 대상 농장 조회 유틸리티

농장 목록 조회 SQL을 한 곳에서 관리하여 일관성을 보장합니다.
ProductivityCollector, WeatherCollector, orchestrator 등에서 공통으로 사용합니다.
"""
from typing import List, Dict, Optional


# 서비스 대상 농장 조회 SQL (공통)
# TS_INS_SERVICE 필터링 조건 (PK: FARM_NO + INSPIG_REG_DT - 이력 관리):
# - INSPIG_YN = 'Y': 인사이트피그 서비스 사용
# - INSPIG_FROM_DT <= SYSDATE: 서비스 시작일 이후
# - SYSDATE <= LEAST(INSPIG_TO_DT, INSPIG_STOP_DT): 종료일/중지일 중 빠른 날짜 이전
# - INSPIG_STOP_DT 기본값: 9999-12-31 (NULL이면 중지 안됨)
# - 같은 농장 중 유효한 최신 건(INSPIG_REG_DT 기준)만 조인
# 주의: DB가 UTC이므로 SYSDATE + 9/24로 KST 변환
SERVICE_FARM_SQL = """
    SELECT DISTINCT F.FARM_NO, F.FARM_NM, F.PRINCIPAL_NM, F.SIGUN_CD,
           NVL(F.COUNTRY_CODE, 'KOR') AS LOCALE
    FROM TA_FARM F
    INNER JOIN (
        SELECT S1.FARM_NO, S1.INSPIG_REG_DT
        FROM TS_INS_SERVICE S1
        WHERE S1.INSPIG_YN = 'Y'
          AND S1.USE_YN = 'Y'
          AND S1.INSPIG_FROM_DT IS NOT NULL
          AND S1.INSPIG_TO_DT IS NOT NULL
          AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') >= S1.INSPIG_FROM_DT
          AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') <= LEAST(
              S1.INSPIG_TO_DT,
              NVL(S1.INSPIG_STOP_DT, '99991231')
          )
          AND S1.INSPIG_REG_DT = (
              SELECT MAX(S2.INSPIG_REG_DT)
              FROM TS_INS_SERVICE S2
              WHERE S2.FARM_NO = S1.FARM_NO
                AND S2.INSPIG_YN = 'Y'
                AND S2.USE_YN = 'Y'
                AND S2.INSPIG_FROM_DT IS NOT NULL
                AND S2.INSPIG_TO_DT IS NOT NULL
                AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') >= S2.INSPIG_FROM_DT
                AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') <= LEAST(
                    S2.INSPIG_TO_DT,
                    NVL(S2.INSPIG_STOP_DT, '99991231')
                )
          )
    ) S ON F.FARM_NO = S.FARM_NO
    WHERE F.USE_YN = 'Y'
    ORDER BY F.FARM_NO
"""

# 농장번호만 조회하는 SQL (ProductivityCollector 등에서 사용)
# PK: FARM_NO + INSPIG_REG_DT (이력 관리) - 유효한 최신 건만 조인
# 주의: DB가 UTC이므로 SYSDATE + 9/24로 KST 변환
SERVICE_FARM_NO_SQL = """
    SELECT DISTINCT F.FARM_NO
    FROM TA_FARM F
    INNER JOIN (
        SELECT S1.FARM_NO, S1.INSPIG_REG_DT
        FROM TS_INS_SERVICE S1
        WHERE S1.INSPIG_YN = 'Y'
          AND S1.USE_YN = 'Y'
          AND S1.INSPIG_FROM_DT IS NOT NULL
          AND S1.INSPIG_TO_DT IS NOT NULL
          AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') >= S1.INSPIG_FROM_DT
          AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') <= LEAST(
              S1.INSPIG_TO_DT,
              NVL(S1.INSPIG_STOP_DT, '99991231')
          )
          AND S1.INSPIG_REG_DT = (
              SELECT MAX(S2.INSPIG_REG_DT)
              FROM TS_INS_SERVICE S2
              WHERE S2.FARM_NO = S1.FARM_NO
                AND S2.INSPIG_YN = 'Y'
                AND S2.USE_YN = 'Y'
                AND S2.INSPIG_FROM_DT IS NOT NULL
                AND S2.INSPIG_TO_DT IS NOT NULL
                AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') >= S2.INSPIG_FROM_DT
                AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') <= LEAST(
                    S2.INSPIG_TO_DT,
                    NVL(S2.INSPIG_STOP_DT, '99991231')
                )
          )
    ) S ON F.FARM_NO = S.FARM_NO
    WHERE F.USE_YN = 'Y'
    ORDER BY F.FARM_NO
"""


def get_service_farms(
    db,
    farm_list: Optional[str] = None,
    exclude_farms: Optional[str] = None,
) -> List[Dict]:
    """서비스 대상 농장 목록 조회 (상세 정보 포함)

    Args:
        db: Database 인스턴스
        farm_list: 특정 농장만 필터링 (콤마 구분, 예: "1387,2807")
        exclude_farms: 제외할 농장 목록 (콤마 구분, 예: "848,1234")
                      farm_list와 함께 사용 시 farm_list에서 exclude_farms 제외

    Returns:
        농장 정보 리스트
        [{'FARM_NO': 1387, 'FARM_NM': '바른양돈', 'LOCALE': 'KOR', ...}, ...]
    """
    sql = SERVICE_FARM_SQL
    params = {}

    # 포함 필터링 (IN)
    if farm_list:
        farm_nos = [int(f.strip()) for f in farm_list.split(',') if f.strip()]
        placeholders = ', '.join([f':f{i}' for i in range(len(farm_nos))])
        sql = sql.replace('ORDER BY F.FARM_NO', f'AND F.FARM_NO IN ({placeholders})\n    ORDER BY F.FARM_NO')
        params.update({f'f{i}': f for i, f in enumerate(farm_nos)})

    # 제외 필터링 (NOT IN)
    if exclude_farms:
        exclude_nos = [int(f.strip()) for f in exclude_farms.split(',') if f.strip()]
        exclude_placeholders = ', '.join([f':ex{i}' for i in range(len(exclude_nos))])
        sql = sql.replace('ORDER BY F.FARM_NO', f'AND F.FARM_NO NOT IN ({exclude_placeholders})\n    ORDER BY F.FARM_NO')
        params.update({f'ex{i}': f for i, f in enumerate(exclude_nos)})

    if params:
        return db.fetch_dict(sql, params)

    return db.fetch_dict(sql)


def get_service_farm_nos(
    db,
    exclude_farms: Optional[str] = None,
) -> List[Dict]:
    """서비스 대상 농장번호 목록 조회 (농장번호만)

    Args:
        db: Database 인스턴스
        exclude_farms: 제외할 농장 목록 (콤마 구분, 예: "848,1234")

    Returns:
        농장번호 리스트
        [{'FARM_NO': 1387}, {'FARM_NO': 2807}, ...]
    """
    sql = SERVICE_FARM_NO_SQL
    params = {}

    # 제외 필터링 (NOT IN)
    if exclude_farms:
        exclude_nos = [int(f.strip()) for f in exclude_farms.split(',') if f.strip()]
        exclude_placeholders = ', '.join([f':ex{i}' for i in range(len(exclude_nos))])
        sql = sql.replace('ORDER BY F.FARM_NO', f'AND F.FARM_NO NOT IN ({exclude_placeholders})\n    ORDER BY F.FARM_NO')
        params.update({f'ex{i}': f for i, f in enumerate(exclude_nos)})

    if params:
        return db.fetch_dict(sql, params)

    return db.fetch_dict(sql)
