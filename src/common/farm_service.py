"""
서비스 대상 농장 조회 유틸리티

농장 목록 조회 SQL을 한 곳에서 관리하여 일관성을 보장합니다.
ProductivityCollector, WeatherCollector, orchestrator 등에서 공통으로 사용합니다.

VW_INS_SERVICE_ACTIVE View 사용:
- inspig, inspig-etl, pig3.1 공통 사용
- TS_INS_SERVICE에서 현재 유효한 최신 구독만 조회

REG_TYPE 필터링:
- 정기 배치: REG_TYPE='AUTO' (기본값)만 대상
- 수동 ETL: REG_TYPE 무관 (farm_list로 직접 지정)
"""
from typing import List, Dict, Optional


# 서비스 대상 농장 조회 SQL (정기 배치용)
# VW_INS_SERVICE_ACTIVE View 사용 + REG_TYPE='AUTO' 필터
# MANUAL 등록은 정기 배치 대상에서 제외
SERVICE_FARM_SQL = """
    SELECT DISTINCT F.FARM_NO, F.FARM_NM, F.PRINCIPAL_NM, F.SIGUN_CD,
           NVL(F.COUNTRY_CODE, 'KOR') AS LOCALE
    FROM TA_FARM F
    INNER JOIN VW_INS_SERVICE_ACTIVE S ON F.FARM_NO = S.FARM_NO
    WHERE F.USE_YN = 'Y'
      AND NVL(S.REG_TYPE, 'AUTO') = 'AUTO'
    ORDER BY F.FARM_NO
"""

# 농장번호만 조회하는 SQL (ProductivityCollector 등에서 사용)
# VW_INS_SERVICE_ACTIVE View 사용 + REG_TYPE='AUTO' 필터
SERVICE_FARM_NO_SQL = """
    SELECT DISTINCT F.FARM_NO
    FROM TA_FARM F
    INNER JOIN VW_INS_SERVICE_ACTIVE S ON F.FARM_NO = S.FARM_NO
    WHERE F.USE_YN = 'Y'
      AND NVL(S.REG_TYPE, 'AUTO') = 'AUTO'
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
