"""
기상청 데이터 수집기
- 기상청 단기예보 API를 통한 날씨 데이터 수집 (예보)
- 초단기실황 API를 통한 실시간 관측 데이터 수집 (시간별 실측)
- ASOS 일자료 API를 통한 일별 실측 데이터 수집
- 격자(NX, NY) 기반 날씨 정보 조회 및 TM_WEATHER, TM_WEATHER_HOURLY 저장
- TS_API_KEY_INFO 테이블에서 API 키 관리 (호출 횟수 기반 로드밸런싱)

수집 전략:
  [1] TM_WEATHER (일별)
      - 단기예보(getVilageFcst): 오늘 ~ +3일 예보 (IS_FORECAST='Y')
      - ASOS 일자료: 어제까지 실측으로 덮어쓰기 (IS_FORECAST='N')

  [2] TM_WEATHER_HOURLY (시간별)
      - 초단기실황(getUltraSrtNcst): 현재 시각 실측 (IS_FORECAST='N')
      - 단기예보(getVilageFcst): 오늘 ~ +3일 예보 (IS_FORECAST='Y')

ASOS 관측소:
  - TM_WEATHER_ASOS 테이블에서 관측소 정보 조회
  - TA_FARM.ASOS_STN_ID 캐싱으로 중복 Haversine 계산 방지
"""
import logging
import math
import requests
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .base import BaseCollector
from ..common import Config, Database, now_kst, ApiKeyManager

logger = logging.getLogger(__name__)

# ASOS 관측소 캐시 (DB에서 로드)
_asos_stations_cache: List[Tuple[int, str, float, float]] = []

# ============================================================================
# 중기예보 지역코드 매핑
# ============================================================================
# 중기예보 API는 격자(NX, NY) 대신 지역코드(regId) 기반으로 조회
# - getMidTa (중기기온): 시/군 단위 코드 (예: 서울 11B10101)
# - getMidLandFcst (중기육상): 광역 단위 코드 (예: 서울/인천/경기 11B00000)
#
# 시군구코드(앞 2자리) → 중기예보 지역코드 매핑
# 참고: https://www.data.go.kr/data/15059468/openapi.do

# 중기기온예보 지역코드 (getMidTa)
# 시도코드(앞 2자리) → 대표 지역 regId
MID_TA_REG_IDS = {
    # 서울/경기/인천
    '11': '11B10101',  # 서울
    '41': '11B20601',  # 수원 (경기 대표)
    '28': '11B20201',  # 인천
    # 강원
    '42': '11D10301',  # 춘천 (강원영서 대표)
    '43': '11D10401',  # 원주 (강원영서)
    # 충청
    '44': '11C20401',  # 대전 (충남권 대표)
    '30': '11C20401',  # 대전광역시
    '36': '11C20401',  # 세종
    '45': '11C10301',  # 청주 (충북 대표)
    # 전라
    '46': '11F20501',  # 광주 (전남권 대표)
    '29': '11F20501',  # 광주광역시
    '47': '11F10201',  # 전주 (전북 대표)
    # 경상
    '48': '11H10701',  # 부산 (경남권 대표)
    '26': '11H10701',  # 부산광역시
    '31': '11H20101',  # 울산
    '49': '11H10501',  # 창원 (경남)
    '50': '11H20201',  # 대구 (경북권 대표)
    '27': '11H20201',  # 대구광역시
    # 제주
    '51': '11G00201',  # 제주
}

# 중기육상예보 지역코드 (getMidLandFcst)
# 시도코드(앞 2자리) → 광역 regId
MID_LAND_REG_IDS = {
    # 서울/경기/인천
    '11': '11B00000',  # 서울, 인천, 경기도
    '41': '11B00000',  # 경기도
    '28': '11B00000',  # 인천
    # 강원 (영서/영동 구분 필요 - 기본 영서)
    '42': '11D10000',  # 강원도영서
    '43': '11D10000',  # 강원도영서
    # 충청
    '44': '11C20000',  # 대전, 세종, 충청남도
    '30': '11C20000',  # 대전광역시
    '36': '11C20000',  # 세종
    '45': '11C10000',  # 충청북도
    # 전라
    '46': '11F20000',  # 광주, 전라남도
    '29': '11F20000',  # 광주광역시
    '47': '11F10000',  # 전북자치도
    # 경상
    '48': '11H10000',  # 부산, 울산, 경상남도
    '26': '11H10000',  # 부산광역시
    '31': '11H10000',  # 울산
    '49': '11H10000',  # 경남
    '50': '11H20000',  # 대구, 경상북도
    '27': '11H20000',  # 대구광역시
    # 제주
    '51': '11G00000',  # 제주도
}

# 기본값 (매핑되지 않는 지역)
DEFAULT_MID_TA_REG_ID = '11B10101'  # 서울
DEFAULT_MID_LAND_REG_ID = '11B00000'  # 서울/인천/경기


def get_mid_ta_reg_id(sigun_cd: str) -> str:
    """시군구코드에서 중기기온예보 지역코드 추출

    Args:
        sigun_cd: 시군구코드 (예: '4113510000')

    Returns:
        중기기온예보 regId (예: '11B20601')
    """
    if not sigun_cd or len(sigun_cd) < 2:
        return DEFAULT_MID_TA_REG_ID
    sido_cd = sigun_cd[:2]
    return MID_TA_REG_IDS.get(sido_cd, DEFAULT_MID_TA_REG_ID)


def get_mid_land_reg_id(sigun_cd: str) -> str:
    """시군구코드에서 중기육상예보 지역코드 추출

    Args:
        sigun_cd: 시군구코드 (예: '4113510000')

    Returns:
        중기육상예보 regId (예: '11B00000')
    """
    if not sigun_cd or len(sigun_cd) < 2:
        return DEFAULT_MID_LAND_REG_ID
    sido_cd = sigun_cd[:2]
    return MID_LAND_REG_IDS.get(sido_cd, DEFAULT_MID_LAND_REG_ID)


def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine 공식으로 두 좌표 간 거리 계산 (km)"""
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return 6371 * c  # 지구 반경 km


def load_asos_stations(db: Database) -> List[Tuple[int, str, float, float]]:
    """TM_WEATHER_ASOS 테이블에서 ASOS 관측소 정보 로드

    Args:
        db: Database 인스턴스

    Returns:
        [(STN_ID, STN_NM, LAT, LON), ...] 리스트
    """
    global _asos_stations_cache

    if _asos_stations_cache:
        return _asos_stations_cache

    sql = """
        SELECT STN_ID, STN_NM, LAT, LON
        FROM TM_WEATHER_ASOS
        WHERE USE_YN = 'Y'
        ORDER BY STN_ID
    """
    rows = db.fetch_dict(sql)

    _asos_stations_cache = [
        (int(row['STN_ID']), row['STN_NM'], float(row['LAT']), float(row['LON']))
        for row in rows
    ]

    logger.info(f"ASOS 관측소 로드: {len(_asos_stations_cache)}개")
    return _asos_stations_cache


def find_nearest_asos_station(lat: float, lon: float,
                              stations: Optional[List[Tuple[int, str, float, float]]] = None) -> Tuple[int, str, float]:
    """위경도에서 가장 가까운 ASOS 관측소 찾기

    Args:
        lat: 위도
        lon: 경도
        stations: ASOS 관측소 리스트 (None이면 캐시 사용)

    Returns:
        (관측소번호, 관측소명, 거리km) 튜플
    """
    if stations is None:
        stations = _asos_stations_cache

    if not stations:
        raise ValueError("ASOS 관측소 목록이 비어있습니다. load_asos_stations()를 먼저 호출하세요.")

    min_dist = float('inf')
    nearest = (stations[0][0], stations[0][1], 0.0)

    for stn_id, stn_name, stn_lat, stn_lon in stations:
        dist = _haversine_distance(lat, lon, stn_lat, stn_lon)

        if dist < min_dist:
            min_dist = dist
            nearest = (stn_id, stn_name, dist)

    return nearest


def update_farm_asos_mapping(db: Database) -> int:
    """TA_FARM의 ASOS_STN_ID 업데이트 (캐싱)

    ASOS_STN_ID가 없는 농장에 대해 가장 가까운 ASOS 관측소 매핑

    Args:
        db: Database 인스턴스

    Returns:
        업데이트된 농장 수
    """
    # ASOS 관측소 로드
    stations = load_asos_stations(db)
    if not stations:
        logger.warning("ASOS 관측소 정보가 없습니다.")
        return 0

    # ASOS_STN_ID가 없는 농장 조회
    sql = """
        SELECT FARM_NO, MAP_X_N AS LON, MAP_Y_N AS LAT
        FROM TA_FARM
        WHERE USE_YN = 'Y'
          AND MAP_X_N IS NOT NULL
          AND MAP_Y_N IS NOT NULL
          AND ASOS_STN_ID IS NULL
    """
    rows = db.fetch_dict(sql)

    if not rows:
        logger.info("ASOS 매핑이 필요한 농장 없음")
        return 0

    update_sql = """
        UPDATE TA_FARM
        SET ASOS_STN_ID = :ASOS_STN_ID,
            ASOS_STN_NM = :ASOS_STN_NM,
            ASOS_DIST_KM = :ASOS_DIST_KM,
            LOG_UPT_DT = SYSDATE
        WHERE FARM_NO = :FARM_NO
    """

    updates = []
    for row in rows:
        try:
            lat = float(row['LAT'])
            lon = float(row['LON'])
            stn_id, stn_nm, dist = find_nearest_asos_station(lat, lon, stations)

            updates.append({
                'FARM_NO': row['FARM_NO'],
                'ASOS_STN_ID': stn_id,
                'ASOS_STN_NM': stn_nm,
                'ASOS_DIST_KM': round(dist, 2),
            })
            logger.debug(f"농장 {row['FARM_NO']}: ASOS {stn_id} ({stn_nm}), {dist:.1f}km")
        except (ValueError, TypeError) as e:
            logger.warning(f"농장 {row['FARM_NO']} ASOS 매핑 실패: {e}")

    if updates:
        db.execute_many(update_sql, updates)
        db.commit()
        logger.info(f"TA_FARM ASOS 매핑 업데이트: {len(updates)}건")

    return len(updates)


def latlon_to_grid(lat: float, lon: float) -> Tuple[int, int]:
    """위경도를 기상청 격자 좌표로 변환 (Lambert Conformal Conic)

    Args:
        lat: 위도 (MAP_Y)
        lon: 경도 (MAP_X)

    Returns:
        (nx, ny) 격자 좌표 튜플
    """
    # 기상청 격자 변환 상수
    RE = 6371.00877    # 지구 반경(km)
    GRID = 5.0         # 격자 간격(km)
    SLAT1 = 30.0       # 투영 위도1(degree)
    SLAT2 = 60.0       # 투영 위도2(degree)
    OLON = 126.0       # 기준점 경도(degree)
    OLAT = 38.0        # 기준점 위도(degree)
    XO = 43            # 기준점 X좌표(GRID)
    YO = 136           # 기준점 Y좌표(GRID)

    DEGRAD = math.pi / 180.0

    re = RE / GRID
    slat1 = SLAT1 * DEGRAD
    slat2 = SLAT2 * DEGRAD
    olon = OLON * DEGRAD
    olat = OLAT * DEGRAD

    sn = math.tan(math.pi * 0.25 + slat2 * 0.5) / math.tan(math.pi * 0.25 + slat1 * 0.5)
    sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(sn)
    sf = math.tan(math.pi * 0.25 + slat1 * 0.5)
    sf = math.pow(sf, sn) * math.cos(slat1) / sn
    ro = math.tan(math.pi * 0.25 + olat * 0.5)
    ro = re * sf / math.pow(ro, sn)

    ra = math.tan(math.pi * 0.25 + lat * DEGRAD * 0.5)
    ra = re * sf / math.pow(ra, sn)
    theta = lon * DEGRAD - olon
    if theta > math.pi:
        theta -= 2.0 * math.pi
    if theta < -math.pi:
        theta += 2.0 * math.pi
    theta *= sn

    nx = int(ra * math.sin(theta) + XO + 0.5)
    ny = int(ro - ra * math.cos(theta) + YO + 0.5)

    return nx, ny


class WeatherCollector(BaseCollector):
    """기상청 날씨 데이터 수집기

    기상청 단기예보 API를 사용하여 격자별 날씨 데이터를 수집합니다.

    - TM_WEATHER: 일별 날씨 (NX, NY, WK_DATE 기준)
    - TM_WEATHER_HOURLY: 시간별 날씨 (NX, NY, WK_DATE, WK_TIME 기준)
    - TS_API_KEY_INFO: API 키 관리 (REQ_CNT 기반 로드밸런싱)

    API 문서:
    - https://www.data.go.kr/data/15084084/openapi.do (단기예보)

    수집 항목:
    - TMP: 기온
    - TMN: 최저기온
    - TMX: 최고기온
    - POP: 강수확률
    - PCP: 1시간 강수량
    - REH: 습도
    - WSD: 풍속
    - VEC: 풍향
    - SKY: 하늘상태 (1:맑음, 3:구름많음, 4:흐림)
    - PTY: 강수형태 (0:없음, 1:비, 2:비/눈, 3:눈, 4:소나기)
    """

    # 기상청 API 카테고리 코드
    CATEGORIES = {
        'TMP': '기온',
        'TMN': '최저기온',
        'TMX': '최고기온',
        'POP': '강수확률',
        'PCP': '1시간강수량',
        'REH': '습도',
        'WSD': '풍속',
        'VEC': '풍향',
        'SKY': '하늘상태',
        'PTY': '강수형태',
    }

    # 하늘상태 코드
    SKY_CODES = {
        '1': ('sunny', '맑음'),
        '3': ('cloudy', '구름많음'),
        '4': ('overcast', '흐림'),
    }

    # 강수형태 코드
    PTY_CODES = {
        '0': ('none', '없음'),
        '1': ('rainy', '비'),
        '2': ('rain_snow', '비/눈'),
        '3': ('snow', '눈'),
        '4': ('shower', '소나기'),
    }

    # 날씨 코드 → 이름 매핑 (역방향 조회용)
    WEATHER_NAMES = {
        'sunny': '맑음',
        'cloudy': '구름많음',
        'overcast': '흐림',
        'rainy': '비',
        'rain_snow': '비/눈',
        'snow': '눈',
        'shower': '소나기',
    }

    def __init__(self, config: Optional[Config] = None, db: Optional[Database] = None):
        super().__init__(config, db)
        self.weather_config = self.config.weather
        self.base_url = self.weather_config.get('base_url', 'https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0')
        self.asos_hourly_url = 'https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
        self.asos_daily_url = 'https://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'
        # 중기예보 API URL
        self.mid_fcst_url = 'https://apis.data.go.kr/1360000/MidFcstInfoService'

        # API 키 관리자
        self.key_manager = ApiKeyManager(self.db)

    def _get_ncst_base_datetime(self) -> Tuple[str, str]:
        """초단기실황 API 호출용 기준 날짜/시간 계산

        초단기실황: 매시 정각 발표, 40분 후 API 제공
        예: 12:00 발표 -> 12:40 이후 조회 가능

        Returns:
            (base_date, base_time) 튜플
        """
        now = now_kst()
        # 40분 이전 데이터 조회
        adjusted = now - timedelta(minutes=40)
        base_date = adjusted.strftime('%Y%m%d')
        base_time = f"{adjusted.hour:02d}00"
        return base_date, base_time

    def _fetch_ultra_srt_ncst(self, nx: int, ny: int, base_date: str, base_time: str) -> List[Dict]:
        """초단기실황 API 호출 (격자 기반 실측 데이터)

        Args:
            nx: 격자 X 좌표
            ny: 격자 Y 좌표
            base_date: 기준 날짜 (YYYYMMDD)
            base_time: 기준 시간 (HHMM)

        Returns:
            실황 데이터 리스트
        """
        url = f"{self.base_url}/getUltraSrtNcst"

        while self.key_manager.has_available_key():
            api_key = self.key_manager.get_current_key()
            if not api_key:
                break

            params = {
                'serviceKey': api_key,
                'pageNo': 1,
                'numOfRows': 100,
                'dataType': 'JSON',
                'base_date': base_date,
                'base_time': base_time,
                'nx': nx,
                'ny': ny,
            }

            try:
                self.logger.debug(f"초단기실황 API 호출: NX={nx}, NY={ny}, base={base_date} {base_time}")
                response = requests.get(url, params=params, timeout=30)

                # HTTP 에러 (401, 403, 429 등) - 다음 키로 재시도
                if response.status_code in (401, 403, 429):
                    self.logger.warning(f"초단기실황 API 키 인증/제한 오류 ({response.status_code}), 다음 키로 재시도")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                response.raise_for_status()

                data = response.json()
                result_code = data.get('response', {}).get('header', {}).get('resultCode')
                result_msg = data.get('response', {}).get('header', {}).get('resultMsg', '')

                if result_code == '00':
                    self.key_manager.increment_count(api_key)
                    items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
                    return items

                elif result_code in ApiKeyManager.LIMIT_ERROR_CODES:
                    self.logger.warning(f"초단기실황 API 호출 제한: {result_code} - {result_msg}")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                else:
                    self.logger.error(f"초단기실황 API 오류: {result_code} - {result_msg}")
                    return []

            except requests.RequestException as e:
                self.logger.error(f"초단기실황 API 호출 실패: {e}")
                return []
            except (KeyError, ValueError) as e:
                self.logger.error(f"초단기실황 API 응답 파싱 실패: {e}")
                return []

        self.logger.error("모든 API 키가 소진되었습니다. (초단기실황)")
        return []

    def _fetch_asos_hourly(self, stn_id: int, start_dt: str, start_hh: str,
                           end_dt: str, end_hh: str) -> List[Dict]:
        """ASOS 시간자료 API 호출 (관측소 기반 실측 데이터)

        Args:
            stn_id: ASOS 관측소 지점번호
            start_dt: 조회 시작일 (YYYYMMDD)
            start_hh: 조회 시작시 (HH)
            end_dt: 조회 종료일 (YYYYMMDD)
            end_hh: 조회 종료시 (HH)

        Returns:
            시간별 관측 데이터 리스트
        """
        while self.key_manager.has_available_key():
            api_key = self.key_manager.get_current_key()
            if not api_key:
                break

            params = {
                'serviceKey': api_key,
                'pageNo': 1,
                'numOfRows': 100,
                'dataType': 'JSON',
                'dataCd': 'ASOS',
                'dateCd': 'HR',
                'startDt': start_dt,
                'startHh': start_hh,
                'endDt': end_dt,
                'endHh': end_hh,
                'stnIds': stn_id,
            }

            try:
                self.logger.debug(f"ASOS 시간자료 API 호출: stnId={stn_id}, {start_dt} {start_hh}시 ~ {end_dt} {end_hh}시")
                response = requests.get(self.asos_hourly_url, params=params, timeout=30)

                # HTTP 에러 (401, 403, 429 등) - 다음 키로 재시도
                if response.status_code in (401, 403, 429):
                    self.logger.warning(f"ASOS 시간자료 API 키 인증/제한 오류 ({response.status_code}), 다음 키로 재시도")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                response.raise_for_status()

                data = response.json()
                result_code = data.get('response', {}).get('header', {}).get('resultCode')
                result_msg = data.get('response', {}).get('header', {}).get('resultMsg', '')

                if result_code == '00':
                    self.key_manager.increment_count(api_key)
                    items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
                    return items if isinstance(items, list) else [items] if items else []

                elif result_code in ApiKeyManager.LIMIT_ERROR_CODES:
                    self.logger.warning(f"ASOS 시간자료 API 호출 제한: {result_code} - {result_msg}")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                else:
                    self.logger.error(f"ASOS 시간자료 API 오류: {result_code} - {result_msg}")
                    return []

            except requests.RequestException as e:
                self.logger.error(f"ASOS 시간자료 API 호출 실패: {e}")
                return []
            except (KeyError, ValueError) as e:
                self.logger.error(f"ASOS 시간자료 API 응답 파싱 실패: {e}")
                return []

        self.logger.error("모든 API 키가 소진되었습니다. (ASOS 시간자료)")
        return []

    def _fetch_asos_daily(self, stn_id: int, start_dt: str, end_dt: str) -> List[Dict]:
        """ASOS 일자료 API 호출 (관측소 기반 일별 실측 데이터)

        Args:
            stn_id: ASOS 관측소 지점번호
            start_dt: 조회 시작일 (YYYYMMDD)
            end_dt: 조회 종료일 (YYYYMMDD)

        Returns:
            일별 관측 데이터 리스트
        """
        while self.key_manager.has_available_key():
            api_key = self.key_manager.get_current_key()
            if not api_key:
                break

            params = {
                'serviceKey': api_key,
                'pageNo': 1,
                'numOfRows': 100,
                'dataType': 'JSON',
                'dataCd': 'ASOS',
                'dateCd': 'DAY',
                'startDt': start_dt,
                'endDt': end_dt,
                'stnIds': stn_id,
            }

            try:
                self.logger.debug(f"ASOS 일자료 API 호출: stnId={stn_id}, {start_dt} ~ {end_dt}")
                response = requests.get(self.asos_daily_url, params=params, timeout=30)

                # HTTP 에러 (401, 403, 429 등) - 다음 키로 재시도
                if response.status_code in (401, 403, 429):
                    self.logger.warning(f"ASOS 일자료 API 키 인증/제한 오류 ({response.status_code}), 다음 키로 재시도")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                response.raise_for_status()

                data = response.json()
                result_code = data.get('response', {}).get('header', {}).get('resultCode')
                result_msg = data.get('response', {}).get('header', {}).get('resultMsg', '')

                if result_code == '00':
                    self.key_manager.increment_count(api_key)
                    items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
                    return items if isinstance(items, list) else [items] if items else []

                elif result_code in ApiKeyManager.LIMIT_ERROR_CODES:
                    self.logger.warning(f"ASOS 일자료 API 호출 제한: {result_code} - {result_msg}")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                else:
                    self.logger.error(f"ASOS 일자료 API 오류: {result_code} - {result_msg}")
                    return []

            except requests.RequestException as e:
                self.logger.error(f"ASOS 일자료 API 호출 실패: {e}")
                return []
            except (KeyError, ValueError) as e:
                self.logger.error(f"ASOS 일자료 API 응답 파싱 실패: {e}")
                return []

        self.logger.error("모든 API 키가 소진되었습니다. (ASOS 일자료)")
        return []

    def _get_base_datetime(self) -> Tuple[str, str]:
        """API 호출용 기준 날짜/시간 계산

        기상청 단기예보 API는 02:00부터 3시간 간격으로 발표
        발표시간: 02, 05, 08, 11, 14, 17, 20, 23시
        (발표 후 약 10분 뒤 데이터 생성)

        Returns:
            (base_date, base_time) 튜플
        """
        now = now_kst()
        announce_hours = [2, 5, 8, 11, 14, 17, 20, 23]

        # 현재 시간에서 10분 빼기 (발표 후 생성 시간 고려)
        adjusted = now - timedelta(minutes=10)
        current_hour = adjusted.hour

        # 가장 가까운 이전 발표 시간 찾기
        valid_hours = [h for h in announce_hours if h <= current_hour]

        if valid_hours:
            base_hour = max(valid_hours)
            base_date = adjusted.strftime('%Y%m%d')
        else:
            # 자정~02시 사이면 전날 23시
            base_hour = 23
            base_date = (adjusted - timedelta(days=1)).strftime('%Y%m%d')

        base_time = f"{base_hour:02d}00"

        return base_date, base_time

    def _fetch_forecast(self, nx: int, ny: int, base_date: str, base_time: str) -> List[Dict]:
        """기상청 단기예보 API 호출 (API 키 로테이션 지원)

        Args:
            nx: 격자 X 좌표
            ny: 격자 Y 좌표
            base_date: 기준 날짜 (YYYYMMDD)
            base_time: 기준 시간 (HHMM)

        Returns:
            예보 데이터 리스트
        """
        url = f"{self.base_url}/getVilageFcst"

        while self.key_manager.has_available_key():
            api_key = self.key_manager.get_current_key()
            if not api_key:
                break

            params = {
                'serviceKey': api_key,
                'pageNo': 1,
                'numOfRows': 1000,
                'dataType': 'JSON',
                'base_date': base_date,
                'base_time': base_time,
                'nx': nx,
                'ny': ny,
            }

            try:
                self.logger.debug(f"API 호출: NX={nx}, NY={ny}, base={base_date} {base_time}")
                response = requests.get(url, params=params, timeout=30)

                # HTTP 에러 (401, 403, 429 등) - 다음 키로 재시도
                if response.status_code in (401, 403, 429):
                    self.logger.warning(f"API 키 인증/제한 오류 ({response.status_code}), 다음 키로 재시도")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                response.raise_for_status()

                data = response.json()

                # 응답 코드 확인
                result_code = data.get('response', {}).get('header', {}).get('resultCode')
                result_msg = data.get('response', {}).get('header', {}).get('resultMsg', '')

                if result_code == '00':
                    # 성공 - REQ_CNT 증가
                    self.key_manager.increment_count(api_key)
                    items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
                    return items

                elif result_code in ApiKeyManager.LIMIT_ERROR_CODES:
                    # 호출 제한 - 다음 키로 재시도
                    self.logger.warning(f"API 호출 제한: {result_code} - {result_msg}")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                else:
                    # 기타 에러
                    self.logger.error(f"API 오류: {result_code} - {result_msg}")
                    return []

            except requests.RequestException as e:
                self.logger.error(f"기상청 API 호출 실패: {e}")
                return []
            except (KeyError, ValueError) as e:
                self.logger.error(f"기상청 API 응답 파싱 실패: {e}")
                return []

        self.logger.error("모든 API 키가 소진되었습니다. (401/403/429 또는 호출제한)")
        return []

    def _parse_forecast_items(self, items: List[Dict], nx: int, ny: int) -> Tuple[Dict, List[Dict]]:
        """예보 데이터 파싱하여 일별/시간별 데이터로 변환

        Args:
            items: API 응답 아이템 목록
            nx: 격자 X
            ny: 격자 Y

        Returns:
            (daily_data, hourly_data) 튜플
            - daily_data: {날짜: {필드: 값}} 형태
            - hourly_data: [{시간별 레코드}] 형태
        """
        daily_data = {}  # {날짜: {TMP_list, TMN, TMX, POP_max, ...}}
        hourly_data = []

        for item in items:
            fcst_date = item.get('fcstDate')
            fcst_time = item.get('fcstTime')
            category = item.get('category')
            value = item.get('fcstValue')

            if not all([fcst_date, fcst_time, category]):
                continue

            # 일별 데이터 집계
            if fcst_date not in daily_data:
                daily_data[fcst_date] = {
                    'WK_DATE': fcst_date,
                    'NX': nx,
                    'NY': ny,
                    'TMP_list': [],
                    'POP_max': 0,
                    'TMN': None,
                    'TMX': None,
                    'SKY_CD': None,
                    'PTY_CD': None,
                    'WEATHER_LIST': [],  # 시간별 날씨코드 목록 (대표 날씨 결정용)
                }

            day = daily_data[fcst_date]

            # 카테고리별 처리
            if category == 'TMP':
                try:
                    day['TMP_list'].append(float(value))
                except (ValueError, TypeError):
                    pass
            elif category == 'TMN':
                try:
                    day['TMN'] = float(value)
                except (ValueError, TypeError):
                    pass
            elif category == 'TMX':
                try:
                    day['TMX'] = float(value)
                except (ValueError, TypeError):
                    pass
            elif category == 'POP':
                try:
                    pop = int(value)
                    if pop > day['POP_max']:
                        day['POP_max'] = pop
                except (ValueError, TypeError):
                    pass
            elif category == 'SKY':
                if day['SKY_CD'] is None:
                    day['SKY_CD'] = value
            elif category == 'PTY':
                if day['PTY_CD'] is None or value != '0':
                    day['PTY_CD'] = value

            # 시간별 데이터
            existing = next((h for h in hourly_data if h['WK_DATE'] == fcst_date and h['WK_TIME'] == fcst_time), None)

            if existing is None:
                existing = {
                    'WK_DATE': fcst_date,
                    'WK_TIME': fcst_time,
                    'NX': nx,
                    'NY': ny,
                    'TEMP': None,
                    'RAIN_PROB': 0,
                    'RAIN_AMT': 0,
                    'HUMIDITY': None,
                    'WIND_SPEED': None,
                    'WIND_DIR': None,
                    'SKY_CD': None,
                    'PTY_CD': None,
                }
                hourly_data.append(existing)

            # 시간별 필드 업데이트
            if category == 'TMP':
                try:
                    existing['TEMP'] = float(value)
                except (ValueError, TypeError):
                    pass
            elif category == 'POP':
                try:
                    existing['RAIN_PROB'] = int(value)
                except (ValueError, TypeError):
                    pass
            elif category == 'PCP':
                try:
                    # "강수없음" 또는 숫자
                    if value not in ('강수없음', ''):
                        existing['RAIN_AMT'] = float(value.replace('mm', '').strip())
                except (ValueError, TypeError):
                    pass
            elif category == 'REH':
                try:
                    existing['HUMIDITY'] = int(value)
                except (ValueError, TypeError):
                    pass
            elif category == 'WSD':
                try:
                    existing['WIND_SPEED'] = float(value)
                except (ValueError, TypeError):
                    pass
            elif category == 'VEC':
                try:
                    existing['WIND_DIR'] = int(value)
                except (ValueError, TypeError):
                    pass
            elif category == 'SKY':
                existing['SKY_CD'] = value
            elif category == 'PTY':
                existing['PTY_CD'] = value

        # 시간별 데이터에서 날씨 코드 목록 수집 (일별 대표 날씨 결정용)
        for h in hourly_data:
            fcst_date = h.get('WK_DATE')
            if fcst_date not in daily_data:
                continue

            pty_cd = h.get('PTY_CD', '0')
            sky_cd = h.get('SKY_CD', '1')

            # PTY > SKY 우선순위로 날씨 코드 결정
            if pty_cd and pty_cd != '0':
                weather_cd, _ = self.PTY_CODES.get(pty_cd, ('unknown', '알수없음'))
            else:
                weather_cd, _ = self.SKY_CODES.get(sky_cd, ('unknown', '알수없음'))

            daily_data[fcst_date]['WEATHER_LIST'].append(weather_cd)

        return daily_data, hourly_data

    def _get_weather_name(self, weather_cd: str) -> str:
        """날씨 코드에서 이름 조회"""
        return self.WEATHER_NAMES.get(weather_cd, '알수없음')

    def _finalize_daily_data(self, daily_data: Dict) -> List[Dict]:
        """일별 데이터 최종 가공

        Args:
            daily_data: {날짜: {필드: 값}} 형태

        Returns:
            TM_WEATHER 레코드 리스트

        Note:
            API는 현재 시점 이후의 예보만 반환하므로,
            오늘 날짜의 경우 DB에 저장된 이전 시간대 데이터와 병합하여
            min/max를 정확하게 계산합니다.
        """
        result = []

        # 오늘 날짜 (KST)
        today_str = now_kst().strftime('%Y%m%d')

        for wk_date, day in daily_data.items():
            nx = day['NX']
            ny = day['NY']

            # API에서 받은 시간별 기온 목록
            # Note: 오늘 날짜의 min/max는 _save_daily_today_aggregated()에서
            # TM_WEATHER_HOURLY DB 집계로 계산 (저장 시점에 정확한 값 사용)
            temp_list = day.get('TMP_list', [])

            # 시간별 데이터가 2개 미만이면 불완전한 데이터로 간주하여 스킵
            # (단기예보 경계일에 1개 시간대만 있는 경우 min=max가 되어 부정확)
            if len(temp_list) < 2:
                self.logger.debug(f"단기예보 스킵: {wk_date} - 시간별 데이터 부족 ({len(temp_list)}개)")
                continue

            temp_avg = sum(temp_list) / len(temp_list) if temp_list else None

            # 최저/최고 기온: 시간별 예보(TMP_list)의 min/max 사용 (네이버 방식)
            # 기상청 TMN/TMX는 특정 시간대 기준(03~09시/09~18시)이라 전체 min/max와 다를 수 있음
            temp_high = max(temp_list) if temp_list else None
            temp_low = min(temp_list) if temp_list else None

            # 날씨 코드 결정: 시간별 데이터에서 가장 빈번한 날씨를 대표로 사용
            # (기존: 강수형태가 있으면 무조건 비/눈 → 1시간만 비여도 하루종일 비 표시)
            weather_list = day.get('WEATHER_LIST', [])
            sky_cd = day.get('SKY_CD', '1')

            if weather_list:
                # 빈도 계산
                weather_counts = Counter(weather_list)
                weather_cd = weather_counts.most_common(1)[0][0]
                weather_nm = self._get_weather_name(weather_cd)
            else:
                # fallback: 기존 방식
                pty_cd = day.get('PTY_CD', '0')
                if pty_cd and pty_cd != '0':
                    weather_cd, weather_nm = self.PTY_CODES.get(pty_cd, ('unknown', '알수없음'))
                else:
                    weather_cd, weather_nm = self.SKY_CODES.get(sky_cd, ('unknown', '알수없음'))

            result.append({
                'WK_DATE': wk_date,
                'NX': day['NX'],
                'NY': day['NY'],
                'TEMP_AVG': round(temp_avg, 1) if temp_avg else None,
                'TEMP_HIGH': temp_high,
                'TEMP_LOW': temp_low,
                'RAIN_PROB': day.get('POP_max', 0),
                'WEATHER_CD': weather_cd,
                'WEATHER_NM': weather_nm,
                'SKY_CD': sky_cd,
            })

        return result

    def _finalize_hourly_data(self, hourly_data: List[Dict]) -> List[Dict]:
        """시간별 데이터 최종 가공"""
        for h in hourly_data:
            pty_cd = h.get('PTY_CD', '0')
            sky_cd = h.get('SKY_CD', '1')

            if pty_cd and pty_cd != '0':
                weather_cd, weather_nm = self.PTY_CODES.get(pty_cd, ('unknown', '알수없음'))
            else:
                weather_cd, weather_nm = self.SKY_CODES.get(sky_cd, ('unknown', '알수없음'))

            h['WEATHER_CD'] = weather_cd
            h['WEATHER_NM'] = weather_nm

        return hourly_data

    def _parse_ncst_items(self, items: List[Dict], nx: int, ny: int,
                          base_date: str, base_time: str) -> Dict:
        """초단기실황 데이터 파싱 (시간별 실측)

        Args:
            items: API 응답 아이템 목록
            nx: 격자 X
            ny: 격자 Y
            base_date: 기준 날짜
            base_time: 기준 시간

        Returns:
            시간별 실측 레코드 (단일)
        """
        record = {
            'WK_DATE': base_date,
            'WK_TIME': base_time,
            'NX': nx,
            'NY': ny,
            'TEMP': None,
            'RAIN_PROB': None,  # 실황에는 강수확률 없음
            'RAIN_AMT': 0,
            'HUMIDITY': None,
            'WIND_SPEED': None,
            'WIND_DIR': None,
            'SKY_CD': None,
            'PTY_CD': None,
            'WEATHER_CD': 'unknown',
            'WEATHER_NM': '알수없음',
            'IS_FORECAST': 'N',  # 실측
        }

        for item in items:
            category = item.get('category')
            value = item.get('obsrValue')

            if category == 'T1H':  # 기온
                try:
                    record['TEMP'] = float(value)
                except (ValueError, TypeError):
                    pass
            elif category == 'RN1':  # 1시간 강수량
                try:
                    if value not in ('강수없음', ''):
                        record['RAIN_AMT'] = float(str(value).replace('mm', '').strip())
                except (ValueError, TypeError):
                    pass
            elif category == 'REH':  # 습도
                try:
                    record['HUMIDITY'] = int(value)
                except (ValueError, TypeError):
                    pass
            elif category == 'WSD':  # 풍속
                try:
                    record['WIND_SPEED'] = float(value)
                except (ValueError, TypeError):
                    pass
            elif category == 'VEC':  # 풍향
                try:
                    record['WIND_DIR'] = int(value)
                except (ValueError, TypeError):
                    pass
            elif category == 'SKY':  # 하늘상태
                record['SKY_CD'] = str(value)
            elif category == 'PTY':  # 강수형태
                record['PTY_CD'] = str(value)

        # 날씨 코드 결정
        # 초단기실황은 SKY(하늘상태) 없이 PTY(강수형태)만 제공
        # PTY=0(강수없음)이면 맑음으로 처리
        pty_cd = record.get('PTY_CD', '0') or '0'
        sky_cd = record.get('SKY_CD') or '1'  # NULL이면 맑음(1)

        if pty_cd != '0':
            record['WEATHER_CD'], record['WEATHER_NM'] = self.PTY_CODES.get(pty_cd, ('unknown', '알수없음'))
        else:
            record['WEATHER_CD'], record['WEATHER_NM'] = self.SKY_CODES.get(sky_cd, ('sunny', '맑음'))

        return record

    def _parse_asos_daily_items(self, items: List[Dict], nx: int, ny: int) -> List[Dict]:
        """ASOS 일자료 파싱 (일별 실측)

        Args:
            items: ASOS API 응답 아이템 목록
            nx: 격자 X (저장용)
            ny: 격자 Y (저장용)

        Returns:
            일별 실측 레코드 리스트
        """
        result = []

        for item in items:
            tm = item.get('tm', '')  # YYYY-MM-DD 형식
            wk_date = tm.replace('-', '') if tm else ''

            if not wk_date:
                continue

            # 날씨 코드 결정 (하늘상태/강수 정보 없으므로 기본값)
            weather_cd = 'sunny'
            weather_nm = '맑음'

            # 강수량이 있으면 비로 표시
            sum_rn = item.get('sumRn')
            if sum_rn and float(sum_rn) > 0:
                weather_cd = 'rainy'
                weather_nm = '비'

            record = {
                'WK_DATE': wk_date,
                'NX': nx,
                'NY': ny,
                'TEMP_AVG': None,
                'TEMP_HIGH': None,
                'TEMP_LOW': None,
                'RAIN_PROB': None,
                'RAIN_AMT': None,
                'HUMIDITY': None,
                'WIND_SPEED': None,
                'WEATHER_CD': weather_cd,
                'WEATHER_NM': weather_nm,
                'SKY_CD': '1',
                'IS_FORECAST': 'N',  # 실측
            }

            # 기온
            try:
                if item.get('avgTa'):
                    record['TEMP_AVG'] = round(float(item['avgTa']), 1)
            except (ValueError, TypeError):
                pass
            try:
                if item.get('maxTa'):
                    record['TEMP_HIGH'] = round(float(item['maxTa']), 1)
            except (ValueError, TypeError):
                pass
            try:
                if item.get('minTa'):
                    record['TEMP_LOW'] = round(float(item['minTa']), 1)
            except (ValueError, TypeError):
                pass

            # 강수량
            try:
                if item.get('sumRn'):
                    record['RAIN_AMT'] = round(float(item['sumRn']), 1)
            except (ValueError, TypeError):
                pass

            # 습도
            try:
                if item.get('avgRhm'):
                    record['HUMIDITY'] = int(float(item['avgRhm']))
            except (ValueError, TypeError):
                pass

            # 풍속
            try:
                if item.get('avgWs'):
                    record['WIND_SPEED'] = round(float(item['avgWs']), 1)
            except (ValueError, TypeError):
                pass

            result.append(record)

        return result

    def _parse_asos_hourly_items(self, items: List[Dict], nx: int, ny: int) -> List[Dict]:
        """ASOS 시간자료 파싱 (시간별 실측)

        Args:
            items: ASOS API 응답 아이템 목록
            nx: 격자 X (저장용)
            ny: 격자 Y (저장용)

        Returns:
            시간별 실측 레코드 리스트
        """
        result = []

        for item in items:
            tm = item.get('tm', '')  # YYYY-MM-DD HH:MM 형식
            if not tm:
                continue

            parts = tm.split(' ')
            if len(parts) < 2:
                continue

            wk_date = parts[0].replace('-', '')
            wk_time = parts[1].replace(':', '')[:4]  # HHMM

            record = {
                'WK_DATE': wk_date,
                'WK_TIME': wk_time,
                'NX': nx,
                'NY': ny,
                'TEMP': None,
                'RAIN_PROB': None,
                'RAIN_AMT': 0,
                'HUMIDITY': None,
                'WIND_SPEED': None,
                'WIND_DIR': None,
                'SKY_CD': None,
                'PTY_CD': None,
                'WEATHER_CD': 'sunny',
                'WEATHER_NM': '맑음',
                'IS_FORECAST': 'N',  # 실측
            }

            # 기온
            try:
                if item.get('ta'):
                    record['TEMP'] = float(item['ta'])
            except (ValueError, TypeError):
                pass

            # 강수량
            try:
                if item.get('rn') and item['rn'] != '':
                    rn = float(item['rn'])
                    record['RAIN_AMT'] = rn
                    if rn > 0:
                        record['WEATHER_CD'] = 'rainy'
                        record['WEATHER_NM'] = '비'
            except (ValueError, TypeError):
                pass

            # 습도
            try:
                if item.get('hm'):
                    record['HUMIDITY'] = int(float(item['hm']))
            except (ValueError, TypeError):
                pass

            # 풍속
            try:
                if item.get('ws'):
                    record['WIND_SPEED'] = float(item['ws'])
            except (ValueError, TypeError):
                pass

            # 풍향
            try:
                if item.get('wd'):
                    record['WIND_DIR'] = int(float(item['wd']))
            except (ValueError, TypeError):
                pass

            result.append(record)

        return result

    def _get_target_grids(self) -> List[Tuple[int, int]]:
        """수집 대상 격자 목록 조회

        TA_FARM의 WEATHER_NX_N, WEATHER_NY_N 기준으로 유니크한 격자 목록 반환
        """
        sql = """
            SELECT DISTINCT WEATHER_NX_N AS NX, WEATHER_NY_N AS NY
            FROM TA_FARM
            WHERE USE_YN = 'Y'
              AND WEATHER_NX_N IS NOT NULL
              AND WEATHER_NY_N IS NOT NULL
        """
        rows = self.db.fetch_dict(sql)
        return [(row['NX'], row['NY']) for row in rows]

    def _get_grids_from_mapxy(self) -> List[Tuple[int, int, float, float]]:
        """MAP_X_N, MAP_Y_N으로부터 격자 좌표 계산

        WEATHER_NX_N, WEATHER_NY_N이 없는 경우 MAP_X_N, MAP_Y_N으로 변환

        Returns:
            [(nx, ny, map_x, map_y), ...]
        """
        sql = """
            SELECT DISTINCT MAP_X_N, MAP_Y_N
            FROM TA_FARM
            WHERE USE_YN = 'Y'
              AND MAP_X_N IS NOT NULL
              AND MAP_Y_N IS NOT NULL
              AND WEATHER_NX_N IS NULL
        """
        rows = self.db.fetch_dict(sql)

        result = []
        for row in rows:
            try:
                lon = float(row['MAP_X_N'])  # 경도 (읍면동 대표)
                lat = float(row['MAP_Y_N'])  # 위도 (읍면동 대표)
                nx, ny = latlon_to_grid(lat, lon)
                result.append((nx, ny, lon, lat))
            except (ValueError, TypeError) as e:
                self.logger.warning(f"좌표 변환 실패: MAP_X_N={row['MAP_X_N']}, MAP_Y_N={row['MAP_Y_N']}: {e}")

        return result

    def _get_grids_with_latlon(self) -> List[Tuple[int, int, float, float]]:
        """격자와 위경도 정보를 함께 조회 (ASOS 관측소 매핑용)

        Returns:
            [(nx, ny, lat, lon), ...] 형태
        """
        sql = """
            SELECT DISTINCT
                WEATHER_NX_N AS NX,
                WEATHER_NY_N AS NY,
                MAP_Y_N AS LAT,
                MAP_X_N AS LON
            FROM TA_FARM
            WHERE USE_YN = 'Y'
              AND WEATHER_NX_N IS NOT NULL
              AND WEATHER_NY_N IS NOT NULL
              AND MAP_X_N IS NOT NULL
              AND MAP_Y_N IS NOT NULL
        """
        rows = self.db.fetch_dict(sql)
        result = []
        for row in rows:
            try:
                result.append((
                    int(row['NX']),
                    int(row['NY']),
                    float(row['LAT']),
                    float(row['LON'])
                ))
            except (ValueError, TypeError):
                pass
        return result

    def collect(self, grids: Optional[List[Tuple[int, int]]] = None, **kwargs) -> Dict[str, List[Dict]]:
        """날씨 데이터 수집 (예보 + 실측)

        수집 전략:
          [1] 단기예보 (getVilageFcst): 오늘~+3일 예보 (IS_FORECAST='Y')
          [2] 초단기실황 (getUltraSrtNcst): 현재 시각 실측 (IS_FORECAST='N')
          [3] ASOS 일자료: 어제까지 실측 (IS_FORECAST='N') - 옵션

        데이터 무결성 보장:
          - 모든 격자 수집 완료 시에만 'is_complete': True 반환
          - API limit 등으로 중단 시 'is_complete': False 반환
          - 호출부(save)에서 is_complete=False일 경우 저장 스킵하여 기존 데이터 유지

        Args:
            grids: 격자 목록 [(nx, ny), ...]. None이면 DB에서 조회

        Returns:
            {'daily': [...], 'hourly': [...], 'ncst': [...], 'is_complete': bool,
             'total_grids': int, 'collected_grids': int, 'failed_grids': [(nx, ny), ...]}
        """
        # API 키 로드
        self.key_manager.load_keys()

        if grids is None:
            grids = self._get_target_grids()

            # WEATHER_NX_N/NY_N이 없는 농장은 MAP_X_N/Y_N으로 변환
            additional = self._get_grids_from_mapxy()
            for nx, ny, _, _ in additional:
                if (nx, ny) not in grids:
                    grids.append((nx, ny))

        if not grids:
            self.logger.warning("수집 대상 격자가 없습니다.")
            return {'daily': [], 'hourly': [], 'ncst': [], 'is_complete': True,
                    'total_grids': 0, 'collected_grids': 0, 'failed_grids': []}

        # 단기예보 기준 날짜/시간
        fcst_base_date, fcst_base_time = self._get_base_datetime()
        # 초단기실황 기준 날짜/시간
        ncst_base_date, ncst_base_time = self._get_ncst_base_datetime()

        # TMN/TMX 확보용 05:00 발표 데이터 조회 여부
        # TMN(최저)과 TMX(최고)는 02:00, 05:00 발표에만 포함됨
        need_tmn_tmx = fcst_base_time not in ('0200', '0500')
        tmn_tmx_base_time = '0500'  # 05:00 발표 데이터 사용

        self.logger.info(f"예보 기준: {fcst_base_date} {fcst_base_time}, 실황 기준: {ncst_base_date} {ncst_base_time}")
        if need_tmn_tmx:
            self.logger.info(f"TMN/TMX 확보용 추가 조회: {fcst_base_date} {tmn_tmx_base_time}")
        self.logger.info(f"대상 격자: {len(grids)}개")

        all_daily = []
        all_hourly = []
        all_ncst = []
        failed_grids = []  # 실패한 격자 목록
        api_limit_reached = False  # API limit으로 인한 중단 여부

        # 중복 격자 제거
        unique_grids = list(set(grids))
        total_grids = len(unique_grids)
        collected_count = 0

        for nx, ny in unique_grids:
            # 모든 키가 limit 상태면 중단 (무결성 보장을 위해 저장하지 않음)
            if not self.key_manager.has_available_key():
                self.logger.error("모든 API 키가 limit 상태입니다. 수집 중단.")
                api_limit_reached = True
                # 남은 격자들을 실패 목록에 추가
                remaining_idx = unique_grids.index((nx, ny))
                failed_grids.extend(unique_grids[remaining_idx:])
                break

            try:
                # [1] 단기예보 수집 (IS_FORECAST='Y')
                items = self._fetch_forecast(nx, ny, fcst_base_date, fcst_base_time)

                # TMN/TMX 확보용 05:00 발표 데이터 조회
                tmn_tmx_map = {}  # {날짜: {'TMN': val, 'TMX': val}}
                if need_tmn_tmx and items:
                    tmn_items = self._fetch_forecast(nx, ny, fcst_base_date, tmn_tmx_base_time)
                    if tmn_items:
                        for item in tmn_items:
                            fcst_date = item.get('fcstDate')
                            category = item.get('category')
                            value = item.get('fcstValue')
                            if fcst_date and category in ('TMN', 'TMX'):
                                if fcst_date not in tmn_tmx_map:
                                    tmn_tmx_map[fcst_date] = {}
                                try:
                                    tmn_tmx_map[fcst_date][category] = float(value)
                                except (ValueError, TypeError):
                                    pass

                if items:
                    daily_data, hourly_data = self._parse_forecast_items(items, nx, ny)

                    # TMN/TMX 병합 (05:00 발표 데이터에서 가져온 값)
                    for wk_date, day in daily_data.items():
                        if wk_date in tmn_tmx_map:
                            if day.get('TMN') is None and 'TMN' in tmn_tmx_map[wk_date]:
                                day['TMN'] = tmn_tmx_map[wk_date]['TMN']
                            if day.get('TMX') is None and 'TMX' in tmn_tmx_map[wk_date]:
                                day['TMX'] = tmn_tmx_map[wk_date]['TMX']

                    daily_records = self._finalize_daily_data(daily_data)
                    hourly_records = self._finalize_hourly_data(hourly_data)

                    # BASE_DATE, BASE_TIME, IS_FORECAST 추가
                    for rec in daily_records:
                        rec['BASE_DATE'] = fcst_base_date
                        rec['BASE_TIME'] = fcst_base_time
                        rec['IS_FORECAST'] = 'Y'
                    for rec in hourly_records:
                        rec['BASE_DATE'] = fcst_base_date
                        rec['BASE_TIME'] = fcst_base_time
                        rec['IS_FORECAST'] = 'Y'

                    all_daily.extend(daily_records)
                    all_hourly.extend(hourly_records)

                    self.logger.debug(f"격자 ({nx}, {ny}) 예보: 일별 {len(daily_records)}건, 시간별 {len(hourly_records)}건")
                else:
                    self.logger.warning(f"격자 ({nx}, {ny}): 예보 데이터 없음")

                # [2] 초단기실황 수집 (IS_FORECAST='N')
                ncst_items = self._fetch_ultra_srt_ncst(nx, ny, ncst_base_date, ncst_base_time)

                if ncst_items:
                    ncst_record = self._parse_ncst_items(ncst_items, nx, ny, ncst_base_date, ncst_base_time)
                    ncst_record['BASE_DATE'] = ncst_base_date
                    ncst_record['BASE_TIME'] = ncst_base_time
                    all_ncst.append(ncst_record)
                    self.logger.debug(f"격자 ({nx}, {ny}) 실황: 1건")

                # 이 격자 수집 완료
                collected_count += 1

            except Exception as e:
                self.logger.error(f"격자 ({nx}, {ny}) 수집 실패: {e}")
                failed_grids.append((nx, ny))
                continue

        # 수집 완료 여부 판단
        is_complete = (collected_count == total_grids) and not api_limit_reached

        self.logger.info(f"수집 완료: 예보 일별 {len(all_daily)}건, 시간별 {len(all_hourly)}건, 실황 {len(all_ncst)}건")
        self.logger.info(f"수집 현황: {collected_count}/{total_grids} 격자 완료, 실패 {len(failed_grids)}개")

        if not is_complete:
            self.logger.warning(f"⚠️ 수집 미완료 (is_complete=False): API limit 또는 오류로 인해 일부 격자 누락")
            self.logger.warning(f"  → 데이터 무결성 보장을 위해 저장하지 않습니다. 기존 데이터 유지.")

        return {
            'daily': all_daily,
            'hourly': all_hourly,
            'ncst': all_ncst,
            'is_complete': is_complete,
            'total_grids': total_grids,
            'collected_grids': collected_count,
            'failed_grids': failed_grids,
        }

    def collect_asos_daily(self, days_back: int = 7,
                            start_dt: Optional[str] = None,
                            end_dt: Optional[str] = None) -> List[Dict]:
        """ASOS 일자료 수집 (과거 일별 실측)

        TA_FARM.ASOS_STN_ID 캐싱을 우선 활용하고, 없는 경우 Haversine 계산.
        격자별로 중복 제거하여 관측소당 1회만 API 호출.

        Args:
            days_back: 몇 일 전까지 조회할지 (기본 7일, start_dt/end_dt 미지정시 사용)
            start_dt: 시작일 (YYYYMMDD) - 지정시 days_back 무시
            end_dt: 종료일 (YYYYMMDD) - 지정시 days_back 무시

        Returns:
            일별 실측 레코드 리스트
        """
        self.key_manager.load_keys()

        # ASOS 관측소 로드 (캐시)
        stations = load_asos_stations(self.db)
        if not stations:
            self.logger.warning("ASOS 관측소 정보가 없습니다. TM_WEATHER_ASOS 테이블을 확인하세요.")
            return []

        # 조회 기간 설정
        now = now_kst()
        if start_dt and end_dt:
            # CLI 옵션으로 기간 지정
            pass
        else:
            # 기본: 어제부터 days_back일 전까지
            end_dt = (now - timedelta(days=1)).strftime('%Y%m%d')
            start_dt = (now - timedelta(days=days_back)).strftime('%Y%m%d')

        # 격자별 ASOS 관측소 매핑 조회 (캐싱된 ASOS_STN_ID 활용)
        # 격자 중복 제거: 같은 (NX, NY)는 한 번만 수집
        grid_asos_map = self._get_grid_asos_mapping(stations)

        if not grid_asos_map:
            self.logger.warning("ASOS 수집 대상 격자가 없습니다.")
            return []

        self.logger.info(f"ASOS 일자료 수집: {start_dt} ~ {end_dt}")
        self.logger.info(f"  대상 격자: {len(grid_asos_map)}개 (중복 제거됨)")
        self.logger.info(f"  대상 관측소: {len(set(v[0] for v in grid_asos_map.values()))}개")

        all_asos_daily = []
        processed_stations = set()

        for (nx, ny), (stn_id, stn_name, dist) in grid_asos_map.items():
            if not self.key_manager.has_available_key():
                self.logger.error("모든 API 키가 limit 상태입니다. ASOS 수집 중단.")
                break

            # 이미 처리한 관측소는 스킵 (같은 관측소 중복 호출 방지)
            if stn_id in processed_stations:
                # 이미 수집한 데이터를 이 격자에도 매핑
                continue
            processed_stations.add(stn_id)

            try:
                items = self._fetch_asos_daily(stn_id, start_dt, end_dt)
                if items:
                    records = self._parse_asos_daily_items(items, nx, ny)
                    all_asos_daily.extend(records)
                    self.logger.debug(f"ASOS ({stn_id} {stn_name}, {dist:.1f}km): {len(records)}건 -> 격자({nx},{ny})")
            except Exception as e:
                self.logger.error(f"ASOS 관측소 {stn_id} 수집 실패: {e}")
                continue

        self.logger.info(f"ASOS 일자료 수집 완료: {len(all_asos_daily)}건")
        return all_asos_daily

    def _get_grid_asos_mapping(self, stations: List[Tuple[int, str, float, float]]) -> Dict[Tuple[int, int], Tuple[int, str, float]]:
        """격자별 ASOS 관측소 매핑 조회

        TA_FARM.ASOS_STN_ID 캐싱을 우선 활용하고, 없는 경우 Haversine 계산.
        격자 중복 제거하여 (NX, NY) -> (STN_ID, STN_NM, DIST_KM) 매핑 반환.

        Args:
            stations: ASOS 관측소 리스트

        Returns:
            {(nx, ny): (stn_id, stn_nm, dist_km), ...}
        """
        # 1. ASOS_STN_ID가 캐싱된 농장 조회 (격자별 중복 제거)
        sql_cached = """
            SELECT DISTINCT
                WEATHER_NX_N AS NX,
                WEATHER_NY_N AS NY,
                ASOS_STN_ID,
                ASOS_STN_NM,
                ASOS_DIST_KM
            FROM TA_FARM
            WHERE USE_YN = 'Y'
              AND WEATHER_NX_N IS NOT NULL
              AND WEATHER_NY_N IS NOT NULL
              AND ASOS_STN_ID IS NOT NULL
        """
        cached_rows = self.db.fetch_dict(sql_cached)

        result = {}
        for row in cached_rows:
            try:
                nx = int(row['NX'])
                ny = int(row['NY'])
                stn_id = int(row['ASOS_STN_ID'])
                stn_nm = row['ASOS_STN_NM'] or ''
                dist = float(row['ASOS_DIST_KM']) if row['ASOS_DIST_KM'] else 0.0
                result[(nx, ny)] = (stn_id, stn_nm, dist)
            except (ValueError, TypeError):
                pass

        self.logger.debug(f"ASOS 캐싱된 격자: {len(result)}개")

        # 2. ASOS_STN_ID가 없는 농장은 Haversine 계산
        sql_uncached = """
            SELECT DISTINCT
                WEATHER_NX_N AS NX,
                WEATHER_NY_N AS NY,
                MAP_Y_N AS LAT,
                MAP_X_N AS LON
            FROM TA_FARM
            WHERE USE_YN = 'Y'
              AND WEATHER_NX_N IS NOT NULL
              AND WEATHER_NY_N IS NOT NULL
              AND MAP_X_N IS NOT NULL
              AND MAP_Y_N IS NOT NULL
              AND ASOS_STN_ID IS NULL
        """
        uncached_rows = self.db.fetch_dict(sql_uncached)

        for row in uncached_rows:
            try:
                nx = int(row['NX'])
                ny = int(row['NY'])

                # 이미 매핑된 격자는 스킵
                if (nx, ny) in result:
                    continue

                lat = float(row['LAT'])
                lon = float(row['LON'])
                stn_id, stn_nm, dist = find_nearest_asos_station(lat, lon, stations)
                result[(nx, ny)] = (stn_id, stn_nm, dist)
            except (ValueError, TypeError):
                pass

        self.logger.debug(f"ASOS 미캐싱 격자 계산: {len(uncached_rows)}개")

        return result

    def save(self, data: Dict[str, List[Dict]]) -> Dict[str, int]:
        """날씨 데이터 저장

        데이터 무결성 보장:
          - is_complete=False인 경우 저장하지 않고 기존 데이터 유지
          - API limit 등으로 일부 격자만 수집된 경우 저장 스킵

        Args:
            data: {'daily': [...], 'hourly': [...], 'ncst': [...],
                   'is_complete': bool, 'total_grids': int, 'collected_grids': int}

        Returns:
            {'daily': 저장건수, 'hourly': 저장건수, 'ncst': 저장건수}
        """
        # 무결성 체크: is_complete=False면 저장하지 않음
        is_complete = data.get('is_complete', True)
        if not is_complete:
            total = data.get('total_grids', 0)
            collected = data.get('collected_grids', 0)
            failed = len(data.get('failed_grids', []))
            self.logger.warning(f"⚠️ 저장 스킵: 수집 미완료 ({collected}/{total} 격자, 실패 {failed}개)")
            self.logger.warning(f"  → 기존 데이터를 유지합니다. 다음 ETL 실행 시 재시도됩니다.")
            return {
                'daily': 0,
                'hourly': 0,
                'ncst': 0,
                'skipped': True,
                'reason': 'incomplete_collection'
            }

        daily_data = data.get('daily', [])
        hourly_data = data.get('hourly', [])
        ncst_data = data.get('ncst', [])

        # [1] 시간별 데이터 먼저 저장 (TM_WEATHER_HOURLY)
        hourly_count = self._save_hourly(hourly_data)

        # [2] 일별 데이터 저장 - DB 집계로 min/max 계산 (오늘 날짜만)
        #     시간별 데이터가 먼저 저장되어야 정확한 집계 가능
        daily_count = self._save_daily_with_aggregation(daily_data)

        # [3] 초단기실황 저장
        ncst_count = self._save_ncst(ncst_data)

        return {
            'daily': daily_count,
            'hourly': hourly_count,
            'ncst': ncst_count,
        }

    def _save_daily(self, data: List[Dict]) -> int:
        """일별 날씨 저장 (TM_WEATHER)

        IS_FORECAST 플래그에 따라 예보/실측 구분하여 저장
        - IS_FORECAST='Y': 예보 데이터 (단기예보)
        - IS_FORECAST='N': 실측 데이터 (ASOS)
        """
        if not data:
            return 0

        # SQL에서 사용하는 필드만 추출 (cx_Oracle executemany 호환)
        required_fields = ['NX', 'NY', 'WK_DATE', 'TEMP_AVG', 'TEMP_HIGH', 'TEMP_LOW',
                          'RAIN_PROB', 'WEATHER_CD', 'WEATHER_NM', 'SKY_CD', 'IS_FORECAST']
        filtered_data = [{k: row.get(k) for k in required_fields} for row in data]

        sql = """
            MERGE INTO TM_WEATHER TGT
            USING (
                SELECT :NX AS NX, :NY AS NY, :WK_DATE AS WK_DATE FROM DUAL
            ) SRC
            ON (TGT.NX = SRC.NX AND TGT.NY = SRC.NY AND TGT.WK_DATE = SRC.WK_DATE)
            WHEN MATCHED THEN
                UPDATE SET
                    TEMP_AVG = :TEMP_AVG,
                    TEMP_HIGH = :TEMP_HIGH,
                    TEMP_LOW = :TEMP_LOW,
                    RAIN_PROB = NVL(:RAIN_PROB, RAIN_PROB),
                    WEATHER_CD = :WEATHER_CD,
                    WEATHER_NM = :WEATHER_NM,
                    SKY_CD = :SKY_CD,
                    FCST_DT = SYSDATE,
                    IS_FORECAST = :IS_FORECAST,
                    LOG_UPT_DT = SYSDATE
            WHEN NOT MATCHED THEN
                INSERT (SEQ, WK_DATE, NX, NY, TEMP_AVG, TEMP_HIGH, TEMP_LOW,
                        RAIN_PROB, WEATHER_CD, WEATHER_NM, SKY_CD,
                        FCST_DT, IS_FORECAST, LOG_INS_DT)
                VALUES (SEQ_TM_WEATHER.NEXTVAL, :WK_DATE, :NX, :NY, :TEMP_AVG, :TEMP_HIGH, :TEMP_LOW,
                        :RAIN_PROB, :WEATHER_CD, :WEATHER_NM, :SKY_CD,
                        SYSDATE, :IS_FORECAST, SYSDATE)
        """

        try:
            self.db.execute_many(sql, filtered_data)
            self.db.commit()
            self.logger.info(f"TM_WEATHER: {len(data)}건 저장 완료")
            return len(data)
        except Exception as e:
            self.logger.error(f"TM_WEATHER 저장 실패: {e}")
            self.db.rollback()
            raise

    def _save_daily_with_aggregation(self, data: List[Dict]) -> int:
        """일별 날씨 저장 - DB 집계로 오늘 min/max 계산

        단기예보는 현재 시점 이후 데이터만 반환하므로,
        오늘 날짜의 경우 TM_WEATHER_HOURLY에서 전체 시간대를 집계하여
        정확한 min/max를 계산합니다.

        흐름:
          1. 오늘 날짜: DB 집계 쿼리로 min/max 계산 후 MERGE
          2. 미래 날짜: API 응답 데이터 그대로 MERGE
        """
        if not data:
            return 0

        today_str = now_kst().strftime('%Y%m%d')
        today_records = []
        future_records = []

        for row in data:
            if row.get('WK_DATE') == today_str:
                today_records.append(row)
            else:
                future_records.append(row)

        saved_count = 0

        # [1] 오늘 날짜: DB 집계로 min/max 계산
        if today_records:
            saved_count += self._save_daily_today_aggregated(today_records)

        # [2] 미래 날짜: API 응답 그대로 저장
        if future_records:
            saved_count += self._save_daily(future_records)

        return saved_count

    def _save_daily_today_aggregated(self, data: List[Dict]) -> int:
        """오늘 날짜 일별 저장 - TM_WEATHER_HOURLY 집계 기반

        TM_WEATHER_HOURLY에서 해당 격자의 전체 시간대 데이터를 집계하여
        정확한 TEMP_LOW, TEMP_HIGH, TEMP_AVG를 계산합니다.
        """
        if not data:
            return 0

        today_str = now_kst().strftime('%Y%m%d')
        updated_data = []

        for row in data:
            nx = row.get('NX')
            ny = row.get('NY')

            # DB에서 오늘 전체 시간대 집계
            agg_sql = """
                SELECT MIN(TEMP) AS TEMP_LOW,
                       MAX(TEMP) AS TEMP_HIGH,
                       ROUND(AVG(TEMP), 1) AS TEMP_AVG
                FROM TM_WEATHER_HOURLY
                WHERE NX = :NX AND NY = :NY AND WK_DATE = :WK_DATE
                  AND TEMP IS NOT NULL
            """
            agg_result = self.db.fetch_dict(agg_sql, {'NX': nx, 'NY': ny, 'WK_DATE': today_str})

            if agg_result and agg_result[0].get('TEMP_LOW') is not None:
                # DB 집계 결과로 덮어쓰기
                row['TEMP_LOW'] = agg_result[0]['TEMP_LOW']
                row['TEMP_HIGH'] = agg_result[0]['TEMP_HIGH']
                row['TEMP_AVG'] = agg_result[0]['TEMP_AVG']
                self.logger.debug(
                    f"오늘 {today_str} ({nx},{ny}): DB 집계 {row['TEMP_LOW']}~{row['TEMP_HIGH']}도"
                )

            updated_data.append(row)

        # 기존 _save_daily 로직으로 저장
        return self._save_daily(updated_data)

    def _save_hourly(self, data: List[Dict]) -> int:
        """시간별 날씨 저장 (TM_WEATHER_HOURLY) - 예보 데이터"""
        if not data:
            return 0

        # SQL에서 사용하는 필드만 추출 (cx_Oracle executemany 호환)
        required_fields = ['NX', 'NY', 'WK_DATE', 'WK_TIME', 'TEMP', 'RAIN_PROB', 'RAIN_AMT',
                          'HUMIDITY', 'WIND_SPEED', 'WIND_DIR', 'WEATHER_CD', 'WEATHER_NM',
                          'SKY_CD', 'PTY_CD', 'BASE_DATE', 'BASE_TIME', 'IS_FORECAST']
        filtered_data = [{k: row.get(k) for k in required_fields} for row in data]

        sql = """
            MERGE INTO TM_WEATHER_HOURLY TGT
            USING (
                SELECT :NX AS NX, :NY AS NY, :WK_DATE AS WK_DATE, :WK_TIME AS WK_TIME FROM DUAL
            ) SRC
            ON (TGT.NX = SRC.NX AND TGT.NY = SRC.NY AND TGT.WK_DATE = SRC.WK_DATE AND TGT.WK_TIME = SRC.WK_TIME)
            WHEN MATCHED THEN
                UPDATE SET
                    TEMP = :TEMP,
                    RAIN_PROB = :RAIN_PROB,
                    RAIN_AMT = :RAIN_AMT,
                    HUMIDITY = :HUMIDITY,
                    WIND_SPEED = :WIND_SPEED,
                    WIND_DIR = :WIND_DIR,
                    WEATHER_CD = :WEATHER_CD,
                    WEATHER_NM = :WEATHER_NM,
                    SKY_CD = :SKY_CD,
                    PTY_CD = :PTY_CD,
                    FCST_DT = SYSDATE,
                    BASE_DATE = :BASE_DATE,
                    BASE_TIME = :BASE_TIME,
                    IS_FORECAST = :IS_FORECAST,
                    LOG_UPT_DT = SYSDATE
            WHEN NOT MATCHED THEN
                INSERT (SEQ, WK_DATE, WK_TIME, NX, NY, TEMP, RAIN_PROB, RAIN_AMT,
                        HUMIDITY, WIND_SPEED, WIND_DIR, WEATHER_CD, WEATHER_NM,
                        SKY_CD, PTY_CD, FCST_DT, BASE_DATE, BASE_TIME, IS_FORECAST, LOG_INS_DT)
                VALUES (SEQ_TM_WEATHER_HOURLY.NEXTVAL, :WK_DATE, :WK_TIME, :NX, :NY, :TEMP, :RAIN_PROB, :RAIN_AMT,
                        :HUMIDITY, :WIND_SPEED, :WIND_DIR, :WEATHER_CD, :WEATHER_NM,
                        :SKY_CD, :PTY_CD, SYSDATE, :BASE_DATE, :BASE_TIME, :IS_FORECAST, SYSDATE)
        """

        try:
            self.db.execute_many(sql, filtered_data)
            self.db.commit()
            self.logger.info(f"TM_WEATHER_HOURLY: {len(data)}건 저장 완료")
            return len(data)
        except Exception as e:
            self.logger.error(f"TM_WEATHER_HOURLY 저장 실패: {e}")
            self.db.rollback()
            raise

    def _save_ncst(self, data: List[Dict]) -> int:
        """초단기실황 저장 (TM_WEATHER_HOURLY) - 실측 데이터

        실측 데이터는 IS_FORECAST='N'으로 저장
        기존 예보 데이터가 있으면 실측으로 덮어씀
        """
        if not data:
            return 0

        # SQL에서 사용하는 필드만 추출 (cx_Oracle executemany 호환)
        required_fields = ['NX', 'NY', 'WK_DATE', 'WK_TIME', 'TEMP', 'RAIN_AMT',
                          'HUMIDITY', 'WIND_SPEED', 'WIND_DIR', 'WEATHER_CD', 'WEATHER_NM',
                          'SKY_CD', 'PTY_CD', 'BASE_DATE', 'BASE_TIME']
        filtered_data = [{k: row.get(k) for k in required_fields} for row in data]

        sql = """
            MERGE INTO TM_WEATHER_HOURLY TGT
            USING (
                SELECT :NX AS NX, :NY AS NY, :WK_DATE AS WK_DATE, :WK_TIME AS WK_TIME FROM DUAL
            ) SRC
            ON (TGT.NX = SRC.NX AND TGT.NY = SRC.NY AND TGT.WK_DATE = SRC.WK_DATE AND TGT.WK_TIME = SRC.WK_TIME)
            WHEN MATCHED THEN
                UPDATE SET
                    TEMP = :TEMP,
                    RAIN_AMT = :RAIN_AMT,
                    HUMIDITY = :HUMIDITY,
                    WIND_SPEED = :WIND_SPEED,
                    WIND_DIR = :WIND_DIR,
                    WEATHER_CD = :WEATHER_CD,
                    WEATHER_NM = :WEATHER_NM,
                    SKY_CD = :SKY_CD,
                    PTY_CD = :PTY_CD,
                    FCST_DT = SYSDATE,
                    BASE_DATE = :BASE_DATE,
                    BASE_TIME = :BASE_TIME,
                    IS_FORECAST = 'N',
                    LOG_UPT_DT = SYSDATE
            WHEN NOT MATCHED THEN
                INSERT (SEQ, WK_DATE, WK_TIME, NX, NY, TEMP, RAIN_AMT,
                        HUMIDITY, WIND_SPEED, WIND_DIR, WEATHER_CD, WEATHER_NM,
                        SKY_CD, PTY_CD, FCST_DT, BASE_DATE, BASE_TIME, IS_FORECAST, LOG_INS_DT)
                VALUES (SEQ_TM_WEATHER_HOURLY.NEXTVAL, :WK_DATE, :WK_TIME, :NX, :NY, :TEMP, :RAIN_AMT,
                        :HUMIDITY, :WIND_SPEED, :WIND_DIR, :WEATHER_CD, :WEATHER_NM,
                        :SKY_CD, :PTY_CD, SYSDATE, :BASE_DATE, :BASE_TIME, 'N', SYSDATE)
        """

        try:
            self.db.execute_many(sql, filtered_data)
            self.db.commit()
            self.logger.info(f"TM_WEATHER_HOURLY (실황): {len(data)}건 저장 완료")
            return len(data)
        except Exception as e:
            self.logger.error(f"TM_WEATHER_HOURLY (실황) 저장 실패: {e}")
            self.db.rollback()
            raise

    def save_asos_daily(self, data: List[Dict]) -> int:
        """ASOS 일자료 저장 (TM_WEATHER) - 실측 데이터

        실측 데이터는 IS_FORECAST='N'으로 저장
        """
        if not data:
            return 0

        return self._save_daily(data)

    def run(self, collect_asos: bool = False,
            collect_mid: bool = True,
            asos_days_back: int = 7,
            asos_start_dt: Optional[str] = None,
            asos_end_dt: Optional[str] = None) -> Dict[str, int]:
        """날씨 수집 실행 (수집 + 저장)

        Args:
            collect_asos: ASOS 일자료도 수집할지 여부 (기본 False)
            collect_mid: 중기예보도 수집할지 여부 (기본 True)
            asos_days_back: ASOS 조회 일수 (기본 7일)
            asos_start_dt: ASOS 시작일 (YYYYMMDD) - 지정시 days_back 무시
            asos_end_dt: ASOS 종료일 (YYYYMMDD) - 지정시 days_back 무시

        Returns:
            {'daily': 건수, 'hourly': 건수, 'ncst': 건수, 'asos': 건수, 'mid': 건수}
        """
        self.logger.info("=== 기상청 날씨 데이터 수집 시작 ===")

        try:
            # [1] 단기예보 + 초단기실황 수집
            data = self.collect()
            result = self.save(data)

            # [2] 중기예보 수집 (기본 활성화)
            mid_count = 0
            if collect_mid:
                mid_data = self.collect_mid_forecast()
                mid_count = self.save_mid_forecast(mid_data)

            result['mid'] = mid_count

            # [3] ASOS 일자료 수집 (옵션)
            asos_count = 0
            if collect_asos:
                asos_data = self.collect_asos_daily(
                    days_back=asos_days_back,
                    start_dt=asos_start_dt,
                    end_dt=asos_end_dt
                )
                asos_count = self.save_asos_daily(asos_data)

            result['asos'] = asos_count

            self.logger.info("=" * 60)
            if result.get('skipped'):
                self.logger.warning(f"⚠️ 저장 스킵됨: {result.get('reason', 'unknown')}")
                self.logger.warning(f"  → 기존 데이터 유지, 다음 ETL 실행 시 재시도")
            else:
                self.logger.info(f"수집 완료:")
                self.logger.info(f"  단기 일별: {result['daily']}건")
                self.logger.info(f"  단기 시간별: {result['hourly']}건")
                self.logger.info(f"  초단기실황: {result['ncst']}건")
                if collect_mid:
                    self.logger.info(f"  중기 일별: {result['mid']}건")
                if collect_asos:
                    self.logger.info(f"  ASOS 일자료: {result['asos']}건")
            self.logger.info("=" * 60)

            return result

        except Exception as e:
            self.logger.error(f"날씨 수집 실패: {e}")
            raise

    # ============================================================================
    # 중기예보 수집 (getMidTa, getMidLandFcst)
    # ============================================================================

    def _get_mid_base_datetime(self) -> str:
        """중기예보 API 호출용 발표시각 계산

        중기예보: 일 2회 발표 (06:00, 18:00)
        발표 후 약 30분 뒤 데이터 제공

        Returns:
            발표시각 (YYYYMMDDHHMM 형식, 예: 202501140600)
        """
        now = now_kst()
        current_hour = now.hour

        # 06:30 이전 → 전날 18:00 발표
        # 06:30~18:30 → 당일 06:00 발표
        # 18:30 이후 → 당일 18:00 발표
        if current_hour < 6 or (current_hour == 6 and now.minute < 30):
            # 전날 18:00
            base_dt = (now - timedelta(days=1)).strftime('%Y%m%d') + '1800'
        elif current_hour < 18 or (current_hour == 18 and now.minute < 30):
            # 당일 06:00
            base_dt = now.strftime('%Y%m%d') + '0600'
        else:
            # 당일 18:00
            base_dt = now.strftime('%Y%m%d') + '1800'

        return base_dt

    def _fetch_mid_ta(self, reg_id: str, tm_fc: str) -> Optional[Dict]:
        """중기기온예보 API 호출 (getMidTa)

        Args:
            reg_id: 예보구역코드 (예: '11B10101')
            tm_fc: 발표시각 (YYYYMMDDHHMM)

        Returns:
            기온예보 데이터 dict 또는 None
        """
        url = f"{self.mid_fcst_url}/getMidTa"

        while self.key_manager.has_available_key():
            api_key = self.key_manager.get_current_key()
            if not api_key:
                break

            params = {
                'serviceKey': api_key,
                'pageNo': 1,
                'numOfRows': 10,
                'dataType': 'JSON',
                'regId': reg_id,
                'tmFc': tm_fc,
            }

            try:
                self.logger.debug(f"중기기온 API 호출: regId={reg_id}, tmFc={tm_fc}")
                response = requests.get(url, params=params, timeout=30)

                if response.status_code in (401, 403, 429):
                    self.logger.warning(f"중기기온 API 키 인증/제한 오류 ({response.status_code})")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                response.raise_for_status()
                data = response.json()

                result_code = data.get('response', {}).get('header', {}).get('resultCode')
                result_msg = data.get('response', {}).get('header', {}).get('resultMsg', '')

                if result_code == '00':
                    self.key_manager.increment_count(api_key)
                    items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
                    if isinstance(items, list) and len(items) > 0:
                        return items[0]
                    elif isinstance(items, dict):
                        return items
                    return None

                elif result_code in ApiKeyManager.LIMIT_ERROR_CODES:
                    self.logger.warning(f"중기기온 API 호출 제한: {result_code} - {result_msg}")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                else:
                    self.logger.error(f"중기기온 API 오류: {result_code} - {result_msg}")
                    return None

            except requests.RequestException as e:
                self.logger.error(f"중기기온 API 호출 실패: {e}")
                return None
            except (KeyError, ValueError) as e:
                self.logger.error(f"중기기온 API 응답 파싱 실패: {e}")
                return None

        self.logger.error("모든 API 키가 소진되었습니다. (중기기온)")
        return None

    def _fetch_mid_land_fcst(self, reg_id: str, tm_fc: str) -> Optional[Dict]:
        """중기육상예보 API 호출 (getMidLandFcst)

        Args:
            reg_id: 예보구역코드 (예: '11B00000')
            tm_fc: 발표시각 (YYYYMMDDHHMM)

        Returns:
            육상예보 데이터 dict 또는 None
        """
        url = f"{self.mid_fcst_url}/getMidLandFcst"

        while self.key_manager.has_available_key():
            api_key = self.key_manager.get_current_key()
            if not api_key:
                break

            params = {
                'serviceKey': api_key,
                'pageNo': 1,
                'numOfRows': 10,
                'dataType': 'JSON',
                'regId': reg_id,
                'tmFc': tm_fc,
            }

            try:
                self.logger.debug(f"중기육상 API 호출: regId={reg_id}, tmFc={tm_fc}")
                response = requests.get(url, params=params, timeout=30)

                if response.status_code in (401, 403, 429):
                    self.logger.warning(f"중기육상 API 키 인증/제한 오류 ({response.status_code})")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                response.raise_for_status()
                data = response.json()

                result_code = data.get('response', {}).get('header', {}).get('resultCode')
                result_msg = data.get('response', {}).get('header', {}).get('resultMsg', '')

                if result_code == '00':
                    self.key_manager.increment_count(api_key)
                    items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
                    if isinstance(items, list) and len(items) > 0:
                        return items[0]
                    elif isinstance(items, dict):
                        return items
                    return None

                elif result_code in ApiKeyManager.LIMIT_ERROR_CODES:
                    self.logger.warning(f"중기육상 API 호출 제한: {result_code} - {result_msg}")
                    self.key_manager.mark_key_exhausted(api_key)
                    continue

                else:
                    self.logger.error(f"중기육상 API 오류: {result_code} - {result_msg}")
                    return None

            except requests.RequestException as e:
                self.logger.error(f"중기육상 API 호출 실패: {e}")
                return None
            except (KeyError, ValueError) as e:
                self.logger.error(f"중기육상 API 응답 파싱 실패: {e}")
                return None

        self.logger.error("모든 API 키가 소진되었습니다. (중기육상)")
        return None

    def _get_target_grids_with_sigun(self) -> List[Tuple[int, int, str]]:
        """격자 목록을 시군구코드와 함께 조회 (중기예보용)

        Returns:
            [(nx, ny, sigun_cd), ...] 형태
        """
        sql = """
            SELECT DISTINCT
                WEATHER_NX_N AS NX,
                WEATHER_NY_N AS NY,
                SIGUN_CD
            FROM TA_FARM
            WHERE USE_YN = 'Y'
              AND WEATHER_NX_N IS NOT NULL
              AND WEATHER_NY_N IS NOT NULL
              AND SIGUN_CD IS NOT NULL
        """
        rows = self.db.fetch_dict(sql)
        result = []
        for row in rows:
            try:
                nx = int(row['NX'])
                ny = int(row['NY'])
                sigun_cd = str(row['SIGUN_CD'] or '')
                result.append((nx, ny, sigun_cd))
            except (ValueError, TypeError):
                pass
        return result

    def collect_mid_forecast(self) -> Dict[str, List[Dict]]:
        """중기예보 수집 (+3일 ~ +10일)

        중기기온예보(getMidTa)와 중기육상예보(getMidLandFcst)를 수집하여
        격자별 일별 데이터로 변환

        Returns:
            {'daily': [일별 레코드 리스트], 'is_complete': bool}
        """
        self.key_manager.load_keys()

        # 발표시각
        tm_fc = self._get_mid_base_datetime()
        self.logger.info(f"중기예보 수집 시작 (발표시각: {tm_fc})")

        # 격자별 시군구코드 조회
        grids_with_sigun = self._get_target_grids_with_sigun()
        if not grids_with_sigun:
            self.logger.warning("중기예보 수집 대상 격자가 없습니다.")
            return {'daily': [], 'is_complete': True}

        # 지역코드별로 그룹핑 (중복 API 호출 방지)
        # {(ta_reg_id, land_reg_id): [(nx, ny), ...]}
        reg_id_groups: Dict[Tuple[str, str], List[Tuple[int, int]]] = {}
        for nx, ny, sigun_cd in grids_with_sigun:
            ta_reg_id = get_mid_ta_reg_id(sigun_cd)
            land_reg_id = get_mid_land_reg_id(sigun_cd)
            key = (ta_reg_id, land_reg_id)
            if key not in reg_id_groups:
                reg_id_groups[key] = []
            reg_id_groups[key].append((nx, ny))

        self.logger.info(f"중기예보 대상: 격자 {len(grids_with_sigun)}개, 지역코드 {len(reg_id_groups)}개")

        # 기준일 계산 (발표시각 기준)
        base_date = datetime.strptime(tm_fc[:8], '%Y%m%d')

        all_daily = []
        failed_reg_ids = []
        api_limit_reached = False

        for (ta_reg_id, land_reg_id), grid_list in reg_id_groups.items():
            if not self.key_manager.has_available_key():
                self.logger.error("모든 API 키가 limit 상태입니다. 중기예보 수집 중단.")
                api_limit_reached = True
                failed_reg_ids.append((ta_reg_id, land_reg_id))
                continue

            try:
                # [1] 중기기온예보 조회
                ta_data = self._fetch_mid_ta(ta_reg_id, tm_fc)

                # [2] 중기육상예보 조회
                land_data = self._fetch_mid_land_fcst(land_reg_id, tm_fc)

                if not ta_data and not land_data:
                    self.logger.warning(f"중기예보 데이터 없음: ta={ta_reg_id}, land={land_reg_id}")
                    continue

                # [3] 격자별로 일별 레코드 생성 (+3일 ~ +10일)
                for nx, ny in grid_list:
                    for day_offset in range(3, 11):  # 3일 후 ~ 10일 후
                        target_date = base_date + timedelta(days=day_offset)
                        wk_date = target_date.strftime('%Y%m%d')

                        record = {
                            'NX': nx,
                            'NY': ny,
                            'WK_DATE': wk_date,
                            'IS_FORECAST': 'Y',
                        }

                        # 기온 데이터 (taMin3~10, taMax3~10)
                        if ta_data:
                            ta_min_key = f'taMin{day_offset}'
                            ta_max_key = f'taMax{day_offset}'
                            record['TEMP_LOW'] = ta_data.get(ta_min_key)
                            record['TEMP_HIGH'] = ta_data.get(ta_max_key)

                            # 평균기온 계산 (최저+최고)/2
                            if record['TEMP_LOW'] is not None and record['TEMP_HIGH'] is not None:
                                try:
                                    record['TEMP_AVG'] = round(
                                        (float(record['TEMP_LOW']) + float(record['TEMP_HIGH'])) / 2, 1
                                    )
                                except (ValueError, TypeError):
                                    record['TEMP_AVG'] = None

                        # 날씨/강수확률 데이터 (wf3Am~10Am, rnSt3Am~10Am 등)
                        if land_data:
                            # 3~7일: AM/PM 구분, 8~10일: 하루 단위
                            if day_offset <= 7:
                                wf_key = f'wf{day_offset}Am'  # AM 기준
                                rn_key_am = f'rnSt{day_offset}Am'
                                rn_key_pm = f'rnSt{day_offset}Pm'
                                # 강수확률: AM/PM 중 높은 값
                                rn_am = land_data.get(rn_key_am)
                                rn_pm = land_data.get(rn_key_pm)
                                if rn_am is not None or rn_pm is not None:
                                    rn_am = int(rn_am) if rn_am is not None else 0
                                    rn_pm = int(rn_pm) if rn_pm is not None else 0
                                    record['RAIN_PROB'] = max(rn_am, rn_pm)
                            else:
                                wf_key = f'wf{day_offset}'
                                rn_key = f'rnSt{day_offset}'
                                record['RAIN_PROB'] = land_data.get(rn_key)

                            # 날씨상태 파싱
                            wf_value = land_data.get(wf_key, '')
                            weather_cd, weather_nm = self._parse_mid_weather(wf_value)
                            record['WEATHER_CD'] = weather_cd
                            record['WEATHER_NM'] = weather_nm
                            record['SKY_CD'] = self._weather_cd_to_sky_cd(weather_cd)

                        # 기온 데이터가 없는 날짜는 스킵 (단기예보 데이터 유지)
                        if record.get('TEMP_LOW') is None and record.get('TEMP_HIGH') is None:
                            self.logger.debug(f"중기예보 스킵: {wk_date} ({nx},{ny}) - 기온 데이터 없음")
                            continue

                        all_daily.append(record)

                self.logger.debug(f"중기예보 처리: ta={ta_reg_id}, land={land_reg_id}, 격자 {len(grid_list)}개")

            except Exception as e:
                self.logger.error(f"중기예보 수집 오류 ({ta_reg_id}, {land_reg_id}): {e}")
                failed_reg_ids.append((ta_reg_id, land_reg_id))
                continue

        is_complete = len(failed_reg_ids) == 0 and not api_limit_reached

        self.logger.info(f"중기예보 수집 완료: 일별 {len(all_daily)}건")
        if not is_complete:
            self.logger.warning(f"⚠️ 중기예보 수집 미완료: 실패 {len(failed_reg_ids)}개 지역")

        return {
            'daily': all_daily,
            'is_complete': is_complete,
        }

    def _parse_mid_weather(self, wf_value: str) -> Tuple[str, str]:
        """중기예보 날씨상태 문자열 파싱

        Args:
            wf_value: 날씨상태 (예: '맑음', '구름많음', '흐리고 비', '흐림')

        Returns:
            (weather_cd, weather_nm) 튜플
        """
        if not wf_value:
            return ('cloudy', '구름많음')

        wf_lower = wf_value.strip()

        # 강수형태 우선 체크
        if '비' in wf_lower and '눈' in wf_lower:
            return ('rain_snow', '비/눈')
        elif '눈' in wf_lower:
            return ('snow', '눈')
        elif '비' in wf_lower or '소나기' in wf_lower:
            return ('rainy', '비')

        # 하늘상태
        if '맑음' in wf_lower:
            return ('sunny', '맑음')
        elif '구름많음' in wf_lower or '구름 많음' in wf_lower:
            return ('cloudy', '구름많음')
        elif '흐림' in wf_lower or '흐리고' in wf_lower:
            return ('overcast', '흐림')

        return ('cloudy', '구름많음')

    def _weather_cd_to_sky_cd(self, weather_cd: str) -> str:
        """weather_cd를 SKY 코드로 변환

        Args:
            weather_cd: 날씨코드 (sunny, cloudy, overcast, rainy, snow 등)

        Returns:
            SKY 코드 (1: 맑음, 3: 구름많음, 4: 흐림)
        """
        mapping = {
            'sunny': '1',
            'cloudy': '3',
            'overcast': '4',
            'rainy': '4',
            'rain_snow': '4',
            'snow': '4',
            'shower': '4',
        }
        return mapping.get(weather_cd, '3')

    def save_mid_forecast(self, data: Dict[str, List[Dict]]) -> int:
        """중기예보 데이터 저장 (TM_WEATHER)

        단기예보 데이터가 없는 날짜에만 INSERT (기존 데이터 덮어쓰기 안함)

        Args:
            data: {'daily': [...], 'is_complete': bool}

        Returns:
            저장 건수
        """
        is_complete = data.get('is_complete', True)
        if not is_complete:
            self.logger.warning("⚠️ 중기예보 저장 스킵: 수집 미완료")
            return 0

        daily_data = data.get('daily', [])
        if not daily_data:
            return 0

        return self._save_mid_daily(daily_data)

    def _save_mid_daily(self, data: List[Dict]) -> int:
        """중기예보 일별 저장 (INSERT only, 기존 데이터 없는 경우만)

        단기예보 데이터가 있는 날짜는 스킵하여 덮어쓰기 방지
        """
        if not data:
            return 0

        # SQL에서 사용하는 필드만 추출 (cx_Oracle executemany 호환)
        required_fields = ['NX', 'NY', 'WK_DATE', 'TEMP_AVG', 'TEMP_HIGH', 'TEMP_LOW',
                          'RAIN_PROB', 'WEATHER_CD', 'WEATHER_NM', 'SKY_CD', 'IS_FORECAST']
        filtered_data = [{k: row.get(k) for k in required_fields} for row in data]

        # 기존 데이터가 없는 경우에만 INSERT (MERGE의 WHEN NOT MATCHED만 사용)
        sql = """
            MERGE INTO TM_WEATHER TGT
            USING (
                SELECT :NX AS NX, :NY AS NY, :WK_DATE AS WK_DATE FROM DUAL
            ) SRC
            ON (TGT.NX = SRC.NX AND TGT.NY = SRC.NY AND TGT.WK_DATE = SRC.WK_DATE)
            WHEN NOT MATCHED THEN
                INSERT (SEQ, WK_DATE, NX, NY, TEMP_AVG, TEMP_HIGH, TEMP_LOW,
                        RAIN_PROB, WEATHER_CD, WEATHER_NM, SKY_CD,
                        FCST_DT, IS_FORECAST, LOG_INS_DT)
                VALUES (SEQ_TM_WEATHER.NEXTVAL, :WK_DATE, :NX, :NY, :TEMP_AVG, :TEMP_HIGH, :TEMP_LOW,
                        :RAIN_PROB, :WEATHER_CD, :WEATHER_NM, :SKY_CD,
                        SYSDATE, :IS_FORECAST, SYSDATE)
        """

        try:
            self.db.execute_many(sql, filtered_data)
            self.db.commit()
            self.logger.info(f"TM_WEATHER(중기): {len(data)}건 저장 시도 (기존 데이터 제외)")
            return len(data)
        except Exception as e:
            self.logger.error(f"TM_WEATHER(중기) 저장 실패: {e}")
            self.db.rollback()
            raise


def update_farm_weather_grid(db: Database):
    """TA_FARM의 WEATHER_NX_N, WEATHER_NY_N 업데이트

    MAP_X_N, MAP_Y_N이 있고 WEATHER_NX_N, WEATHER_NY_N이 없는 농장의 격자 좌표 계산
    (읍면동 대표 좌표 기반)
    """
    logger.info("TA_FARM 격자 좌표 업데이트 시작")

    # 업데이트 대상 조회 (읍면동 대표 좌표 기준)
    sql = """
        SELECT FARM_NO, MAP_X_N, MAP_Y_N
        FROM TA_FARM
        WHERE USE_YN = 'Y'
          AND MAP_X_N IS NOT NULL
          AND MAP_Y_N IS NOT NULL
          AND (WEATHER_NX_N IS NULL OR WEATHER_NY_N IS NULL)
    """
    rows = db.fetch_dict(sql)

    if not rows:
        logger.info("업데이트 대상 농장 없음")
        return 0

    update_sql = """
        UPDATE TA_FARM
        SET WEATHER_NX_N = :NX, WEATHER_NY_N = :NY, LOG_UPT_DT = SYSDATE
        WHERE FARM_NO = :FARM_NO
    """

    updates = []
    for row in rows:
        try:
            lon = float(row['MAP_X_N'])
            lat = float(row['MAP_Y_N'])
            nx, ny = latlon_to_grid(lat, lon)
            updates.append({
                'FARM_NO': row['FARM_NO'],
                'NX': nx,
                'NY': ny,
            })
            logger.debug(f"농장 {row['FARM_NO']}: ({lon}, {lat}) -> 격자 ({nx}, {ny})")
        except (ValueError, TypeError) as e:
            logger.warning(f"농장 {row['FARM_NO']} 좌표 변환 실패: {e}")

    if updates:
        db.execute_many(update_sql, updates)
        db.commit()
        logger.info(f"TA_FARM 격자 좌표 업데이트: {len(updates)}건")

    return len(updates)
