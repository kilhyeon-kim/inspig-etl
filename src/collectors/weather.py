"""
기상청 데이터 수집기
- 기상청 단기예보 API를 통한 날씨 데이터 수집
- 격자(NX, NY) 기반 날씨 정보 조회 및 TM_WEATHER, TM_WEATHER_HOURLY 저장
- TS_API_KEY_INFO 테이블에서 API 키 관리 (호출 횟수 기반 로드밸런싱)
"""
import logging
import math
import requests
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .base import BaseCollector
from ..common import Config, Database, now_kst, ApiKeyManager

logger = logging.getLogger(__name__)


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

    def __init__(self, config: Optional[Config] = None, db: Optional[Database] = None):
        super().__init__(config, db)
        self.weather_config = self.config.weather
        self.base_url = self.weather_config.get('base_url', 'https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0')

        # API 키 관리자
        self.key_manager = ApiKeyManager(self.db)

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

        self.logger.error("모든 API 키가 limit 상태입니다.")
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

        return daily_data, hourly_data

    def _finalize_daily_data(self, daily_data: Dict) -> List[Dict]:
        """일별 데이터 최종 가공

        Args:
            daily_data: {날짜: {필드: 값}} 형태

        Returns:
            TM_WEATHER 레코드 리스트
        """
        result = []

        for wk_date, day in daily_data.items():
            # 평균 기온 계산
            temp_list = day.get('TMP_list', [])
            temp_avg = sum(temp_list) / len(temp_list) if temp_list else None

            # 날씨 코드 결정 (강수형태 > 하늘상태)
            pty_cd = day.get('PTY_CD', '0')
            sky_cd = day.get('SKY_CD', '1')

            if pty_cd and pty_cd != '0':
                weather_cd, weather_nm = self.PTY_CODES.get(pty_cd, ('unknown', '알수없음'))
            else:
                weather_cd, weather_nm = self.SKY_CODES.get(sky_cd, ('unknown', '알수없음'))

            result.append({
                'WK_DATE': wk_date,
                'NX': day['NX'],
                'NY': day['NY'],
                'TEMP_AVG': round(temp_avg, 1) if temp_avg else None,
                'TEMP_HIGH': day.get('TMX'),
                'TEMP_LOW': day.get('TMN'),
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

    def _get_target_grids(self) -> List[Tuple[int, int]]:
        """수집 대상 격자 목록 조회

        TA_FARM의 WEATHER_NX, WEATHER_NY 기준으로 유니크한 격자 목록 반환
        """
        sql = """
            SELECT DISTINCT WEATHER_NX AS NX, WEATHER_NY AS NY
            FROM TA_FARM
            WHERE USE_YN = 'Y'
              AND WEATHER_NX IS NOT NULL
              AND WEATHER_NY IS NOT NULL
        """
        rows = self.db.fetch_dict(sql)
        return [(row['NX'], row['NY']) for row in rows]

    def _get_grids_from_mapxy(self) -> List[Tuple[int, int, float, float]]:
        """MAP_X, MAP_Y로부터 격자 좌표 계산

        WEATHER_NX, WEATHER_NY가 없는 경우 MAP_X, MAP_Y로 변환

        Returns:
            [(nx, ny, map_x, map_y), ...]
        """
        sql = """
            SELECT DISTINCT MAP_X, MAP_Y
            FROM TA_FARM
            WHERE USE_YN = 'Y'
              AND MAP_X IS NOT NULL
              AND MAP_Y IS NOT NULL
              AND WEATHER_NX IS NULL
        """
        rows = self.db.fetch_dict(sql)

        result = []
        for row in rows:
            try:
                lon = float(row['MAP_X'])  # 경도
                lat = float(row['MAP_Y'])  # 위도
                nx, ny = latlon_to_grid(lat, lon)
                result.append((nx, ny, lon, lat))
            except (ValueError, TypeError) as e:
                self.logger.warning(f"좌표 변환 실패: MAP_X={row['MAP_X']}, MAP_Y={row['MAP_Y']}: {e}")

        return result

    def collect(self, grids: Optional[List[Tuple[int, int]]] = None, **kwargs) -> Dict[str, List[Dict]]:
        """날씨 데이터 수집

        Args:
            grids: 격자 목록 [(nx, ny), ...]. None이면 DB에서 조회

        Returns:
            {'daily': [...], 'hourly': [...]} 형태의 수집 결과
        """
        # API 키 로드
        self.key_manager.load_keys()

        if grids is None:
            grids = self._get_target_grids()

            # WEATHER_NX/NY가 없는 농장은 MAP_X/Y로 변환
            additional = self._get_grids_from_mapxy()
            for nx, ny, _, _ in additional:
                if (nx, ny) not in grids:
                    grids.append((nx, ny))

        if not grids:
            self.logger.warning("수집 대상 격자가 없습니다.")
            return {'daily': [], 'hourly': []}

        base_date, base_time = self._get_base_datetime()
        self.logger.info(f"기준 날짜/시간: {base_date} {base_time}, 대상 격자: {len(grids)}개")

        all_daily = []
        all_hourly = []

        # 중복 격자 제거
        unique_grids = list(set(grids))

        for nx, ny in unique_grids:
            # 모든 키가 limit 상태면 중단
            if not self.key_manager.has_available_key():
                self.logger.error("모든 API 키가 limit 상태입니다. 수집 중단.")
                break

            try:
                items = self._fetch_forecast(nx, ny, base_date, base_time)

                if not items:
                    self.logger.warning(f"격자 ({nx}, {ny}): 데이터 없음")
                    continue

                daily_data, hourly_data = self._parse_forecast_items(items, nx, ny)

                daily_records = self._finalize_daily_data(daily_data)
                hourly_records = self._finalize_hourly_data(hourly_data)

                # BASE_DATE, BASE_TIME 추가
                for rec in daily_records:
                    rec['BASE_DATE'] = base_date
                    rec['BASE_TIME'] = base_time
                for rec in hourly_records:
                    rec['BASE_DATE'] = base_date
                    rec['BASE_TIME'] = base_time

                all_daily.extend(daily_records)
                all_hourly.extend(hourly_records)

                self.logger.info(f"격자 ({nx}, {ny}): 일별 {len(daily_records)}건, 시간별 {len(hourly_records)}건")

            except Exception as e:
                self.logger.error(f"격자 ({nx}, {ny}) 수집 실패: {e}")
                continue

        return {
            'daily': all_daily,
            'hourly': all_hourly,
        }

    def save(self, data: Dict[str, List[Dict]]) -> Dict[str, int]:
        """날씨 데이터 저장

        Args:
            data: {'daily': [...], 'hourly': [...]} 형태

        Returns:
            {'daily': 저장건수, 'hourly': 저장건수}
        """
        daily_data = data.get('daily', [])
        hourly_data = data.get('hourly', [])

        daily_count = self._save_daily(daily_data)
        hourly_count = self._save_hourly(hourly_data)

        return {
            'daily': daily_count,
            'hourly': hourly_count,
        }

    def _save_daily(self, data: List[Dict]) -> int:
        """일별 날씨 저장 (TM_WEATHER)"""
        if not data:
            return 0

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
                    RAIN_PROB = :RAIN_PROB,
                    WEATHER_CD = :WEATHER_CD,
                    WEATHER_NM = :WEATHER_NM,
                    SKY_CD = :SKY_CD,
                    FCST_DT = SYSDATE,
                    IS_FORECAST = 'Y',
                    LOG_UPT_DT = SYSDATE
            WHEN NOT MATCHED THEN
                INSERT (SEQ, WK_DATE, NX, NY, TEMP_AVG, TEMP_HIGH, TEMP_LOW,
                        RAIN_PROB, WEATHER_CD, WEATHER_NM, SKY_CD,
                        FCST_DT, IS_FORECAST, LOG_INS_DT)
                VALUES (SEQ_TM_WEATHER.NEXTVAL, :WK_DATE, :NX, :NY, :TEMP_AVG, :TEMP_HIGH, :TEMP_LOW,
                        :RAIN_PROB, :WEATHER_CD, :WEATHER_NM, :SKY_CD,
                        SYSDATE, 'Y', SYSDATE)
        """

        try:
            self.db.execute_many(sql, data)
            self.db.commit()
            self.logger.info(f"TM_WEATHER: {len(data)}건 저장 완료")
            return len(data)
        except Exception as e:
            self.logger.error(f"TM_WEATHER 저장 실패: {e}")
            self.db.rollback()
            raise

    def _save_hourly(self, data: List[Dict]) -> int:
        """시간별 날씨 저장 (TM_WEATHER_HOURLY)"""
        if not data:
            return 0

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
                    BASE_TIME = :BASE_TIME
            WHEN NOT MATCHED THEN
                INSERT (SEQ, WK_DATE, WK_TIME, NX, NY, TEMP, RAIN_PROB, RAIN_AMT,
                        HUMIDITY, WIND_SPEED, WIND_DIR, WEATHER_CD, WEATHER_NM,
                        SKY_CD, PTY_CD, FCST_DT, BASE_DATE, BASE_TIME, LOG_INS_DT)
                VALUES (SEQ_TM_WEATHER_HOURLY.NEXTVAL, :WK_DATE, :WK_TIME, :NX, :NY, :TEMP, :RAIN_PROB, :RAIN_AMT,
                        :HUMIDITY, :WIND_SPEED, :WIND_DIR, :WEATHER_CD, :WEATHER_NM,
                        :SKY_CD, :PTY_CD, SYSDATE, :BASE_DATE, :BASE_TIME, SYSDATE)
        """

        try:
            self.db.execute_many(sql, data)
            self.db.commit()
            self.logger.info(f"TM_WEATHER_HOURLY: {len(data)}건 저장 완료")
            return len(data)
        except Exception as e:
            self.logger.error(f"TM_WEATHER_HOURLY 저장 실패: {e}")
            self.db.rollback()
            raise

    def run(self) -> Dict[str, int]:
        """날씨 수집 실행 (수집 + 저장)"""
        self.logger.info("=== 기상청 날씨 데이터 수집 시작 ===")

        try:
            data = self.collect()
            result = self.save(data)

            self.logger.info(f"=== 수집 완료: 일별 {result['daily']}건, 시간별 {result['hourly']}건 ===")
            return result

        except Exception as e:
            self.logger.error(f"날씨 수집 실패: {e}")
            raise


def update_farm_weather_grid(db: Database):
    """TA_FARM의 WEATHER_NX, WEATHER_NY 업데이트

    MAP_X, MAP_Y가 있고 WEATHER_NX, WEATHER_NY가 없는 농장의 격자 좌표 계산
    """
    logger.info("TA_FARM 격자 좌표 업데이트 시작")

    # 업데이트 대상 조회
    sql = """
        SELECT FARM_NO, MAP_X, MAP_Y
        FROM TA_FARM
        WHERE USE_YN = 'Y'
          AND MAP_X IS NOT NULL
          AND MAP_Y IS NOT NULL
          AND (WEATHER_NX IS NULL OR WEATHER_NY IS NULL)
    """
    rows = db.fetch_dict(sql)

    if not rows:
        logger.info("업데이트 대상 농장 없음")
        return 0

    update_sql = """
        UPDATE TA_FARM
        SET WEATHER_NX = :NX, WEATHER_NY = :NY, LOG_UPT_DT = SYSDATE
        WHERE FARM_NO = :FARM_NO
    """

    updates = []
    for row in rows:
        try:
            lon = float(row['MAP_X'])
            lat = float(row['MAP_Y'])
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
