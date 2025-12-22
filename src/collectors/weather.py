"""
기상청 데이터 수집기
- 기상청 단기예보 API를 통한 날씨 데이터 수집
- 농장별 위치 기반 날씨 정보 조회 및 저장
"""
import logging
import requests
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .base import BaseCollector
from ..common import Config, Database

logger = logging.getLogger(__name__)


class WeatherCollector(BaseCollector):
    """기상청 날씨 데이터 수집기

    기상청 단기예보 API를 사용하여 농장별 날씨 데이터를 수집합니다.

    API 문서:
    - https://www.data.go.kr/data/15084084/openapi.do

    수집 항목:
    - 기온 (TMP)
    - 습도 (REH)
    - 강수량 (PCP)
    - 풍속 (WSD)
    """

    # 기상청 API 카테고리 코드
    CATEGORIES = {
        'TMP': '기온',
        'REH': '습도',
        'PCP': '1시간 강수량',
        'WSD': '풍속',
        'SKY': '하늘상태',
        'PTY': '강수형태',
    }

    def __init__(self, config: Optional[Config] = None, db: Optional[Database] = None):
        super().__init__(config, db)
        self.weather_config = self.config.weather
        self.api_key = self.weather_config.get('api_key', '')
        self.base_url = self.weather_config.get('base_url', '')

    def _get_base_datetime(self) -> tuple:
        """API 호출용 기준 날짜/시간 계산

        기상청 단기예보 API는 02:00부터 3시간 간격으로 발표
        발표시간: 02, 05, 08, 11, 14, 17, 20, 23시

        Returns:
            (base_date, base_time) 튜플
        """
        now = datetime.now()
        # 발표 시간 목록
        announce_hours = [2, 5, 8, 11, 14, 17, 20, 23]

        # 현재 시간에서 가장 가까운 이전 발표 시간 찾기
        current_hour = now.hour
        base_hour = max([h for h in announce_hours if h <= current_hour], default=23)

        if base_hour == 23 and current_hour < 23:
            # 자정~02시 사이면 전날 23시
            base_date = (now - timedelta(days=1)).strftime('%Y%m%d')
        else:
            base_date = now.strftime('%Y%m%d')

        base_time = f"{base_hour:02d}00"

        return base_date, base_time

    def _convert_grid(self, lat: float, lon: float) -> tuple:
        """위경도를 기상청 격자 좌표로 변환

        기상청 격자 좌표 변환 공식 (LCC 투영법)

        Args:
            lat: 위도
            lon: 경도

        Returns:
            (nx, ny) 격자 좌표 튜플
        """
        import math

        # 기상청 격자 변환 상수
        RE = 6371.00877  # 지구 반경(km)
        GRID = 5.0       # 격자 간격(km)
        SLAT1 = 30.0     # 투영 위도1
        SLAT2 = 60.0     # 투영 위도2
        OLON = 126.0     # 기준점 경도
        OLAT = 38.0      # 기준점 위도
        XO = 43          # 기준점 X좌표
        YO = 136         # 기준점 Y좌표

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

    def _fetch_weather(self, nx: int, ny: int, base_date: str, base_time: str) -> List[Dict]:
        """기상청 API 호출

        Args:
            nx: 격자 X 좌표
            ny: 격자 Y 좌표
            base_date: 기준 날짜 (YYYYMMDD)
            base_time: 기준 시간 (HHMM)

        Returns:
            날씨 데이터 리스트
        """
        if not self.api_key:
            self.logger.warning("기상청 API 키가 설정되지 않았습니다.")
            return []

        url = f"{self.base_url}/getVilageFcst"
        params = {
            'serviceKey': self.api_key,
            'pageNo': 1,
            'numOfRows': 1000,
            'dataType': 'JSON',
            'base_date': base_date,
            'base_time': base_time,
            'nx': nx,
            'ny': ny,
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])

            return items

        except requests.RequestException as e:
            self.logger.error(f"기상청 API 호출 실패: {e}")
            return []
        except (KeyError, ValueError) as e:
            self.logger.error(f"기상청 API 응답 파싱 실패: {e}")
            return []

    def collect(self, farm_list: Optional[List[Dict]] = None, **kwargs) -> List[Dict[str, Any]]:
        """날씨 데이터 수집

        Args:
            farm_list: 농장 목록 (farm_no, lat, lon 포함)
                      None이면 DB에서 조회

        Returns:
            수집된 날씨 데이터 리스트
        """
        if farm_list is None:
            # DB에서 농장 목록 조회
            farm_list = self._get_farm_list()

        if not farm_list:
            self.logger.warning("수집 대상 농장이 없습니다.")
            return []

        base_date, base_time = self._get_base_datetime()
        self.logger.info(f"기준 날짜/시간: {base_date} {base_time}")

        result = []

        for farm in farm_list:
            farm_no = farm.get('FARM_NO')
            lat = farm.get('LAT')
            lon = farm.get('LON')

            if not lat or not lon:
                self.logger.warning(f"농장 {farm_no}: 위경도 정보 없음")
                continue

            try:
                nx, ny = self._convert_grid(float(lat), float(lon))
                weather_items = self._fetch_weather(nx, ny, base_date, base_time)

                for item in weather_items:
                    category = item.get('category')
                    if category in self.CATEGORIES:
                        result.append({
                            'FARM_NO': farm_no,
                            'BASE_DATE': base_date,
                            'BASE_TIME': base_time,
                            'FCST_DATE': item.get('fcstDate'),
                            'FCST_TIME': item.get('fcstTime'),
                            'CATEGORY': category,
                            'CATEGORY_NM': self.CATEGORIES[category],
                            'FCST_VALUE': item.get('fcstValue'),
                            'NX': nx,
                            'NY': ny,
                        })

                self.logger.info(f"농장 {farm_no}: {len(weather_items)}건 수집")

            except Exception as e:
                self.logger.error(f"농장 {farm_no} 날씨 수집 실패: {e}")
                continue

        return result

    def _get_farm_list(self) -> List[Dict]:
        """DB에서 농장 목록 조회"""
        sql = """
            SELECT FARM_NO, LAT, LON
            FROM TA_FARM
            WHERE USE_YN = 'Y'
              AND LAT IS NOT NULL
              AND LON IS NOT NULL
        """
        return self.db.fetch_dict(sql)

    def save(self, data: List[Dict[str, Any]]) -> int:
        """날씨 데이터 저장

        Args:
            data: 저장할 날씨 데이터

        Returns:
            저장된 레코드 수
        """
        if not data:
            return 0

        # MERGE 문으로 중복 방지
        sql = """
            MERGE INTO TS_WEATHER TGT
            USING (
                SELECT :FARM_NO AS FARM_NO,
                       :BASE_DATE AS BASE_DATE,
                       :BASE_TIME AS BASE_TIME,
                       :FCST_DATE AS FCST_DATE,
                       :FCST_TIME AS FCST_TIME,
                       :CATEGORY AS CATEGORY
                FROM DUAL
            ) SRC
            ON (TGT.FARM_NO = SRC.FARM_NO
                AND TGT.BASE_DATE = SRC.BASE_DATE
                AND TGT.BASE_TIME = SRC.BASE_TIME
                AND TGT.FCST_DATE = SRC.FCST_DATE
                AND TGT.FCST_TIME = SRC.FCST_TIME
                AND TGT.CATEGORY = SRC.CATEGORY)
            WHEN MATCHED THEN
                UPDATE SET
                    FCST_VALUE = :FCST_VALUE,
                    UPD_DT = SYSDATE
            WHEN NOT MATCHED THEN
                INSERT (FARM_NO, BASE_DATE, BASE_TIME, FCST_DATE, FCST_TIME,
                        CATEGORY, CATEGORY_NM, FCST_VALUE, NX, NY, INS_DT)
                VALUES (:FARM_NO, :BASE_DATE, :BASE_TIME, :FCST_DATE, :FCST_TIME,
                        :CATEGORY, :CATEGORY_NM, :FCST_VALUE, :NX, :NY, SYSDATE)
        """

        try:
            self.db.execute_many(sql, data)
            return len(data)
        except Exception as e:
            self.logger.error(f"날씨 데이터 저장 실패: {e}")
            raise
