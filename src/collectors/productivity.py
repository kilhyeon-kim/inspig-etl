"""
생산성 데이터 수집기
- 10.4.35.10:11000 서버의 생산성 API를 통한 데이터 수집
- 농장별 PSY, MSY 등 생산성 지표 조회 및 저장

API 예시:
http://10.4.35.10:11000/statistics/productivity/period/{
    farmNo:1456,
    lang:ko,
    numOfPeriod:1,
    period:Y,
    reportType:2,
    serviceId:01051,
    sizeOfPeriod:1,
    statDate:20251231
}
"""
import logging
import requests
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from .base import BaseCollector
from ..common import Config, Database

logger = logging.getLogger(__name__)


class ProductivityCollector(BaseCollector):
    """생산성 데이터 수집기

    10.4.35.10 서버의 기존 Java API를 호출하여 생산성 데이터를 수집합니다.

    수집 항목:
    - PSY (Pigs Sold per Year)
    - MSY (Marketed per Sow per Year)
    - 분만율, 이유두수, 평균산차 등
    """

    def __init__(self, config: Optional[Config] = None, db: Optional[Database] = None):
        super().__init__(config, db)
        self.api_config = self.config.api
        self.base_url = self.api_config.get('productivity_base_url', 'http://10.4.35.10:11000')
        self.timeout = self.api_config.get('productivity_timeout', 60)

    def _build_api_url(
        self,
        farm_no: int,
        stat_date: str,
        period: str = 'Y',
        num_of_period: int = 1,
        size_of_period: int = 1,
        report_type: int = 2,
        service_id: str = '01051',
        lang: str = 'ko'
    ) -> str:
        """API URL 생성

        Args:
            farm_no: 농장 번호
            stat_date: 기준 날짜 (YYYYMMDD)
            period: 기간 구분 (Y: 년, M: 월, W: 주)
            num_of_period: 기간 수
            size_of_period: 기간 크기
            report_type: 리포트 타입
            service_id: 서비스 ID
            lang: 언어

        Returns:
            완성된 API URL
        """
        # URL 경로 파라미터 구성
        path_params = (
            f"farmNo:{farm_no},"
            f"lang:{lang},"
            f"numOfPeriod:{num_of_period},"
            f"period:{period},"
            f"reportType:{report_type},"
            f"serviceId:{service_id},"
            f"sizeOfPeriod:{size_of_period},"
            f"statDate:{stat_date}"
        )

        url = f"{self.base_url}/statistics/productivity/period/{{{path_params}}}"
        return url

    def _fetch_productivity(self, farm_no: int, stat_date: str, **kwargs) -> Optional[Dict]:
        """생산성 API 호출

        Args:
            farm_no: 농장 번호
            stat_date: 기준 날짜

        Returns:
            생산성 데이터 딕셔너리
        """
        url = self._build_api_url(farm_no, stat_date, **kwargs)

        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            return data

        except requests.RequestException as e:
            self.logger.error(f"생산성 API 호출 실패 (농장 {farm_no}): {e}")
            return None
        except (KeyError, ValueError) as e:
            self.logger.error(f"생산성 API 응답 파싱 실패 (농장 {farm_no}): {e}")
            return None

    def collect(
        self,
        farm_list: Optional[List[Dict]] = None,
        stat_date: Optional[str] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """생산성 데이터 수집

        Args:
            farm_list: 농장 목록 (farm_no 포함)
                      None이면 DB에서 조회
            stat_date: 기준 날짜 (YYYYMMDD)
                      None이면 현재 날짜

        Returns:
            수집된 생산성 데이터 리스트
        """
        if farm_list is None:
            farm_list = self._get_farm_list()

        if not farm_list:
            self.logger.warning("수집 대상 농장이 없습니다.")
            return []

        if stat_date is None:
            stat_date = datetime.now().strftime('%Y%m%d')

        self.logger.info(f"기준 날짜: {stat_date}, 대상 농장: {len(farm_list)}개")

        result = []

        for farm in farm_list:
            farm_no = farm.get('FARM_NO')

            try:
                data = self._fetch_productivity(farm_no, stat_date, **kwargs)

                if data:
                    # API 응답을 DB 저장 형식으로 변환
                    processed = self._process_response(farm_no, stat_date, data)
                    if processed:
                        result.append(processed)
                        self.logger.info(f"농장 {farm_no}: 수집 성공")
                else:
                    self.logger.warning(f"농장 {farm_no}: 데이터 없음")

            except Exception as e:
                self.logger.error(f"농장 {farm_no} 생산성 수집 실패: {e}")
                continue

        return result

    def _get_farm_list(self) -> List[Dict]:
        """DB에서 대상 농장 목록 조회"""
        sql = """
            SELECT DISTINCT F.FARM_NO
            FROM TA_FARM F
            INNER JOIN TS_INS_SERVICE S ON F.FARM_NO = S.FARM_NO
            WHERE F.USE_YN = 'Y'
              AND S.INSPIG_YN = 'Y'
              AND S.USE_YN = 'Y'
              AND S.INSPIG_STOP_DT IS NULL
        """
        return self.db.fetch_dict(sql)

    def _process_response(self, farm_no: int, stat_date: str, data: Dict) -> Optional[Dict]:
        """API 응답을 DB 저장 형식으로 변환

        Args:
            farm_no: 농장 번호
            stat_date: 기준 날짜
            data: API 응답 데이터

        Returns:
            변환된 데이터 딕셔너리
        """
        # TODO: 실제 API 응답 구조에 맞게 구현 필요
        # 현재는 기본 구조만 정의

        try:
            return {
                'FARM_NO': farm_no,
                'STAT_DATE': stat_date,
                'RAW_DATA': str(data),  # 임시: JSON 문자열로 저장
                'INS_DT': datetime.now(),
            }
        except Exception as e:
            self.logger.error(f"응답 변환 실패: {e}")
            return None

    def save(self, data: List[Dict[str, Any]]) -> int:
        """생산성 데이터 저장

        Args:
            data: 저장할 데이터

        Returns:
            저장된 레코드 수
        """
        if not data:
            return 0

        # TODO: 실제 테이블 구조에 맞게 구현 필요
        self.logger.info(f"생산성 데이터 저장: {len(data)}건 (미구현)")

        return len(data)
