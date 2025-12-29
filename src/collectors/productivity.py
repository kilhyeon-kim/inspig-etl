"""
생산성 데이터 수집기
- 10.4.35.10:11000 서버의 생산성 API를 통한 데이터 수집
- 농장별 PSY, MSY 등 생산성 지표 조회 및 저장

API 호출 형식:
GET http://10.4.35.10:11000/statistics/productivity/period/{farmNo}
    ?statDate=YYYY-MM-DD      # 가변 (기준일)
    &memberId=null            # 가변 (회원ID, 기본값 null)
    &lang=ko                  # 고정
    &serviceId=01051          # 고정
    &period=W/M               # 가변 (W:주간, M:월간)
    &sizeOfPeriod=1           # 고정
    &numOfPeriod=1            # 고정
    &pumjongCd=- 전체 -       # 고정
    &reportType=1             # 고정

응답 구조:
{
    "data": [
        {
            "__INDEX__": "2024-12-23",
            "__STATCD__": "035001",
            "__PCODE__": "035",
            "__PCNAME__": "생산회전율",
            "__STATNM__": "연초모돈수",
            "__VAL__": "554.55",
            "__TOOLTIP__": "설명"
        },
        ...
    ]
}

테이블 구조 (TS_PRODUCTIVITY):
    - 1개 테이블에 PCODE로 구분
    - UK: FARM_NO + PCODE + STAT_YEAR + PERIOD + PERIOD_NO
    - 컬럼: C + 뒤3자리 (예: 031001 -> C001)

사용 예시:
    # 주간 데이터 수집 (2024년 52주차)
    collector = ProductivityCollector()
    data = collector.collect(period='W', stat_date='20241223')
    collector.save(data)

    # 월간 데이터 수집 (2024년 12월)
    data = collector.collect(period='M', stat_date='20241223')
    collector.save(data)
"""
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

from .base import BaseCollector
from ..common import Config, Database, today_kst

logger = logging.getLogger(__name__)


class ProductivityCollector(BaseCollector):
    """생산성 데이터 수집기

    10.4.35.10 서버의 기존 Java API를 호출하여 생산성 데이터를 수집합니다.

    수집 항목:
    - PSY (Pigs Sold per Year) - 034023
    - MSY (Marketed per Sow per Year) - 034029
    - 분만율, 이유두수, 평균산차 등

    테이블 구조:
    - TS_PRODUCTIVITY: 1개 테이블에 PCODE로 구분
    - UK: FARM_NO + PCODE + STAT_YEAR + PERIOD + PERIOD_NO
    """

    def __init__(self, config: Optional[Config] = None, db: Optional[Database] = None):
        super().__init__(config, db)
        self.api_config = self.config.api
        self.base_url = self.api_config.get('productivity_base_url', 'http://10.4.35.10:11000')
        self.timeout = self.api_config.get('productivity_timeout', 60)
        self.max_workers = self.api_config.get('productivity_workers', 4)

    # 고정 파라미터 (statDate, memberId, period 외 모두 고정)
    FIXED_PARAMS = {
        'lang': 'ko',
        'serviceId': '01051',
        'sizeOfPeriod': '1',
        'numOfPeriod': '1',
        'pumjongCd': '- 전체 -',
        'reportType': '1',
    }

    # 지원하는 기간 구분
    VALID_PERIODS = ('W', 'M', 'Q')  # W:주간, M:월간, Q:분기

    # PCODE 목록
    VALID_PCODES = ('031', '032', '033', '034', '035')

    def _fetch_productivity(
        self,
        farm_no: int,
        stat_date: str,
        period: str = 'W',
        member_id: str = 'null',
    ) -> Optional[Dict]:
        """생산성 API 호출

        Args:
            farm_no: 농장 번호
            stat_date: 기준 날짜 (YYYYMMDD 또는 YYYY-MM-DD)
            period: 기간 구분 (W:주간, M:월간)
            member_id: 회원 ID (기본값 'null')

        Returns:
            생산성 데이터 딕셔너리

        Note:
            API 파라미터 중 statDate, memberId, period 외에는 모두 고정값 사용
        """
        # stat_date를 YYYY-MM-DD 형식으로 변환
        if len(stat_date) == 8:
            stat_date_formatted = f"{stat_date[:4]}-{stat_date[4:6]}-{stat_date[6:8]}"
        else:
            stat_date_formatted = stat_date

        url = f"{self.base_url}/statistics/productivity/period/{farm_no}"
        params = {
            'statDate': stat_date_formatted,
            'memberId': member_id,
            'period': period,
            **self.FIXED_PARAMS,
        }

        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            return data

        except requests.RequestException as e:
            self.logger.error(f"생산성 API 호출 실패 (농장 {farm_no}): {e}")
            return None
        except (KeyError, ValueError) as e:
            self.logger.error(f"생산성 API 응답 파싱 실패 (농장 {farm_no}): {e}")
            return None

    def _calculate_period_info(self, stat_date: str, period: str) -> Dict[str, int]:
        """기준일로부터 년도, 기간차수 계산

        Args:
            stat_date: 기준 날짜 (YYYYMMDD 또는 YYYY-MM-DD)
            period: 기간 구분 (W:주간, M:월간, Q:분기)

        Returns:
            {'stat_year': 2024, 'period_no': 52}
        """
        # stat_date를 datetime으로 변환
        if len(stat_date) == 8:
            dt = datetime.strptime(stat_date, '%Y%m%d')
        else:
            dt = datetime.strptime(stat_date, '%Y-%m-%d')

        stat_year = dt.year

        if period == 'W':
            # ISO 주차 (1~53)
            period_no = dt.isocalendar()[1]
        elif period == 'M':
            # 월 (1~12)
            period_no = dt.month
        elif period == 'Q':
            # 분기 (1~4)
            period_no = (dt.month - 1) // 3 + 1
        else:
            period_no = 1

        return {'stat_year': stat_year, 'period_no': period_no}

    def collect(
        self,
        farm_list: Optional[List[Dict]] = None,
        stat_date: Optional[str] = None,
        period: str = 'W',
        **kwargs
    ) -> List[Dict[str, Any]]:
        """생산성 데이터 수집

        Args:
            farm_list: 농장 목록 (FARM_NO 포함)
                      None이면 DB에서 조회
            stat_date: 기준 날짜 (YYYYMMDD)
                      None이면 현재 날짜
            period: 기간 구분 (W:주간, M:월간, Q:분기)
                   기본값 'W' (주간)

        Returns:
            수집된 데이터 리스트
            [
                {'FARM_NO': 1234, 'PCODE': '031', 'STAT_YEAR': 2024, 'PERIOD': 'W', 'PERIOD_NO': 52, ...},
                {'FARM_NO': 1234, 'PCODE': '032', ...},
                ...
            ]

        Raises:
            ValueError: 잘못된 period 값
        """
        # period 유효성 검사
        if period not in self.VALID_PERIODS:
            raise ValueError(f"잘못된 period 값: {period}. 허용값: {self.VALID_PERIODS}")

        if farm_list is None:
            farm_list = self._get_farm_list()

        if not farm_list:
            self.logger.warning("수집 대상 농장이 없습니다.")
            return []

        if stat_date is None:
            stat_date = today_kst()  # 한국 시간 기준

        # 년도, 기간차수 계산
        period_info = self._calculate_period_info(stat_date, period)

        period_name = {'W': '주간', 'M': '월간', 'Q': '분기'}.get(period, period)
        self.logger.info(
            f"기준 날짜: {stat_date}, 대상 농장: {len(farm_list)}개, "
            f"기간: {period_name}({period}), "
            f"년도: {period_info['stat_year']}, 차수: {period_info['period_no']}"
        )

        # 결과 저장
        result = []
        success_cnt = 0
        error_cnt = 0

        def fetch_single_farm(farm: Dict) -> List[Dict]:
            """단일 농장 데이터 수집"""
            farm_no = farm.get('FARM_NO')
            try:
                # API 호출 (Q는 API에서 지원하지 않으므로 M으로 호출)
                api_period = 'M' if period == 'Q' else period
                data = self._fetch_productivity(farm_no, stat_date, api_period)
                if data and 'data' in data:
                    return self._process_response(farm_no, stat_date, period, period_info, data)
                return []
            except Exception as e:
                self.logger.error(f"농장 {farm_no} 생산성 수집 실패: {e}")
                return []

        # 병렬 처리로 API 호출
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_farm = {
                executor.submit(fetch_single_farm, farm): farm
                for farm in farm_list
            }

            for future in as_completed(future_to_farm):
                farm = future_to_farm[future]
                farm_no = farm.get('FARM_NO')

                try:
                    farm_data = future.result()
                    if farm_data:
                        result.extend(farm_data)
                        success_cnt += 1
                        self.logger.debug(f"농장 {farm_no}: 수집 완료 ({len(farm_data)}건)")
                    else:
                        error_cnt += 1
                        self.logger.warning(f"농장 {farm_no}: 데이터 없음")
                except Exception as e:
                    error_cnt += 1
                    self.logger.error(f"농장 {farm_no} 처리 예외: {e}")

        self.logger.info(f"수집 완료: 성공 {success_cnt}개, 실패 {error_cnt}개, 총 레코드 {len(result)}건")
        return result

    def _get_farm_list(self) -> List[Dict]:
        """DB에서 대상 농장 목록 조회

        TS_INS_SERVICE 필터링 조건 (orchestrator._get_target_farms와 동일):
        - INSPIG_YN = 'Y': 인사이트피그 서비스 사용
        - INSPIG_FROM_DT <= SYSDATE: 서비스 시작일 이후
        - SYSDATE <= LEAST(INSPIG_TO_DT, INSPIG_STOP_DT): 종료일/중지일 중 빠른 날짜 이전
        - INSPIG_STOP_DT 기본값: 9999-12-31 (NULL이면 중지 안됨)
        """
        sql = """
            SELECT DISTINCT F.FARM_NO
            FROM TA_FARM F
            INNER JOIN TS_INS_SERVICE S ON F.FARM_NO = S.FARM_NO
            WHERE F.USE_YN = 'Y'
              AND S.INSPIG_YN = 'Y'
              AND S.USE_YN = 'Y'
              AND S.INSPIG_FROM_DT IS NOT NULL
              AND S.INSPIG_TO_DT IS NOT NULL
              AND TO_CHAR(SYSDATE, 'YYYYMMDD') >= S.INSPIG_FROM_DT
              AND TO_CHAR(SYSDATE, 'YYYYMMDD') <= LEAST(
                  S.INSPIG_TO_DT,
                  NVL(S.INSPIG_STOP_DT, '99991231')
              )
            ORDER BY F.FARM_NO
        """
        return self.db.fetch_dict(sql)

    def _process_response(
        self, farm_no: int, stat_date: str, period: str, period_info: Dict, data: Dict
    ) -> List[Dict]:
        """API 응답을 PCODE별 Row 형식으로 변환

        Args:
            farm_no: 농장 번호
            stat_date: 기준 날짜 (YYYYMMDD)
            period: 기간 구분
            period_info: {'stat_year': 2024, 'period_no': 52}
            data: API 응답 데이터

        Returns:
            PCODE별 Row 리스트
            [
                {'FARM_NO': 1234, 'PCODE': '031', 'STAT_YEAR': 2024, 'PERIOD': 'W', 'PERIOD_NO': 52, 'C001': 10.5, ...},
                {'FARM_NO': 1234, 'PCODE': '032', ...},
                ...
            ]
        """
        # stat_date를 YYYY-MM-DD 형식으로 변환
        if len(stat_date) == 8:
            stat_date_formatted = f"{stat_date[:4]}-{stat_date[4:6]}-{stat_date[6:8]}"
        else:
            stat_date_formatted = stat_date

        # PCODE별 Row 초기화
        rows = {}
        for pcode in self.VALID_PCODES:
            rows[pcode] = {
                'FARM_NO': farm_no,
                'PCODE': pcode,
                'STAT_YEAR': period_info['stat_year'],
                'PERIOD': period,
                'PERIOD_NO': period_info['period_no'],
                'STAT_DATE': stat_date_formatted,
            }

        items = data.get('data', [])

        for item in items:
            try:
                stat_cd = item.get('__STATCD__')
                if not stat_cd or len(stat_cd) != 6:
                    continue

                pcode = stat_cd[:3]  # 앞 3자리: PCODE
                col_suffix = stat_cd[3:]  # 뒤 3자리: 컬럼 suffix

                if pcode not in rows:
                    continue

                # 컬럼명: C + 뒤 3자리 (예: 031001 -> C001)
                col_name = f"C{col_suffix}"

                # 값 변환
                stat_val = item.get('__VAL__')
                if stat_val is not None and str(stat_val).strip():
                    try:
                        stat_val = float(stat_val)
                    except (ValueError, TypeError):
                        stat_val = None

                rows[pcode][col_name] = stat_val

            except Exception as e:
                self.logger.warning(f"항목 변환 실패: {e}, item={item}")
                continue

        # 데이터가 있는 PCODE만 반환 (기본 필드 외에 C??? 컬럼이 있는 경우)
        result = []
        for pcode, row in rows.items():
            c_columns = [k for k in row.keys() if k.startswith('C')]
            if c_columns:
                result.append(row)

        return result

    def save(self, data: List[Dict[str, Any]]) -> int:
        """생산성 데이터 저장 (DELETE 후 INSERT)

        기존 데이터가 존재하면 삭제 후 새로 생성합니다.
        UK: FARM_NO + PCODE + STAT_YEAR + PERIOD + PERIOD_NO

        Args:
            data: 수집된 데이터 리스트

        Returns:
            저장된 총 레코드 수
        """
        if not data:
            return 0

        # 모든 row의 C 컬럼을 합집합으로 수집
        all_c_columns = set()
        for row in data:
            c_cols = [col for col in row.keys() if col.startswith('C')]
            all_c_columns.update(c_cols)

        c_columns = sorted(all_c_columns)

        # INSERT 컬럼/값 생성
        base_cols = ['SEQ', 'FARM_NO', 'PCODE', 'STAT_YEAR', 'PERIOD', 'PERIOD_NO', 'STAT_DATE']
        insert_cols = base_cols + c_columns + ['INS_DT']

        insert_vals = ['SEQ_TS_PRODUCTIVITY.NEXTVAL', ':FARM_NO', ':PCODE', ':STAT_YEAR', ':PERIOD', ':PERIOD_NO', ':STAT_DATE']
        insert_vals += [f':{col}' for col in c_columns]
        insert_vals += ['SYSDATE']

        insert_sql = f"""
            INSERT INTO TS_PRODUCTIVITY ({', '.join(insert_cols)})
            VALUES ({', '.join(insert_vals)})
        """

        delete_sql = """
            DELETE FROM TS_PRODUCTIVITY
            WHERE FARM_NO = :FARM_NO
              AND PCODE = :PCODE
              AND STAT_YEAR = :STAT_YEAR
              AND PERIOD = :PERIOD
              AND PERIOD_NO = :PERIOD_NO
        """

        # 모든 row에 누락된 C 컬럼을 None으로 채움
        for row in data:
            for col in c_columns:
                if col not in row:
                    row[col] = None

        try:
            # 1. 기존 데이터 삭제
            delete_count = 0
            for row in data:
                delete_params = {
                    'FARM_NO': row['FARM_NO'],
                    'PCODE': row['PCODE'],
                    'STAT_YEAR': row['STAT_YEAR'],
                    'PERIOD': row['PERIOD'],
                    'PERIOD_NO': row['PERIOD_NO'],
                }
                result = self.db.execute(delete_sql, delete_params)
                if result:
                    delete_count += result

            if delete_count > 0:
                self.logger.info(f"TS_PRODUCTIVITY 기존 데이터 삭제: {delete_count}건")

            # 2. 새 데이터 INSERT
            self.db.execute_many(insert_sql, data)
            self.logger.info(f"TS_PRODUCTIVITY 저장 완료: {len(data)}건")
            return len(data)

        except Exception as e:
            self.logger.error(f"TS_PRODUCTIVITY 저장 실패: {e}")
            raise

    def update_ins_week_sangsi(self, stat_year: int, period: str, period_no: int) -> int:
        """TS_PRODUCTIVITY의 상시모돈수를 TS_INS_WEEK에 업데이트

        방식 2: ETL에서 TS_PRODUCTIVITY → TS_INS_WEEK.MODON_SANGSI_CNT 경유

        Args:
            stat_year: 통계년도
            period: 기간구분 (W:주간, M:월간, Q:분기)
            period_no: 기간차수 (W:1~53, M:1~12, Q:1~4)

        Returns:
            업데이트된 레코드 수
        """
        # period → DAY_GB 변환: W→WEEK, M→MON, Q→QT
        day_gb_map = {'W': 'WEEK', 'M': 'MON', 'Q': 'QT'}
        day_gb = day_gb_map.get(period, 'WEEK')

        update_sql = """
            UPDATE TS_INS_WEEK W
            SET W.MODON_SANGSI_CNT = (
                SELECT NVL(P.C001, 0)
                FROM TS_PRODUCTIVITY P
                WHERE P.FARM_NO = W.FARM_NO
                  AND P.PCODE = '035'
                  AND P.STAT_YEAR = :stat_year
                  AND P.PERIOD = :period
                  AND P.PERIOD_NO = :period_no
            )
            WHERE W.REPORT_YEAR = :stat_year
              AND W.REPORT_WEEK_NO = :period_no
              AND EXISTS (
                SELECT 1 FROM TS_INS_MASTER M
                WHERE M.SEQ = W.MASTER_SEQ
                  AND M.DAY_GB = :day_gb
              )
        """

        try:
            params = {
                'stat_year': stat_year,
                'period': period,
                'period_no': period_no,
                'day_gb': day_gb
            }
            result = self.db.execute(update_sql, params)
            self.logger.info(
                f"TS_INS_WEEK.MODON_SANGSI_CNT 업데이트 완료: "
                f"{stat_year}년 {period}{period_no}, {result}건"
            )
            return result if result else 0

        except Exception as e:
            self.logger.error(f"TS_INS_WEEK.MODON_SANGSI_CNT 업데이트 실패: {e}")
            raise
