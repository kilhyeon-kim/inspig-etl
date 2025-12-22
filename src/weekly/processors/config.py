"""
농장 설정값 저장 프로세서
SP_INS_WEEK_CONFIG 프로시저 Python 전환

역할:
- 농장별 설정값을 TS_INS_WEEK_SUB에 1ROW로 저장
- 다른 프로세서에서 이 값을 조회하여 사용
- 프론트엔드 info-note 헬프 메시지 표시용 데이터 제공
"""
import json
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Any, Dict, Optional

from .base import BaseProcessor

logger = logging.getLogger(__name__)


class ConfigProcessor(BaseProcessor):
    """농장 설정값 저장 프로세서"""

    PROC_NAME = 'ConfigProcessor'

    # 설정 코드 목록 (9개)
    CONFIG_CODES = [
        '140002',  # 평균임신기간 (기본 115)
        '140003',  # 평균포유기간 (기본 21)
        '140004',  # 기준출하체중 (기본 110)
        '140005',  # 기준출하일령 (기본 180)
        '140006',  # 후보돈초발정체크일령 (기본 180)
        '140007',  # 후보돈초교배일령 (기본 240)
        '140008',  # 평균재귀일 (기본 7)
        '140012',  # 기준규격체중 (기본 100)
        '140018',  # 후보돈초교배평균재발정일 (기본 20)
    ]

    # 기본값
    DEFAULTS = {
        '140002': 115,  # 평균임신기간
        '140003': 21,   # 평균포유기간
        '140004': 110,  # 기준출하체중
        '140005': 180,  # 기준출하일령
        '140006': 180,  # 후보돈초발정체크일령
        '140007': 240,  # 후보돈초교배일령
        '140008': 7,    # 평균재귀일
        '140012': 100,  # 기준규격체중
        '140018': 20,   # 후보돈초교배평균재발정일
    }

    def process(self, dt_from: str, dt_to: str, **kwargs) -> Dict[str, Any]:
        """농장 설정값 저장

        Args:
            dt_from: 시작일 (YYYYMMDD) - 사용하지 않음
            dt_to: 종료일 (YYYYMMDD) - 사용하지 않음

        Returns:
            처리 결과 딕셔너리
        """
        self.logger.info(f"농장 설정값 저장 시작: 농장={self.farm_no}")

        # 1. 기존 데이터 삭제
        self._delete_existing()

        # 2. 설정값 조회
        config_values = self._get_config_values()

        # 3. 이유후육성율 계산 (최근 6개월)
        rearing_rate, rate_from, rate_to = self._calculate_rearing_rate(config_values)
        config_values['REARING_RATE'] = rearing_rate

        # 4. JSON 배열 생성 (프론트엔드용)
        codes_json, names_json, values_json = self._build_json_arrays(config_values)

        # 5. SUB 테이블에 저장
        self._insert_config(config_values, codes_json, names_json, values_json, rate_from, rate_to)

        self.logger.info(f"농장 설정값 저장 완료: 농장={self.farm_no}")

        return {
            'status': 'success',
            'config': config_values,
        }

    def _delete_existing(self) -> None:
        """기존 CONFIG 데이터 삭제"""
        sql = """
        DELETE FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq
          AND FARM_NO = :farm_no
          AND GUBUN = 'CONFIG'
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

    def _get_config_values(self) -> Dict[str, Any]:
        """농장 설정값 조회

        TC_CODE_SYS: 시스템 기본값
        TC_FARM_CONFIG: 농장별 설정값 (없으면 시스템 기본값 사용)
        """
        sql = """
        SELECT
            TA1.CODE,
            TA1.CNAME,
            NVL(TA2.CVALUE, TA1.ORIGIN_VALUE) AS CVALUE,
            TA1.SORT_NO
        FROM (
            SELECT T1.CODE, T1.CNAME, T1.CVALUE AS ORIGIN_VALUE, T1.SORT_NO
            FROM TC_CODE_SYS T1
            WHERE T1.PCODE = '14'
              AND T1.CODE IN ('140002', '140003', '140004', '140005', '140006',
                              '140007', '140008', '140012', '140018')
              AND T1.LANGUAGE_CD = 'ko'
              AND T1.USE_YN = 'Y'
        ) TA1
        LEFT OUTER JOIN TC_FARM_CONFIG TA2
            ON TA1.CODE = TA2.CODE
           AND TA2.FARM_NO = :farm_no
           AND TA2.USE_YN = 'Y'
        ORDER BY TA1.SORT_NO
        """

        rows = self.fetch_dict(sql, {'farm_no': self.farm_no})

        # 결과를 딕셔너리로 변환
        config = {}
        for row in rows:
            code = row['CODE']
            try:
                config[code] = int(row['CVALUE']) if row['CVALUE'] else self.DEFAULTS.get(code, 0)
            except (ValueError, TypeError):
                config[code] = self.DEFAULTS.get(code, 0)
            config[f"{code}_NAME"] = row['CNAME']
            config[f"{code}_SORT"] = row['SORT_NO']

        # 누락된 코드는 기본값으로 채움
        for code, default in self.DEFAULTS.items():
            if code not in config:
                config[code] = default

        return config

    def _calculate_rearing_rate(self, config_values: Dict[str, Any]) -> tuple:
        """이유후육성율 계산 (최근 6개월, 당월 제외)

        이유후육성율 = 출하두수 / 이유두수 * 100
        출하일령 Offset = 기준출하일령 - 평균포유기간

        Returns:
            (이유후육성율, 시작월, 종료월)
        """
        ship_day = config_values.get('140005', 180)
        wean_period = config_values.get('140003', 21)
        ship_offset = ship_day - wean_period

        # 기간 계산 (최근 6개월, 당월 제외)
        today = datetime.now()
        month_start = today.replace(day=1)
        rate_from = (month_start - relativedelta(months=6)).strftime('%y.%m')
        rate_to = (month_start - relativedelta(months=1)).strftime('%y.%m')

        # 출하 날짜 범위
        ship_date_from = (month_start - relativedelta(months=6)).strftime('%Y-%m-%d')
        ship_date_to = month_start.strftime('%Y-%m-%d')

        # 이유 날짜 범위 (출하일령 Offset 역산)
        wean_date_from = (month_start - relativedelta(months=6) - relativedelta(days=ship_offset)).strftime('%Y%m%d')
        wean_date_to = (month_start - relativedelta(days=ship_offset)).strftime('%Y%m%d')

        try:
            sql = """
            WITH RAW_DATA AS (
                -- 출하 데이터 집계
                SELECT SUBSTR(REPLACE(L.DOCHUK_DT, '-', ''), 1, 6) AS YM,
                       COUNT(*) AS SHIP_CNT,
                       0 AS WEAN_CNT
                FROM TM_LPD_DATA L
                WHERE L.FARM_NO = :farm_no
                  AND L.USE_YN = 'Y'
                  AND L.DOCHUK_DT >= :ship_date_from
                  AND L.DOCHUK_DT < :ship_date_to
                GROUP BY SUBSTR(REPLACE(L.DOCHUK_DT, '-', ''), 1, 6)
                UNION ALL
                -- 이유 데이터 집계
                SELECT TO_CHAR(TO_DATE(E.WK_DT, 'YYYYMMDD') + :ship_offset, 'YYYYMM') AS YM,
                       0 AS SHIP_CNT,
                       SUM(NVL(E.DUSU, 0) + NVL(E.DUSU_SU, 0)) AS WEAN_CNT
                FROM TB_EU E
                WHERE E.FARM_NO = :farm_no
                  AND E.USE_YN = 'Y'
                  AND E.WK_DT >= :wean_date_from
                  AND E.WK_DT < :wean_date_to
                GROUP BY TO_CHAR(TO_DATE(E.WK_DT, 'YYYYMMDD') + :ship_offset, 'YYYYMM')
            ),
            MONTHLY_STATS AS (
                SELECT YM,
                       SUM(SHIP_CNT) AS SHIP_CNT,
                       SUM(WEAN_CNT) AS WEAN_CNT
                FROM RAW_DATA
                GROUP BY YM
            )
            SELECT ROUND(AVG(CASE WHEN WEAN_CNT > 0 THEN SHIP_CNT / WEAN_CNT * 100 END), 1) AS RATE
            FROM MONTHLY_STATS
            """

            result = self.fetch_one(sql, {
                'farm_no': self.farm_no,
                'ship_date_from': ship_date_from,
                'ship_date_to': ship_date_to,
                'ship_offset': ship_offset,
                'wean_date_from': wean_date_from,
                'wean_date_to': wean_date_to,
            })

            rearing_rate = result[0] if result and result[0] else 85.0

        except Exception as e:
            self.logger.warning(f"이유후육성율 계산 실패: {e}")
            rearing_rate = 85.0

        return rearing_rate, rate_from, rate_to

    def _build_json_arrays(self, config_values: Dict[str, Any]) -> tuple:
        """프론트엔드용 JSON 배열 생성

        Returns:
            (codes_json, names_json, values_json)
        """
        codes = []
        names = []
        values = []

        # 정렬 순서대로 처리
        sorted_codes = sorted(
            [(code, config_values.get(f"{code}_SORT", 999)) for code in self.CONFIG_CODES],
            key=lambda x: x[1]
        )

        for code, _ in sorted_codes:
            codes.append(code)
            names.append(config_values.get(f"{code}_NAME", code))
            values.append(config_values.get(code, self.DEFAULTS.get(code, 0)))

        # 이유후육성율 추가
        codes.append('REARING_RATE')
        names.append('이유후육성율(6개월)')
        values.append(config_values.get('REARING_RATE', 85))

        return json.dumps(codes), json.dumps(names, ensure_ascii=False), json.dumps(values)

    def _insert_config(
        self,
        config_values: Dict[str, Any],
        codes_json: str,
        names_json: str,
        values_json: str,
        rate_from: str,
        rate_to: str,
    ) -> None:
        """CONFIG 데이터 INSERT"""
        sql = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SORT_NO,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, VAL_1,
            STR_1, STR_2, STR_3, STR_4, STR_5
        ) VALUES (
            :master_seq, :farm_no, 'CONFIG', 1,
            :preg_period, :wean_period, :ship_day, :first_gb_day, :avg_return, :rearing_rate,
            :codes_json, :names_json, :values_json, :rate_from, :rate_to
        )
        """

        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'preg_period': config_values.get('140002', 115),
            'wean_period': config_values.get('140003', 21),
            'ship_day': config_values.get('140005', 180),
            'first_gb_day': config_values.get('140007', 240),
            'avg_return': config_values.get('140008', 7),
            'rearing_rate': config_values.get('REARING_RATE', 85),
            'codes_json': codes_json,
            'names_json': names_json,
            'values_json': values_json,
            'rate_from': rate_from,
            'rate_to': rate_to,
        })

    def get_config(self) -> Dict[str, Any]:
        """저장된 설정값 조회 (다른 프로세서에서 사용)

        Returns:
            설정값 딕셔너리
        """
        sql = """
        SELECT CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, VAL_1
        FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq
          AND FARM_NO = :farm_no
          AND GUBUN = 'CONFIG'
        """

        result = self.fetch_one(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

        if result:
            return {
                'preg_period': result[0] or 115,      # 평균임신기간
                'wean_period': result[1] or 21,       # 평균포유기간
                'ship_day': result[2] or 180,         # 기준출하일령
                'first_gb_day': result[3] or 240,     # 후보돈초교배일령
                'avg_return': result[4] or 7,         # 평균재귀일
                'rearing_rate': result[5] or 85,      # 이유후육성율
            }
        else:
            return {
                'preg_period': 115,
                'wean_period': 21,
                'ship_day': 180,
                'first_gb_day': 240,
                'avg_return': 7,
                'rearing_rate': 85,
            }
