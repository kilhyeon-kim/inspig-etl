"""
관리대상 모돈 추출 프로세서
SP_INS_WEEK_ALERT_POPUP 프로시저 Python 전환

역할:
- 5개 유형의 관리대상 모돈 추출
  1. 미교배 후보돈 (HUBO)
  2. 이유후 미교배 (EU_MI)
  3. 사고후 미교배 (SG_MI)
  4. 분만지연 (BM_DELAY)
  5. 이유지연 (EU_DELAY)
- TS_INS_WEEK_SUB (GUBUN='ALERT') 저장
"""
import logging
from typing import Any, Dict

from .base import BaseProcessor

logger = logging.getLogger(__name__)


class AlertProcessor(BaseProcessor):
    """관리대상 모돈 추출 프로세서"""

    PROC_NAME = 'AlertProcessor'

    def process(self, dt_from: str, dt_to: str, **kwargs) -> Dict[str, Any]:
        """관리대상 모돈 데이터 추출

        Args:
            dt_from: 시작일 (YYYYMMDD) - 사용하지 않음
            dt_to: 종료일 (YYYYMMDD) - 기준일

        Returns:
            처리 결과 딕셔너리
        """
        self.logger.info(f"관리대상 모돈 추출 시작: 농장={self.farm_no}")

        # 1. 농장 설정값 조회
        config = self._get_config()

        # 2. 기존 데이터 삭제
        self._delete_existing()

        # 3. 통합 쿼리로 데이터 INSERT
        proc_cnt = self._insert_alert_data(dt_to, config)

        # 4. TS_INS_WEEK 요약 업데이트
        self._update_week_summary()

        self.logger.info(f"관리대상 모돈 추출 완료: 농장={self.farm_no}, 처리={proc_cnt}건")

        return {
            'status': 'success',
            'proc_cnt': proc_cnt,
        }

    def _get_config(self) -> Dict[str, int]:
        """농장 설정값 조회 (CONFIG에서 저장한 값)"""
        sql = """
        SELECT NVL(CNT_1, 115),
               NVL(CNT_2, 21),
               NVL(CNT_4, 240),
               NVL(CNT_5, 7)
        FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq
          AND FARM_NO = :farm_no
          AND GUBUN = 'CONFIG'
        """
        result = self.fetch_one(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

        if result:
            return {
                'preg_period': result[0],   # 평균임신기간
                'wean_period': result[1],   # 평균포유기간
                'first_gb_day': result[2],  # 후보돈초교배일령
                'avg_return': result[3],    # 평균재귀일
            }
        return {
            'preg_period': 115,
            'wean_period': 21,
            'first_gb_day': 240,
            'avg_return': 7,
        }

    def _delete_existing(self) -> None:
        """기존 ALERT 데이터 삭제"""
        sql = """
        DELETE FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no AND GUBUN = 'ALERT'
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

    def _insert_alert_data(self, dt_to: str, config: Dict[str, int]) -> int:
        """5개 유형 × 4개 구간 데이터 INSERT"""
        sql = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SORT_NO, CODE_1,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5
        )
        WITH
        LAST_WK AS (
            SELECT FARM_NO, PIG_NO, MAX(SEQ) AS MSEQ
            FROM TB_MODON_WK
            WHERE FARM_NO = :farm_no AND USE_YN = 'Y'
            GROUP BY FARM_NO, PIG_NO
        ),
        NO_WK_MODON AS (
            SELECT MD.FARM_NO, MD.PIG_NO
            FROM TB_MODON MD
            LEFT OUTER JOIN (
                SELECT DISTINCT FARM_NO, PIG_NO FROM TB_MODON_WK WHERE FARM_NO = :farm_no AND USE_YN = 'Y'
            ) WK ON WK.FARM_NO = MD.FARM_NO AND WK.PIG_NO = MD.PIG_NO
            WHERE MD.FARM_NO = :farm_no
              AND MD.USE_YN = 'Y'
              AND MD.OUT_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD')
              AND WK.PIG_NO IS NULL
        ),
        HUBO AS (
            SELECT (TO_DATE(:base_dt, 'YYYYMMDD') - MD.BIRTH_DT) - :first_gb_day AS DELAY_DAYS
            FROM TB_MODON MD
            INNER JOIN NO_WK_MODON NW ON NW.FARM_NO = MD.FARM_NO AND NW.PIG_NO = MD.PIG_NO
            WHERE MD.IN_DT < TO_DATE(:base_dt, 'YYYYMMDD') + 1
              AND (TO_DATE(:base_dt, 'YYYYMMDD') - MD.BIRTH_DT) - :first_gb_day >= 0
              AND (MD.STATUS_CD = '010001' OR (MD.STATUS_CD = '010002' AND MD.IN_SANCHA = 0 AND MD.IN_GYOBAE_CNT = 1))
        ),
        EU_MI AS (
            SELECT (TO_DATE(:base_dt, 'YYYYMMDD') - WK.WK_DATE) - :avg_return + 1 AS DELAY_DAYS
            FROM TB_MODON MD
            JOIN LAST_WK LW ON LW.FARM_NO = MD.FARM_NO AND LW.PIG_NO = MD.PIG_NO
            JOIN TB_MODON_WK WK ON WK.FARM_NO = LW.FARM_NO AND WK.PIG_NO = LW.PIG_NO AND WK.SEQ = LW.MSEQ
            WHERE MD.FARM_NO = :farm_no
              AND MD.OUT_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD') AND MD.USE_YN = 'Y'
              AND WK.WK_GUBUN = 'E' AND WK.DAERI_YN = 'N'
              AND (TO_DATE(:base_dt, 'YYYYMMDD') - WK.WK_DATE) + 1 >= :avg_return
            UNION ALL
            SELECT (TO_DATE(:base_dt, 'YYYYMMDD') - MD.LAST_WK_DT) - :avg_return AS DELAY_DAYS
            FROM TB_MODON MD
            INNER JOIN NO_WK_MODON NW ON NW.FARM_NO = MD.FARM_NO AND NW.PIG_NO = MD.PIG_NO
            WHERE MD.STATUS_CD = '010005'
              AND MD.IN_DT < TO_DATE(:base_dt, 'YYYYMMDD') + 1
              AND (TO_DATE(:base_dt, 'YYYYMMDD') - MD.LAST_WK_DT) - :avg_return >= 0
        ),
        SG_MI AS (
            SELECT (TO_DATE(:base_dt, 'YYYYMMDD') - WK.WK_DATE) AS DELAY_DAYS
            FROM TB_MODON MD
            JOIN LAST_WK LW ON LW.FARM_NO = MD.FARM_NO AND LW.PIG_NO = MD.PIG_NO
            JOIN TB_MODON_WK WK ON WK.FARM_NO = LW.FARM_NO AND WK.PIG_NO = LW.PIG_NO AND WK.SEQ = LW.MSEQ
            WHERE MD.FARM_NO = :farm_no
              AND MD.OUT_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD') AND MD.USE_YN = 'Y'
              AND WK.WK_GUBUN = 'F'
              AND (TO_DATE(:base_dt, 'YYYYMMDD') - WK.WK_DATE) >= 0
            UNION ALL
            SELECT (TO_DATE(:base_dt, 'YYYYMMDD') - MD.LAST_WK_DT) AS DELAY_DAYS
            FROM TB_MODON MD
            INNER JOIN NO_WK_MODON NW ON NW.FARM_NO = MD.FARM_NO AND NW.PIG_NO = MD.PIG_NO
            WHERE MD.STATUS_CD IN ('010006', '010007')
              AND MD.IN_DT < TO_DATE(:base_dt, 'YYYYMMDD') + 1
              AND (TO_DATE(:base_dt, 'YYYYMMDD') - MD.LAST_WK_DT) >= 0
        ),
        BM_DELAY AS (
            SELECT (TO_DATE(:base_dt, 'YYYYMMDD') - WK.WK_DATE) - :preg_period AS DELAY_DAYS
            FROM TB_MODON MD
            JOIN LAST_WK LW ON LW.FARM_NO = MD.FARM_NO AND LW.PIG_NO = MD.PIG_NO
            JOIN TB_MODON_WK WK ON WK.FARM_NO = LW.FARM_NO AND WK.PIG_NO = LW.PIG_NO AND WK.SEQ = LW.MSEQ
            WHERE MD.FARM_NO = :farm_no
              AND MD.OUT_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD') AND MD.USE_YN = 'Y'
              AND WK.WK_GUBUN = 'G'
              AND (TO_DATE(:base_dt, 'YYYYMMDD') - WK.WK_DATE) - :preg_period >= 0
        ),
        EU_DELAY AS (
            SELECT (TO_DATE(:base_dt, 'YYYYMMDD') - WK.WK_DATE) - :wean_period AS DELAY_DAYS
            FROM TB_MODON MD
            JOIN LAST_WK LW ON LW.FARM_NO = MD.FARM_NO AND LW.PIG_NO = MD.PIG_NO
            JOIN TB_MODON_WK WK ON WK.FARM_NO = LW.FARM_NO AND WK.PIG_NO = LW.PIG_NO AND WK.SEQ = LW.MSEQ
            WHERE MD.FARM_NO = :farm_no
              AND MD.OUT_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD') AND MD.USE_YN = 'Y'
              AND WK.WK_GUBUN = 'B'
              AND (TO_DATE(:base_dt, 'YYYYMMDD') - WK.WK_DATE) - :wean_period >= 0
        ),
        ALL_DELAYS AS (
            SELECT 'HUBO' AS TYPE_CD, DELAY_DAYS FROM HUBO
            UNION ALL SELECT 'EU_MI', DELAY_DAYS FROM EU_MI
            UNION ALL SELECT 'SG_MI', DELAY_DAYS FROM SG_MI
            UNION ALL SELECT 'BM_DELAY', DELAY_DAYS FROM BM_DELAY
            UNION ALL SELECT 'EU_DELAY', DELAY_DAYS FROM EU_DELAY
        ),
        PERIODS AS (
            SELECT 1 AS SORT_NO, '~3' AS PERIOD, 0 AS MIN_DAY, 3 AS MAX_DAY FROM DUAL UNION ALL
            SELECT 2, '4~7', 4, 7 FROM DUAL UNION ALL
            SELECT 3, '8~14', 8, 14 FROM DUAL UNION ALL
            SELECT 4, '14~', 15, 9999 FROM DUAL
        )
        SELECT
            :master_seq, :farm_no, 'ALERT', P.SORT_NO, P.PERIOD,
            NVL(SUM(CASE WHEN AD.TYPE_CD = 'HUBO' THEN 1 END), 0),
            NVL(SUM(CASE WHEN AD.TYPE_CD = 'EU_MI' THEN 1 END), 0),
            NVL(SUM(CASE WHEN AD.TYPE_CD = 'SG_MI' THEN 1 END), 0),
            NVL(SUM(CASE WHEN AD.TYPE_CD = 'BM_DELAY' THEN 1 END), 0),
            NVL(SUM(CASE WHEN AD.TYPE_CD = 'EU_DELAY' THEN 1 END), 0)
        FROM PERIODS P
        LEFT OUTER JOIN ALL_DELAYS AD ON AD.DELAY_DAYS >= P.MIN_DAY AND AD.DELAY_DAYS <= P.MAX_DAY
        GROUP BY P.SORT_NO, P.PERIOD
        ORDER BY P.SORT_NO
        """

        return self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'base_dt': dt_to,
            'first_gb_day': config['first_gb_day'],
            'avg_return': config['avg_return'],
            'preg_period': config['preg_period'],
            'wean_period': config['wean_period'],
        })

    def _update_week_summary(self) -> None:
        """TS_INS_WEEK 요약 업데이트"""
        sql = """
        UPDATE TS_INS_WEEK W
        SET (ALERT_TOTAL, ALERT_HUBO, ALERT_EU_MI, ALERT_SG_MI, ALERT_BM_DELAY, ALERT_EU_DELAY) = (
            SELECT NVL(SUM(CNT_1 + CNT_2 + CNT_3 + CNT_4 + CNT_5), 0),
                   NVL(SUM(CNT_1), 0), NVL(SUM(CNT_2), 0), NVL(SUM(CNT_3), 0),
                   NVL(SUM(CNT_4), 0), NVL(SUM(CNT_5), 0)
            FROM TS_INS_WEEK_SUB S
            WHERE S.MASTER_SEQ = :master_seq AND S.FARM_NO = :farm_no AND S.GUBUN = 'ALERT'
        )
        WHERE W.MASTER_SEQ = :master_seq AND W.FARM_NO = :farm_no
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})
