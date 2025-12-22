"""
교배 팝업 데이터 추출 프로세서
SP_INS_WEEK_GB_POPUP 프로시저 Python 전환

역할:
- 교배 요약 통계 (GUBUN='GB', SUB_GUBUN='STAT')
- 재귀일별 교배복수 차트 (GUBUN='GB', SUB_GUBUN='CHART')
"""
import logging
from typing import Any, Dict

from .base import BaseProcessor

logger = logging.getLogger(__name__)


class MatingProcessor(BaseProcessor):
    """교배 팝업 프로세서"""

    PROC_NAME = 'MatingProcessor'

    def process(self, dt_from: str, dt_to: str, **kwargs) -> Dict[str, Any]:
        """교배 데이터 추출

        Args:
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)

        Returns:
            처리 결과 딕셔너리
        """
        self.logger.info(f"교배 팝업 시작: 농장={self.farm_no}, 기간={dt_from}~{dt_to}")

        # 날짜 포맷 변환 (YYYYMMDD → yyyy-MM-dd)
        sdt = f"{dt_from[:4]}-{dt_from[4:6]}-{dt_from[6:8]}"
        edt = f"{dt_to[:4]}-{dt_to[4:6]}-{dt_to[6:8]}"

        # 1. 기존 데이터 삭제
        self._delete_existing()

        # 2. 예정 복수 조회
        plan_hubo, plan_js = self._get_plan_counts(sdt, edt)

        # 3. 연간 누적 교배복수 조회
        acc_gb_cnt = self._get_acc_count(dt_to)

        # 4. 교배 요약 통계 집계 및 INSERT
        stats = self._insert_stats(dt_from, dt_to, plan_hubo, plan_js, acc_gb_cnt)

        # 5. 재귀일별 교배복수 차트 INSERT
        chart_cnt = self._insert_chart(dt_from, dt_to)

        # 6. TS_INS_WEEK 업데이트
        self._update_week(stats['total_cnt'], acc_gb_cnt)

        self.logger.info(f"교배 팝업 완료: 농장={self.farm_no}, 합계={stats['total_cnt']}")

        return {
            'status': 'success',
            'total_cnt': stats['total_cnt'],
            'chart_cnt': chart_cnt,
        }

    def _delete_existing(self) -> None:
        """기존 GB 데이터 삭제"""
        sql = """
        DELETE FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no AND GUBUN = 'GB'
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

    def _get_plan_counts(self, sdt: str, edt: str) -> tuple:
        """예정 복수 조회 (FN_MD_SCHEDULE_BSE_2020)"""
        # 초교배 예정 (후보돈: 010001)
        sql_hubo = """
        SELECT COUNT(*)
        FROM TABLE(FN_MD_SCHEDULE_BSE_2020(
            :farm_no, 'JOB-DAJANG', '150005', '010001',
            :sdt, :edt, NULL, 'ko', 'yyyy-MM-dd', '-1', NULL
        ))
        """
        result = self.fetch_one(sql_hubo, {'farm_no': self.farm_no, 'sdt': sdt, 'edt': edt})
        plan_hubo = result[0] if result else 0

        # 정상교배 예정 (이유돈: 010005)
        sql_js = """
        SELECT COUNT(*)
        FROM TABLE(FN_MD_SCHEDULE_BSE_2020(
            :farm_no, 'JOB-DAJANG', '150005', '010005',
            :sdt, :edt, NULL, 'ko', 'yyyy-MM-dd', '-1', NULL
        ))
        """
        result = self.fetch_one(sql_js, {'farm_no': self.farm_no, 'sdt': sdt, 'edt': edt})
        plan_js = result[0] if result else 0

        return plan_hubo, plan_js

    def _get_acc_count(self, dt_to: str) -> int:
        """연간 누적 교배복수 조회 (1/1 ~ 기준일)"""
        sql = """
        SELECT COUNT(*)
        FROM TB_MODON_WK
        WHERE FARM_NO = :farm_no
          AND WK_GUBUN = 'G'
          AND USE_YN = 'Y'
          AND WK_DT >= :year_start
          AND WK_DT <= :dt_to
        """
        year_start = dt_to[:4] + '0101'
        result = self.fetch_one(sql, {'farm_no': self.farm_no, 'year_start': year_start, 'dt_to': dt_to})
        return result[0] if result else 0

    def _insert_stats(self, dt_from: str, dt_to: str, plan_hubo: int, plan_js: int, acc_gb_cnt: int) -> Dict[str, Any]:
        """교배 요약 통계 집계 및 INSERT"""
        # 집계
        sql_agg = """
        SELECT
            NVL(COUNT(*), 0),
            NVL(SUM(CASE WHEN C.WK_GUBUN = 'F' THEN 1 ELSE 0 END), 0),
            NVL(SUM(CASE WHEN C.WK_GUBUN = 'B' THEN 1 ELSE 0 END), 0),
            NVL(ROUND(AVG(
                CASE WHEN A.GYOBAE_CNT = 1 AND B.WK_DT IS NOT NULL
                          AND NOT (A.SANCHA = 0 AND A.GYOBAE_CNT = 1)
                     THEN TO_DATE(A.WK_DT, 'YYYYMMDD') - TO_DATE(B.WK_DT, 'YYYYMMDD') END
            ), 1), 0),
            NVL(ROUND(AVG(
                CASE WHEN A.SANCHA = 0 AND A.GYOBAE_CNT = 1
                     THEN TO_DATE(A.WK_DT, 'YYYYMMDD') - D.BIRTH_DT END
            ), 1), 0),
            NVL(SUM(CASE WHEN A.SANCHA = 0 AND A.GYOBAE_CNT = 1 THEN 1 ELSE 0 END), 0),
            NVL(SUM(CASE WHEN A.GYOBAE_CNT > 1 THEN 1 ELSE 0 END), 0),
            NVL(SUM(CASE WHEN A.GYOBAE_CNT = 1 THEN 1 ELSE 0 END), 0)
        FROM TB_MODON_WK A
        LEFT OUTER JOIN TB_MODON_WK B ON B.FARM_NO = A.FARM_NO AND B.PIG_NO = A.PIG_NO
           AND B.SEQ = A.SEQ - 1 AND B.USE_YN = 'Y'
        LEFT OUTER JOIN TB_MODON_WK C ON C.FARM_NO = A.FARM_NO AND C.PIG_NO = A.PIG_NO
           AND C.SEQ = A.SEQ + 1 AND C.USE_YN = 'Y'
        INNER JOIN TB_MODON D ON D.FARM_NO = A.FARM_NO AND D.PIG_NO = A.PIG_NO AND D.USE_YN = 'Y'
        WHERE A.FARM_NO = :farm_no
          AND A.WK_GUBUN = 'G'
          AND A.USE_YN = 'Y'
          AND A.WK_DT >= :dt_from
          AND A.WK_DT <= :dt_to
        """
        result = self.fetch_one(sql_agg, {'farm_no': self.farm_no, 'dt_from': dt_from, 'dt_to': dt_to})

        stats = {
            'total_cnt': result[0] if result else 0,
            'sago_cnt': result[1] if result else 0,
            'bunman_cnt': result[2] if result else 0,
            'avg_return': result[3] if result else 0,
            'avg_first_gb': result[4] if result else 0,
            'first_gb_cnt': result[5] if result else 0,
            'sago_gb_cnt': result[6] if result else 0,
            'js_gb_cnt': result[7] if result else 0,
        }

        # INSERT
        sql_ins = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            CNT_1, CNT_2, CNT_3, VAL_1, VAL_2, CNT_4, CNT_5, CNT_6,
            CNT_7, CNT_8, CNT_9
        ) VALUES (
            :master_seq, :farm_no, 'GB', 'STAT', 1,
            :total_cnt, :sago_cnt, :bunman_cnt, :avg_return, :avg_first_gb,
            :first_gb_cnt, :sago_gb_cnt, :js_gb_cnt, :plan_hubo, :plan_js, :acc_gb_cnt
        )
        """
        self.execute(sql_ins, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            **stats,
            'plan_hubo': plan_hubo,
            'plan_js': plan_js,
            'acc_gb_cnt': acc_gb_cnt,
        })

        return stats

    def _insert_chart(self, dt_from: str, dt_to: str) -> int:
        """재귀일별 교배복수 차트 INSERT"""
        sql = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO, CODE_1, CNT_1
        )
        SELECT :master_seq, :farm_no, 'GB', 'CHART', SORT_NO, PERIOD, CNT
        FROM (
            SELECT
                CASE
                    WHEN RETURN_DAY <= 7 THEN '~7'
                    WHEN RETURN_DAY <= 10 THEN '10'
                    WHEN RETURN_DAY <= 15 THEN '15'
                    WHEN RETURN_DAY <= 20 THEN '20'
                    WHEN RETURN_DAY <= 25 THEN '25'
                    WHEN RETURN_DAY <= 30 THEN '30'
                    WHEN RETURN_DAY <= 35 THEN '35'
                    WHEN RETURN_DAY <= 40 THEN '40'
                    WHEN RETURN_DAY <= 45 THEN '45'
                    WHEN RETURN_DAY <= 50 THEN '50'
                    ELSE '50↑'
                END AS PERIOD,
                CASE
                    WHEN RETURN_DAY <= 7 THEN 1
                    WHEN RETURN_DAY <= 10 THEN 2
                    WHEN RETURN_DAY <= 15 THEN 3
                    WHEN RETURN_DAY <= 20 THEN 4
                    WHEN RETURN_DAY <= 25 THEN 5
                    WHEN RETURN_DAY <= 30 THEN 6
                    WHEN RETURN_DAY <= 35 THEN 7
                    WHEN RETURN_DAY <= 40 THEN 8
                    WHEN RETURN_DAY <= 45 THEN 9
                    WHEN RETURN_DAY <= 50 THEN 10
                    ELSE 11
                END AS SORT_NO,
                COUNT(*) AS CNT
            FROM (
                SELECT TO_DATE(A.WK_DT, 'YYYYMMDD') - TO_DATE(E.WK_DT, 'YYYYMMDD') AS RETURN_DAY
                FROM TB_MODON_WK A
                LEFT OUTER JOIN (
                    SELECT FARM_NO, PIG_NO, WK_DT,
                           ROW_NUMBER() OVER (PARTITION BY FARM_NO, PIG_NO ORDER BY SEQ DESC) AS RN
                    FROM TB_MODON_WK
                    WHERE FARM_NO = :farm_no AND WK_GUBUN = 'E' AND USE_YN = 'Y'
                ) E ON E.FARM_NO = A.FARM_NO AND E.PIG_NO = A.PIG_NO AND E.RN = 1
                WHERE A.FARM_NO = :farm_no
                  AND A.WK_GUBUN = 'G'
                  AND A.USE_YN = 'Y'
                  AND A.WK_DT >= :dt_from
                  AND A.WK_DT <= :dt_to
                  AND NOT (A.SANCHA = 0 AND A.GYOBAE_CNT = 1)
                  AND E.WK_DT IS NOT NULL
            )
            GROUP BY
                CASE WHEN RETURN_DAY <= 7 THEN '~7' WHEN RETURN_DAY <= 10 THEN '10'
                     WHEN RETURN_DAY <= 15 THEN '15' WHEN RETURN_DAY <= 20 THEN '20'
                     WHEN RETURN_DAY <= 25 THEN '25' WHEN RETURN_DAY <= 30 THEN '30'
                     WHEN RETURN_DAY <= 35 THEN '35' WHEN RETURN_DAY <= 40 THEN '40'
                     WHEN RETURN_DAY <= 45 THEN '45' WHEN RETURN_DAY <= 50 THEN '50'
                     ELSE '50↑' END,
                CASE WHEN RETURN_DAY <= 7 THEN 1 WHEN RETURN_DAY <= 10 THEN 2
                     WHEN RETURN_DAY <= 15 THEN 3 WHEN RETURN_DAY <= 20 THEN 4
                     WHEN RETURN_DAY <= 25 THEN 5 WHEN RETURN_DAY <= 30 THEN 6
                     WHEN RETURN_DAY <= 35 THEN 7 WHEN RETURN_DAY <= 40 THEN 8
                     WHEN RETURN_DAY <= 45 THEN 9 WHEN RETURN_DAY <= 50 THEN 10
                     ELSE 11 END
        )
        ORDER BY SORT_NO
        """
        return self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'dt_from': dt_from,
            'dt_to': dt_to,
        })

    def _update_week(self, total_cnt: int, acc_gb_cnt: int) -> None:
        """TS_INS_WEEK 메인 테이블 업데이트"""
        sql = """
        UPDATE TS_INS_WEEK
        SET LAST_GB_CNT = :total_cnt,
            LAST_GB_SUM = :acc_gb_cnt
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
        """
        self.execute(sql, {
            'total_cnt': total_cnt,
            'acc_gb_cnt': acc_gb_cnt,
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
        })
