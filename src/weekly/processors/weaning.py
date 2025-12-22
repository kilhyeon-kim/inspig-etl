"""
이유 팝업 데이터 추출 프로세서
SP_INS_WEEK_EU_POPUP 프로시저 Python 전환

역할:
- 이유 요약 통계 (GUBUN='EU')
- TB_MODON_WK + TB_EU + TB_BUNMAN 조인
- 자돈 증감 내역 (TB_MODON_JADON_TRANS)
- 포유개시 계산 (실산 - 폐사 + 양자전입 - 양자전출)
- TS_INS_WEEK 이유 관련 컬럼 업데이트
"""
import logging
from typing import Any, Dict

from .base import BaseProcessor

logger = logging.getLogger(__name__)


class WeaningProcessor(BaseProcessor):
    """이유 팝업 프로세서"""

    PROC_NAME = 'WeaningProcessor'

    def process(self, dt_from: str, dt_to: str, **kwargs) -> Dict[str, Any]:
        """이유 데이터 추출

        Args:
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)

        Returns:
            처리 결과 딕셔너리
        """
        self.logger.info(f"이유 팝업 시작: 농장={self.farm_no}, 기간={dt_from}~{dt_to}")

        # 날짜 포맷 변환
        sdt = f"{dt_from[:4]}-{dt_from[4:6]}-{dt_from[6:8]}"
        edt = f"{dt_to[:4]}-{dt_to[4:6]}-{dt_to[6:8]}"

        # 1. 기존 데이터 삭제
        self._delete_existing()

        # 2. 이유 예정 복수 조회
        plan_eu = self._get_plan_count(sdt, edt)

        # 3. 연간 누적 실적 조회
        acc_stats = self._get_acc_stats(dt_to)

        # 4. 이유 통계 집계 및 INSERT (자돈 증감 포함)
        stats = self._insert_stats(dt_from, dt_to, plan_eu, acc_stats)

        # 5. TS_INS_WEEK 업데이트
        self._update_week(stats, acc_stats)

        self.logger.info(f"이유 팝업 완료: 농장={self.farm_no}, 이유복수={stats.get('total_cnt', 0)}")

        return {
            'status': 'success',
            **stats,
        }

    def _delete_existing(self) -> None:
        """기존 EU 데이터 삭제"""
        sql = """
        DELETE FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no AND GUBUN = 'EU'
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

    def _get_plan_count(self, sdt: str, edt: str) -> int:
        """이유 예정 복수 조회 (FN_MD_SCHEDULE_BSE_2020)"""
        sql = """
        SELECT COUNT(*)
        FROM TABLE(FN_MD_SCHEDULE_BSE_2020(
            :farm_no, 'JOB-DAJANG', '150003', NULL,
            :sdt, :edt, NULL, 'ko', 'yyyy-MM-dd', '-1', NULL
        ))
        """
        result = self.fetch_one(sql, {'farm_no': self.farm_no, 'sdt': sdt, 'edt': edt})
        return result[0] if result else 0

    def _get_acc_stats(self, dt_to: str) -> Dict[str, Any]:
        """연간 누적 실적 조회 (1/1 ~ 기준일)"""
        year_start = dt_to[:4] + '0101'

        sql = """
        SELECT
            COUNT(*),
            NVL(SUM(NVL(D.DUSU, 0) + NVL(D.DUSU_SU, 0)), 0),
            NVL(ROUND(AVG(NVL(D.DUSU, 0) + NVL(D.DUSU_SU, 0)), 1), 0)
        FROM TB_MODON_WK A
        INNER JOIN TB_EU D
            ON D.FARM_NO = A.FARM_NO AND D.PIG_NO = A.PIG_NO
           AND D.WK_DT = A.WK_DT AND D.WK_GUBUN = A.WK_GUBUN AND D.USE_YN = 'Y'
        WHERE A.FARM_NO = :farm_no
          AND A.WK_GUBUN = 'E'
          AND A.USE_YN = 'Y'
          AND A.WK_DT >= :year_start
          AND A.WK_DT <= :dt_to
        """
        result = self.fetch_one(sql, {'farm_no': self.farm_no, 'year_start': year_start, 'dt_to': dt_to})

        return {
            'acc_eu_cnt': result[0] if result else 0,
            'acc_eu_jd': result[1] if result else 0,
            'acc_avg_jd': result[2] if result else 0,
        }

    def _insert_stats(self, dt_from: str, dt_to: str, plan_eu: int, acc_stats: Dict) -> Dict[str, Any]:
        """이유 통계 집계 및 INSERT (자돈 증감 포함)

        WITH절로 사전 집계:
        - JADON_TRANS_AGG: 자돈 증감 내역 (SANCHA+WK_DT 기준)
        - JADON_POGAE_AGG: 포유개시용 (BUN_DT 기준)
        - NEXT_WK: 다음 작업일 계산
        - JADON_PERIOD_AGG: 기간별 자돈 증감 합계
        """
        sql = """
        WITH
        JADON_TRANS_AGG AS (
            SELECT JT.FARM_NO, JT.PIG_NO, JT.SANCHA, JT.WK_DT,
                   SUM(CASE WHEN JT.GUBUN_CD = '160001' THEN NVL(JT.DUSU,0)+NVL(JT.DUSU_SU,0) ELSE 0 END) AS PS_DS,
                   SUM(CASE WHEN JT.GUBUN_CD = '160002' THEN NVL(JT.DUSU,0)+NVL(JT.DUSU_SU,0) ELSE 0 END) AS BB_DS,
                   SUM(CASE WHEN JT.GUBUN_CD = '160003' THEN NVL(JT.DUSU,0)+NVL(JT.DUSU_SU,0) ELSE 0 END) AS JI_DS,
                   SUM(CASE WHEN JT.GUBUN_CD = '160004' THEN NVL(JT.DUSU,0)+NVL(JT.DUSU_SU,0) ELSE 0 END) AS JC_DS
            FROM TB_MODON_JADON_TRANS JT
            WHERE JT.FARM_NO = :farm_no AND JT.USE_YN = 'Y'
            GROUP BY JT.FARM_NO, JT.PIG_NO, JT.SANCHA, JT.WK_DT
        ),
        JADON_POGAE_AGG AS (
            SELECT JT.FARM_NO, JT.PIG_NO, JT.BUN_DT,
                   SUM(CASE WHEN JT.GUBUN_CD = '160001' THEN NVL(JT.DUSU,0)+NVL(JT.DUSU_SU,0) ELSE 0 END) AS PS_DS,
                   SUM(CASE WHEN JT.GUBUN_CD = '160003' THEN NVL(JT.DUSU,0)+NVL(JT.DUSU_SU,0) ELSE 0 END) AS JI_DS,
                   SUM(CASE WHEN JT.GUBUN_CD = '160004' THEN NVL(JT.DUSU,0)+NVL(JT.DUSU_SU,0) ELSE 0 END) AS JC_DS
            FROM TB_MODON_JADON_TRANS JT
            WHERE JT.FARM_NO = :farm_no AND JT.USE_YN = 'Y'
            GROUP BY JT.FARM_NO, JT.PIG_NO, JT.BUN_DT
        ),
        NEXT_WK AS (
            SELECT FARM_NO, PIG_NO, WK_DT AS CUR_WK_DT,
                   MIN(NEXT_WK_DT) AS NEXT_WK_DT, MIN(NEXT_WK_GUBUN) KEEP (DENSE_RANK FIRST ORDER BY NEXT_WK_DT) AS NEXT_WK_GUBUN
            FROM (
                SELECT A.FARM_NO, A.PIG_NO, A.WK_DT,
                       B.WK_DT AS NEXT_WK_DT, B.WK_GUBUN AS NEXT_WK_GUBUN
                FROM TB_MODON_WK A
                INNER JOIN TB_MODON_WK B
                    ON B.FARM_NO = A.FARM_NO AND B.PIG_NO = A.PIG_NO
                   AND B.WK_DT > A.WK_DT AND B.USE_YN = 'Y'
                WHERE A.FARM_NO = :farm_no
                  AND A.WK_GUBUN = 'E'
                  AND A.USE_YN = 'Y'
                  AND A.WK_DT >= :dt_from
                  AND A.WK_DT <= :dt_to
            )
            GROUP BY FARM_NO, PIG_NO, WK_DT
        ),
        JADON_PERIOD_AGG AS (
            SELECT A.FARM_NO, A.PIG_NO, A.SANCHA, A.WK_DT AS EU_WK_DT, B.WK_DT AS BM_WK_DT,
                   NVL(SUM(JT.PS_DS), 0) AS SUM_PS_DS,
                   NVL(SUM(JT.BB_DS), 0) AS SUM_BB_DS,
                   NVL(SUM(JT.JI_DS), 0) AS SUM_JI_DS,
                   NVL(SUM(JT.JC_DS), 0) AS SUM_JC_DS
            FROM TB_MODON_WK A
            INNER JOIN TB_MODON_WK B
                ON B.FARM_NO = A.FARM_NO AND B.PIG_NO = A.PIG_NO
               AND B.SANCHA = A.SANCHA AND B.WK_GUBUN = 'B' AND B.USE_YN = 'Y'
            LEFT OUTER JOIN NEXT_WK NW
                ON NW.FARM_NO = A.FARM_NO AND NW.PIG_NO = A.PIG_NO AND NW.CUR_WK_DT = A.WK_DT
            LEFT OUTER JOIN JADON_TRANS_AGG JT
                ON JT.FARM_NO = A.FARM_NO AND JT.PIG_NO = A.PIG_NO AND JT.SANCHA = A.SANCHA
               AND JT.WK_DT >= B.WK_DT
               AND JT.WK_DT <= CASE WHEN NW.NEXT_WK_GUBUN = 'G' THEN NW.NEXT_WK_DT
                                    WHEN NW.NEXT_WK_DT IS NULL AND A.DAERI_YN = 'N' THEN :dt_to
                                    ELSE TO_CHAR(TO_DATE(A.WK_DT, 'YYYYMMDD') - 1, 'YYYYMMDD') END
            WHERE A.FARM_NO = :farm_no
              AND A.WK_GUBUN = 'E'
              AND A.USE_YN = 'Y'
              AND A.WK_DT >= :dt_from
              AND A.WK_DT <= :dt_to
            GROUP BY A.FARM_NO, A.PIG_NO, A.SANCHA, A.WK_DT, B.WK_DT
        )
        SELECT
            COUNT(*),
            NVL(SUM(NVL(D.DUSU, 0) + NVL(D.DUSU_SU, 0)), 0),
            NVL(SUM(NVL(E.SILSAN, 0) + NVL(E.SASAN, 0) + NVL(E.MILA, 0)), 0),
            NVL(SUM(NVL(E.SILSAN, 0)), 0),
            NVL(SUM(TO_DATE(A.WK_DT, 'YYYYMMDD') - TO_DATE(B.WK_DT, 'YYYYMMDD')), 0),
            NVL(SUM(NVL(D.TOTAL_KG, 0)), 0),
            NVL(ROUND(AVG(NVL(D.DUSU, 0) + NVL(D.DUSU_SU, 0)), 1), 0),
            NVL(ROUND(AVG(TO_DATE(A.WK_DT, 'YYYYMMDD') - TO_DATE(B.WK_DT, 'YYYYMMDD')), 1), 0),
            NVL(SUM(PA.SUM_PS_DS), 0),
            NVL(SUM(PA.SUM_BB_DS), 0),
            NVL(SUM(PA.SUM_JI_DS), 0),
            NVL(SUM(PA.SUM_JC_DS), 0),
            NVL(SUM(
                NVL(E.SILSAN, 0)
                - NVL(PO.PS_DS, 0)
                + NVL(PO.JI_DS, 0)
                - NVL(PO.JC_DS, 0)
            ), 0)
        FROM TB_MODON_WK A
        INNER JOIN TB_EU D
            ON D.FARM_NO = A.FARM_NO AND D.PIG_NO = A.PIG_NO
           AND D.WK_DT = A.WK_DT AND D.WK_GUBUN = A.WK_GUBUN AND D.USE_YN = 'Y'
        INNER JOIN TB_MODON_WK B
            ON B.FARM_NO = A.FARM_NO AND B.PIG_NO = A.PIG_NO
           AND B.SANCHA = A.SANCHA AND B.WK_GUBUN = 'B' AND B.USE_YN = 'Y'
        INNER JOIN TB_BUNMAN E
            ON E.FARM_NO = B.FARM_NO AND E.PIG_NO = B.PIG_NO
           AND E.WK_DT = B.WK_DT AND E.WK_GUBUN = B.WK_GUBUN AND E.USE_YN = 'Y'
        LEFT OUTER JOIN JADON_PERIOD_AGG PA
            ON PA.FARM_NO = A.FARM_NO AND PA.PIG_NO = A.PIG_NO
           AND PA.SANCHA = A.SANCHA AND PA.EU_WK_DT = A.WK_DT
        LEFT OUTER JOIN JADON_POGAE_AGG PO
            ON PO.FARM_NO = A.FARM_NO AND PO.PIG_NO = A.PIG_NO AND PO.BUN_DT = B.WK_DT
        WHERE A.FARM_NO = :farm_no
          AND A.WK_GUBUN = 'E'
          AND A.USE_YN = 'Y'
          AND A.WK_DT >= :dt_from
          AND A.WK_DT <= :dt_to
        """
        result = self.fetch_one(sql, {'farm_no': self.farm_no, 'dt_from': dt_from, 'dt_to': dt_to})

        total_cnt = result[0] if result else 0
        sum_eudusu = result[1] if result else 0
        sum_chongsan = result[2] if result else 0
        sum_silsan = result[3] if result else 0
        sum_pougigan = result[4] if result else 0
        sum_kg = result[5] if result else 0
        avg_eudusu = result[6] if result else 0
        avg_pougigan = result[7] if result else 0
        sum_ps_ds = result[8] if result else 0
        sum_bb_ds = result[9] if result else 0
        sum_ji_ds = result[10] if result else 0
        sum_jc_ds = result[11] if result else 0
        sum_pogae = result[12] if result else 0

        # 평균체중 계산 (TOTAL_KG / 이유복수)
        avg_kg = round(sum_kg / total_cnt, 2) if total_cnt > 0 else 0

        # 이유육성율 계산 (이유두수 / 실산 * 100)
        survival_rate = round(sum_eudusu / sum_silsan * 100, 1) if sum_silsan > 0 else 0

        # 평균 이유두수 증감 계산 (지난주 평균 - 1년 평균)
        chg_jd = round(avg_eudusu - acc_stats['acc_avg_jd'], 1) if acc_stats['acc_avg_jd'] > 0 else 0

        stats = {
            'total_cnt': total_cnt,
            'sum_eudusu': sum_eudusu,
            'sum_chongsan': sum_chongsan,
            'sum_silsan': sum_silsan,
            'sum_pougigan': sum_pougigan,
            'avg_eudusu': avg_eudusu,
            'avg_kg': avg_kg,
            'avg_pougigan': avg_pougigan,
            'survival_rate': survival_rate,
            'sum_pogae': sum_pogae,
            'sum_ps_ds': sum_ps_ds,
            'sum_bb_ds': sum_bb_ds,
            'sum_ji_ds': sum_ji_ds,
            'sum_jc_ds': sum_jc_ds,
            'chg_jd': chg_jd,
            'plan_eu': plan_eu,
        }

        # INSERT
        sql_ins = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SORT_NO,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5,
            CNT_6, CNT_7, CNT_8, CNT_9,
            VAL_1, VAL_2, VAL_3, VAL_4, VAL_5,
            STR_1
        ) VALUES (
            :master_seq, :farm_no, 'EU', 1,
            :total_cnt, :sum_eudusu, :sum_silsan, :sum_pougigan, :plan_eu,
            :sum_ps_ds, :sum_bb_ds, :sum_ji_ds, :sum_jc_ds,
            :avg_eudusu, :avg_kg, :survival_rate, :avg_pougigan, :sum_pogae,
            TO_CHAR(:sum_chongsan)
        )
        """
        self.execute(sql_ins, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            **stats,
        })

        return stats

    def _update_week(self, stats: Dict[str, Any], acc_stats: Dict[str, Any]) -> None:
        """TS_INS_WEEK 이유 관련 컬럼 업데이트"""
        sql = """
        UPDATE TS_INS_WEEK
        SET LAST_EU_CNT = :total_cnt,
            LAST_EU_JD_CNT = :sum_eudusu,
            LAST_EU_AVG_JD = :avg_eudusu,
            LAST_EU_AVG_KG = :avg_kg,
            LAST_EU_SUM_CNT = :acc_eu_cnt,
            LAST_EU_SUM_JD = :acc_eu_jd,
            LAST_EU_SUM_AVG_JD = :acc_avg_jd,
            LAST_EU_CHG_JD = :chg_jd
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            **stats,
            **acc_stats,
        })
