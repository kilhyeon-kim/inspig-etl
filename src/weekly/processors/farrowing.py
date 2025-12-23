"""
분만 팝업 데이터 추출 프로세서
SP_INS_WEEK_BM_POPUP 프로시저 Python 전환

역할:
- 분만 요약 통계 (GUBUN='BM')
- TB_MODON_WK + TB_BUNMAN 조인
- 포유개시 계산 (자돈 증감 포함)
"""
import logging
from typing import Any, Dict

from .base import BaseProcessor

logger = logging.getLogger(__name__)


class FarrowingProcessor(BaseProcessor):
    """분만 팝업 프로세서"""

    PROC_NAME = 'FarrowingProcessor'

    def process(self, dt_from: str, dt_to: str, **kwargs) -> Dict[str, Any]:
        """분만 데이터 추출

        Args:
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)

        Returns:
            처리 결과 딕셔너리
        """
        self.logger.info(f"분만 팝업 시작: 농장={self.farm_no}, 기간={dt_from}~{dt_to}")

        # 날짜 포맷 변환
        sdt = f"{dt_from[:4]}-{dt_from[4:6]}-{dt_from[6:8]}"
        edt = f"{dt_to[:4]}-{dt_to[4:6]}-{dt_to[6:8]}"

        # 1. 기존 데이터 삭제
        self._delete_existing()

        # 2. 분만 예정 복수 조회
        plan_bm = self._get_plan_count(sdt, edt)

        # 3. 연간 누적 실적 조회
        acc_stats = self._get_acc_stats(dt_to)

        # 4. 분만 통계 집계 및 INSERT
        stats = self._insert_stats(dt_from, dt_to, plan_bm, acc_stats)

        # 5. TS_INS_WEEK 업데이트
        self._update_week(stats, acc_stats)

        self.logger.info(f"분만 팝업 완료: 농장={self.farm_no}, 분만복수={stats.get('total_cnt', 0)}")

        return {
            'status': 'success',
            **stats,
        }

    def _delete_existing(self) -> None:
        """기존 BM 데이터 삭제"""
        sql = """
        DELETE FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no AND GUBUN = 'BM'
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

    def _get_plan_count(self, sdt: str, edt: str) -> int:
        """분만 예정 복수 조회 (FN_MD_SCHEDULE_BSE_2020)"""
        sql = """
        SELECT COUNT(*)
        FROM TABLE(FN_MD_SCHEDULE_BSE_2020(
            :farm_no, 'JOB-DAJANG', '150002', NULL,
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
            NVL(SUM(NVL(B.SILSAN,0) + NVL(B.SASAN,0) + NVL(B.MILA,0)), 0),
            NVL(SUM(B.SILSAN), 0),
            NVL(ROUND(AVG(NVL(B.SILSAN,0) + NVL(B.SASAN,0) + NVL(B.MILA,0)), 1), 0),
            NVL(ROUND(AVG(B.SILSAN), 1), 0)
        FROM TB_MODON_WK A
        INNER JOIN TB_BUNMAN B
            ON B.FARM_NO = A.FARM_NO AND B.PIG_NO = A.PIG_NO
           AND B.WK_DT = A.WK_DT AND B.WK_GUBUN = A.WK_GUBUN AND B.USE_YN = 'Y'
        WHERE A.FARM_NO = :farm_no
          AND A.WK_GUBUN = 'B'
          AND A.USE_YN = 'Y'
          AND A.WK_DT >= :year_start
          AND A.WK_DT <= :dt_to
        """
        result = self.fetch_one(sql, {'farm_no': self.farm_no, 'year_start': year_start, 'dt_to': dt_to})

        return {
            'acc_bm_cnt': result[0] if result else 0,
            'acc_total': result[1] if result else 0,
            'acc_live': result[2] if result else 0,
            'acc_avg_total': result[3] if result else 0,
            'acc_avg_live': result[4] if result else 0,
        }

    def _insert_stats(self, dt_from: str, dt_to: str, plan_bm: int, acc_stats: Dict) -> Dict[str, Any]:
        """분만 통계 집계 및 INSERT (포유개시 포함)"""
        # 분만 통계 집계 (WITH절로 자돈 증감 사전 집계)
        sql = """
        WITH JADON_POGAE_AGG AS (
            SELECT JT.FARM_NO, JT.PIG_NO, TO_CHAR(JT.BUN_DT, 'YYYYMMDD') AS BUN_DT,
                   SUM(CASE WHEN JT.GUBUN_CD = '160001' THEN NVL(JT.DUSU,0)+NVL(JT.DUSU_SU,0) ELSE 0 END) AS PS_DS,
                   SUM(CASE WHEN JT.GUBUN_CD = '160003' THEN NVL(JT.DUSU,0)+NVL(JT.DUSU_SU,0) ELSE 0 END) AS JI_DS,
                   SUM(CASE WHEN JT.GUBUN_CD = '160004' THEN NVL(JT.DUSU,0)+NVL(JT.DUSU_SU,0) ELSE 0 END) AS JC_DS
            FROM TB_MODON_JADON_TRANS JT
            WHERE JT.FARM_NO = :farm_no AND JT.USE_YN = 'Y'
            GROUP BY JT.FARM_NO, JT.PIG_NO, TO_CHAR(JT.BUN_DT, 'YYYYMMDD')
        )
        SELECT
            COUNT(*),
            NVL(SUM(NVL(B.SILSAN,0) + NVL(B.SASAN,0) + NVL(B.MILA,0)), 0),
            NVL(SUM(NVL(B.SILSAN, 0)), 0),
            NVL(SUM(NVL(B.SASAN, 0)), 0),
            NVL(SUM(NVL(B.MILA, 0)), 0),
            NVL(SUM(NVL(B.SILSAN, 0) - NVL(PO.PS_DS, 0) + NVL(PO.JI_DS, 0) - NVL(PO.JC_DS, 0)), 0),
            NVL(ROUND(AVG(NVL(B.SILSAN,0) + NVL(B.SASAN,0) + NVL(B.MILA,0)), 1), 0),
            NVL(ROUND(AVG(NVL(B.SILSAN, 0)), 1), 0),
            NVL(ROUND(AVG(NVL(B.SASAN, 0)), 1), 0),
            NVL(ROUND(AVG(NVL(B.MILA, 0)), 1), 0),
            NVL(ROUND(AVG(NVL(B.SILSAN, 0) - NVL(PO.PS_DS, 0) + NVL(PO.JI_DS, 0) - NVL(PO.JC_DS, 0)), 1), 0)
        FROM TB_MODON_WK A
        INNER JOIN TB_BUNMAN B
            ON B.FARM_NO = A.FARM_NO AND B.PIG_NO = A.PIG_NO
           AND B.WK_DT = A.WK_DT AND B.WK_GUBUN = A.WK_GUBUN AND B.USE_YN = 'Y'
        LEFT OUTER JOIN JADON_POGAE_AGG PO
            ON PO.FARM_NO = A.FARM_NO AND PO.PIG_NO = A.PIG_NO AND PO.BUN_DT = A.WK_DT
        WHERE A.FARM_NO = :farm_no
          AND A.WK_GUBUN = 'B'
          AND A.USE_YN = 'Y'
          AND A.WK_DT >= :dt_from
          AND A.WK_DT <= :dt_to
        """
        result = self.fetch_one(sql, {'farm_no': self.farm_no, 'dt_from': dt_from, 'dt_to': dt_to})

        stats = {
            'total_cnt': result[0] if result else 0,
            'sum_total': result[1] if result else 0,
            'sum_live': result[2] if result else 0,
            'sum_dead': result[3] if result else 0,
            'sum_mummy': result[4] if result else 0,
            'sum_pogae': result[5] if result else 0,
            'avg_total': result[6] if result else 0,
            'avg_live': result[7] if result else 0,
            'avg_dead': result[8] if result else 0,
            'avg_mummy': result[9] if result else 0,
            'avg_pogae': result[10] if result else 0,
        }

        # INSERT
        sql_ins = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SORT_NO,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6, CNT_7,
            VAL_1, VAL_2, VAL_3, VAL_4, VAL_5
        ) VALUES (
            :master_seq, :farm_no, 'BM', 1,
            :total_cnt, :sum_total, :sum_live, :sum_dead, :sum_mummy, :sum_pogae, :plan_bm,
            :avg_total, :avg_live, :avg_dead, :avg_mummy, :avg_pogae
        )
        """
        self.execute(sql_ins, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'plan_bm': plan_bm,
            'total_cnt': stats.get('total_cnt', 0),
            'sum_total': stats.get('sum_total', 0),
            'sum_live': stats.get('sum_live', 0),
            'sum_dead': stats.get('sum_dead', 0),
            'sum_mummy': stats.get('sum_mummy', 0),
            'sum_pogae': stats.get('sum_pogae', 0),
            'avg_total': stats.get('avg_total', 0),
            'avg_live': stats.get('avg_live', 0),
            'avg_dead': stats.get('avg_dead', 0),
            'avg_mummy': stats.get('avg_mummy', 0),
            'avg_pogae': stats.get('avg_pogae', 0),
        })

        return stats

    def _update_week(self, stats: Dict[str, Any], acc_stats: Dict[str, Any]) -> None:
        """TS_INS_WEEK 분만 관련 컬럼 업데이트"""
        sql = """
        UPDATE TS_INS_WEEK
        SET LAST_BM_CNT = :total_cnt,
            LAST_BM_TOTAL = :sum_total,
            LAST_BM_LIVE = :sum_live,
            LAST_BM_DEAD = :sum_dead,
            LAST_BM_MUMMY = :sum_mummy,
            LAST_BM_AVG_TOTAL = :avg_total,
            LAST_BM_AVG_LIVE = :avg_live,
            LAST_BM_SUM_CNT = :acc_bm_cnt,
            LAST_BM_SUM_TOTAL = :acc_total,
            LAST_BM_SUM_LIVE = :acc_live,
            LAST_BM_SUM_AVG_TOTAL = :acc_avg_total,
            LAST_BM_SUM_AVG_LIVE = :acc_avg_live
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'total_cnt': stats.get('total_cnt', 0),
            'sum_total': stats.get('sum_total', 0),
            'sum_live': stats.get('sum_live', 0),
            'sum_dead': stats.get('sum_dead', 0),
            'sum_mummy': stats.get('sum_mummy', 0),
            'avg_total': stats.get('avg_total', 0),
            'avg_live': stats.get('avg_live', 0),
            'acc_bm_cnt': acc_stats.get('acc_bm_cnt', 0),
            'acc_total': acc_stats.get('acc_total', 0),
            'acc_live': acc_stats.get('acc_live', 0),
            'acc_avg_total': acc_stats.get('acc_avg_total', 0),
            'acc_avg_live': acc_stats.get('acc_avg_live', 0),
        })
