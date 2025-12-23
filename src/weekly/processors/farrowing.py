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
        """연간 누적 실적 조회 (1/1 ~ 기준일)

        data_loader.bunman 데이터를 필터링하여 계산
        """
        year_start = dt_to[:4] + '0101'

        if not self.data_loader:
            return {'acc_bm_cnt': 0, 'acc_total': 0, 'acc_live': 0, 'acc_avg_total': 0, 'acc_avg_live': 0}

        data = self.data_loader.get_data()
        bunman_list = data.get('bunman', [])

        # 연간 데이터 필터링 (BUN_DT 기준)
        filtered = [
            b for b in bunman_list
            if b.get('BUN_DT') and year_start <= str(b['BUN_DT'])[:8] <= dt_to
        ]

        if not filtered:
            return {'acc_bm_cnt': 0, 'acc_total': 0, 'acc_live': 0, 'acc_avg_total': 0, 'acc_avg_live': 0}

        acc_bm_cnt = len(filtered)
        acc_total = sum((b.get('SILSAN') or 0) + (b.get('SASAN') or 0) + (b.get('MUMMY') or 0) for b in filtered)
        acc_live = sum(b.get('SILSAN') or 0 for b in filtered)
        acc_avg_total = round(acc_total / acc_bm_cnt, 1) if acc_bm_cnt > 0 else 0
        acc_avg_live = round(acc_live / acc_bm_cnt, 1) if acc_bm_cnt > 0 else 0

        return {
            'acc_bm_cnt': acc_bm_cnt,
            'acc_total': acc_total,
            'acc_live': acc_live,
            'acc_avg_total': acc_avg_total,
            'acc_avg_live': acc_avg_live,
        }

    def _insert_stats(self, dt_from: str, dt_to: str, plan_bm: int, acc_stats: Dict) -> Dict[str, Any]:
        """분만 통계 집계 및 INSERT (포유개시 포함)

        data_loader.bunman, jadon_trans 데이터를 필터링하여 계산
        """
        if not self.data_loader:
            stats = {'total_cnt': 0, 'sum_total': 0, 'sum_live': 0, 'sum_dead': 0,
                     'sum_mummy': 0, 'sum_pogae': 0, 'avg_total': 0, 'avg_live': 0,
                     'avg_dead': 0, 'avg_mummy': 0, 'avg_pogae': 0}
        else:
            data = self.data_loader.get_data()
            bunman_list = data.get('bunman', [])
            jadon_trans = data.get('jadon_trans', [])

            # 주간 분만 데이터 필터링
            week_bunman = [
                b for b in bunman_list
                if b.get('BUN_DT') and dt_from <= str(b['BUN_DT'])[:8] <= dt_to
            ]

            # 자돈 증감 집계 (모돈+분만일 기준)
            jadon_agg = {}  # {(modon_no, bun_dt): {'ps': 0, 'ji': 0, 'jc': 0}}
            for jt in jadon_trans:
                modon_no = str(jt.get('MODON_NO', ''))
                bun_dt = str(jt.get('BUN_DT', '') or '')[:8]
                gubun_cd = str(jt.get('TRANS_GUBUN_CD', '') or '')
                trans_cnt = jt.get('TRANS_CNT') or 0

                if not modon_no or not bun_dt:
                    continue

                key = (modon_no, bun_dt)
                if key not in jadon_agg:
                    jadon_agg[key] = {'ps': 0, 'ji': 0, 'jc': 0}

                if gubun_cd == '160001':  # 포유사고
                    jadon_agg[key]['ps'] += trans_cnt
                elif gubun_cd == '160003':  # 자돈입
                    jadon_agg[key]['ji'] += trans_cnt
                elif gubun_cd == '160004':  # 자돈출
                    jadon_agg[key]['jc'] += trans_cnt

            # 통계 계산
            total_cnt = len(week_bunman)
            sum_live = sum(b.get('SILSAN') or 0 for b in week_bunman)
            sum_dead = sum(b.get('SASAN') or 0 for b in week_bunman)
            sum_mummy = sum(b.get('MUMMY') or 0 for b in week_bunman)
            sum_total = sum_live + sum_dead + sum_mummy

            # 포유개시 계산: 실산 - 포유사고 + 자돈입 - 자돈출
            pogae_list = []
            for b in week_bunman:
                modon_no = str(b.get('MODON_NO', ''))
                bun_dt = str(b.get('BUN_DT', '') or '')[:8]
                silsan = b.get('SILSAN') or 0
                key = (modon_no, bun_dt)
                adj = jadon_agg.get(key, {'ps': 0, 'ji': 0, 'jc': 0})
                pogae = silsan - adj['ps'] + adj['ji'] - adj['jc']
                pogae_list.append(pogae)

            sum_pogae = sum(pogae_list)

            # 평균 계산
            avg_total = round(sum_total / total_cnt, 1) if total_cnt > 0 else 0
            avg_live = round(sum_live / total_cnt, 1) if total_cnt > 0 else 0
            avg_dead = round(sum_dead / total_cnt, 1) if total_cnt > 0 else 0
            avg_mummy = round(sum_mummy / total_cnt, 1) if total_cnt > 0 else 0
            avg_pogae = round(sum_pogae / total_cnt, 1) if total_cnt > 0 else 0

            stats = {
                'total_cnt': total_cnt,
                'sum_total': sum_total,
                'sum_live': sum_live,
                'sum_dead': sum_dead,
                'sum_mummy': sum_mummy,
                'sum_pogae': sum_pogae,
                'avg_total': avg_total,
                'avg_live': avg_live,
                'avg_dead': avg_dead,
                'avg_mummy': avg_mummy,
                'avg_pogae': avg_pogae,
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
