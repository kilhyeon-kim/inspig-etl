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
        """연간 누적 교배복수 조회 (1/1 ~ 기준일)

        data_loader.gb 데이터를 필터링하여 계산
        """
        year_start = dt_to[:4] + '0101'

        if not self.data_loader:
            return 0

        data = self.data_loader.get_data()
        gb_list = data.get('gb', [])

        # 연간 교배 데이터 필터링
        filtered = [
            g for g in gb_list
            if g.get('GB_DT') and year_start <= str(g['GB_DT'])[:8] <= dt_to
        ]

        return len(filtered)

    def _insert_stats(self, dt_from: str, dt_to: str, plan_hubo: int, plan_js: int, acc_gb_cnt: int) -> Dict[str, Any]:
        """교배 요약 통계 집계 및 INSERT

        data_loader.modon_wk 데이터를 필터링하여 계산
        - PREV_*: 이전 작업 정보 (SEQ-1) - 재귀일 계산용
        - NEXT_*: 다음 작업 정보 (SEQ+1) - 사고/분만 여부 판단용
        """
        if not self.data_loader:
            stats = {'total_cnt': 0, 'sago_cnt': 0, 'bunman_cnt': 0, 'avg_return': 0,
                     'avg_first_gb': 0, 'first_gb_cnt': 0, 'sago_gb_cnt': 0, 'js_gb_cnt': 0}
        else:
            from datetime import datetime
            data = self.data_loader.get_data()
            modon_wk = data.get('modon_wk', [])
            modon_list = data.get('modon', [])

            # 모돈 생년월일 딕셔너리
            modon_birth = {str(m.get('MODON_NO', '')): m.get('BIRTH_DT') for m in modon_list}

            # 주간 교배 데이터 필터링 (WK_GUBUN = 'G')
            week_gb = [
                wk for wk in modon_wk
                if wk.get('WK_GUBUN') == 'G'
                and wk.get('WK_DT') and dt_from <= str(wk['WK_DT'])[:8] <= dt_to
            ]

            total_cnt = len(week_gb)

            # 다음 작업이 사고(F)인 건수
            sago_cnt = sum(1 for wk in week_gb if wk.get('NEXT_WK_GUBUN') == 'F')

            # 다음 작업이 분만(B)인 건수
            bunman_cnt = sum(1 for wk in week_gb if wk.get('NEXT_WK_GUBUN') == 'B')

            # 재귀일 계산 (정상교배만: GYOBAE_CNT=1, 초교배 제외: NOT(SANCHA=0 AND GYOBAE_CNT=1))
            return_days = []
            for wk in week_gb:
                gyobae_cnt = wk.get('GYOBAE_CNT') or 0
                sancha = wk.get('SANCHA') or 0
                prev_wk_dt = wk.get('PREV_WK_DT')
                wk_dt = wk.get('WK_DT')

                # 정상교배 (GYOBAE_CNT=1) AND 이전 작업일 있음 AND 초교배 아님
                if gyobae_cnt == 1 and prev_wk_dt and not (sancha == 0 and gyobae_cnt == 1):
                    try:
                        cur_dt = datetime.strptime(str(wk_dt)[:8], '%Y%m%d')
                        prv_dt = datetime.strptime(str(prev_wk_dt)[:8], '%Y%m%d')
                        return_days.append((cur_dt - prv_dt).days)
                    except (ValueError, TypeError):
                        pass

            avg_return = round(sum(return_days) / len(return_days), 1) if return_days else 0

            # 초교배일령 계산 (SANCHA=0 AND GYOBAE_CNT=1)
            first_gb_days = []
            first_gb_cnt = 0
            for wk in week_gb:
                sancha = wk.get('SANCHA') or 0
                gyobae_cnt = wk.get('GYOBAE_CNT') or 0
                if sancha == 0 and gyobae_cnt == 1:
                    first_gb_cnt += 1
                    modon_no = str(wk.get('MODON_NO', ''))
                    birth_dt = modon_birth.get(modon_no)
                    wk_dt = wk.get('WK_DT')
                    if birth_dt and wk_dt:
                        try:
                            cur_dt = datetime.strptime(str(wk_dt)[:8], '%Y%m%d')
                            bir_dt = datetime.strptime(str(birth_dt)[:8], '%Y%m%d')
                            first_gb_days.append((cur_dt - bir_dt).days)
                        except (ValueError, TypeError):
                            pass

            avg_first_gb = round(sum(first_gb_days) / len(first_gb_days), 1) if first_gb_days else 0

            # 재교배 건수 (GYOBAE_CNT > 1)
            sago_gb_cnt = sum(1 for wk in week_gb if (wk.get('GYOBAE_CNT') or 0) > 1)

            # 정상교배 건수 (GYOBAE_CNT = 1)
            js_gb_cnt = sum(1 for wk in week_gb if (wk.get('GYOBAE_CNT') or 0) == 1)

            stats = {
                'total_cnt': total_cnt,
                'sago_cnt': sago_cnt,
                'bunman_cnt': bunman_cnt,
                'avg_return': avg_return,
                'avg_first_gb': avg_first_gb,
                'first_gb_cnt': first_gb_cnt,
                'sago_gb_cnt': sago_gb_cnt,
                'js_gb_cnt': js_gb_cnt,
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
            'total_cnt': stats.get('total_cnt', 0),
            'sago_cnt': stats.get('sago_cnt', 0),
            'bunman_cnt': stats.get('bunman_cnt', 0),
            'avg_return': stats.get('avg_return', 0),
            'avg_first_gb': stats.get('avg_first_gb', 0),
            'first_gb_cnt': stats.get('first_gb_cnt', 0),
            'sago_gb_cnt': stats.get('sago_gb_cnt', 0),
            'js_gb_cnt': stats.get('js_gb_cnt', 0),
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
