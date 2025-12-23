"""
도태폐사 팝업 데이터 추출 프로세서
SP_INS_WEEK_DOPE_POPUP 프로시저 Python 전환

아키텍처 v2:
- FarmDataLoader에서 로드된 데이터를 Python으로 가공
- SQL 조회 제거, INSERT/UPDATE만 수행
- Oracle 의존도 최소화

역할:
- 도태폐사 통계 (GUBUN='DOPE', SUB_GUBUN='STAT')
  - SORT_NO=1: 지난주 유형별 통계
  - SORT_NO=2: 최근1개월 + 당해년도 누계
- 도태폐사 원인별 목록 (GUBUN='DOPE', SUB_GUBUN='LIST')
- 도태폐사 상태별 차트 (GUBUN='DOPE', SUB_GUBUN='CHART')
- TS_INS_WEEK 도태폐사 관련 컬럼 업데이트

OUT_GUBUN_CD 4개 유형:
  CNT_1: 도태(080001)  CNT_2: 폐사(080002)  CNT_3: 전출(080003)  CNT_4: 판매(080004)

STATUS_CODE 7개 상태:
  CNT_1: 후보돈(010001)   CNT_2: 임신돈(010002)   CNT_3: 포유돈(010003)
  CNT_4: 대리모돈(010004) CNT_5: 이유모돈(010005) CNT_6: 재발돈(010006)
  CNT_7: 유산돈(010007)
"""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from .base import BaseProcessor

logger = logging.getLogger(__name__)

# 도폐사 유형 코드
OUT_GUBUN_DOTAE = '080001'    # 도태
OUT_GUBUN_PYESA = '080002'    # 폐사
OUT_GUBUN_JEONCHUL = '080003' # 전출
OUT_GUBUN_PANMAE = '080004'   # 판매

OUT_GUBUN_ORDER = [OUT_GUBUN_DOTAE, OUT_GUBUN_PYESA, OUT_GUBUN_JEONCHUL, OUT_GUBUN_PANMAE]

# 상태 코드
STATUS_ORDER = ['010001', '010002', '010003', '010004', '010005', '010006', '010007']


class CullingProcessor(BaseProcessor):
    """도태폐사 팝업 프로세서 (v2 - Python 가공)"""

    PROC_NAME = 'CullingProcessor'

    def process(self, dt_from: str, dt_to: str, **kwargs) -> Dict[str, Any]:
        """도태폐사 데이터 추출

        Args:
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)

        Returns:
            처리 결과 딕셔너리
        """
        self.logger.info(f"도태폐사 팝업 시작: 농장={self.farm_no}, 기간={dt_from}~{dt_to}")

        # 최근1개월 시작일 (dt_from - 30일)
        month_from = (datetime.strptime(dt_from, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')
        # 당해년도 시작일
        year_from = dt_to[:4] + '0101'

        # 1. 기존 데이터 삭제
        self._delete_existing()

        # 2. 도폐사 모돈 데이터 조회 (Python에서 처리)
        culled_modon = self._get_culled_modon()

        # 3. 기간별 필터링
        week_modon = self._filter_by_out_period(culled_modon, dt_from, dt_to)
        month_modon = self._filter_by_out_period(culled_modon, month_from, dt_to)
        year_modon = self._filter_by_out_period(culled_modon, year_from, dt_to)

        # 4. 집계 데이터 (전체 도폐사)
        week_total = len(week_modon)
        year_total = len(year_modon)

        # 5. 유형별 통계 INSERT (SORT_NO=1: 지난주, SORT_NO=2: 최근1개월)
        self._insert_stats_python(week_modon, month_modon, year_total)

        # 6. 원인별 목록 INSERT (LIST)
        list_cnt = self._insert_list_python(week_modon, month_modon)

        # 7. 상태별 차트 INSERT (CHART)
        self._insert_chart_python(week_modon)

        # 8. TS_INS_WEEK 업데이트 (도태+폐사만 카운트)
        self._update_week(week_modon, year_modon)

        self.logger.info(f"도태폐사 팝업 완료: 농장={self.farm_no}, 지난주도폐사={week_total}")

        return {
            'status': 'success',
            'week_total': week_total,
            'year_total': year_total,
            'list_cnt': list_cnt,
        }

    def _get_culled_modon(self) -> List[Dict]:
        """도폐사된 모돈 데이터 조회

        FarmDataLoader.get_culled_modon() 사용:
        - data_loader에서 이미 2년 이내 모돈 조회 (WK_GUBUN <> 'Z' 조건 포함)
        - Oracle SF_GET_MODONGB_STATUS 함수로 CALC_STATUS_CD 계산됨
        - OUT_DT <= base_date 조건으로 도폐사 모돈만 필터링

        Returns:
            도폐사 모돈 리스트
        """
        if self.data_loader:
            return self.data_loader.get_culled_modon()
        return []

    def _filter_by_out_period(self, modon_list: List[Dict],
                               dt_from: str, dt_to: str) -> List[Dict]:
        """OUT_DT 기준 기간 필터링

        Args:
            modon_list: 모돈 리스트
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)

        Returns:
            필터링된 모돈 리스트
        """
        result = []
        for modon in modon_list:
            out_dt = modon.get('OUT_DT')
            if not out_dt:
                continue

            # 날짜 형식 변환
            if hasattr(out_dt, 'strftime'):
                out_dt_str = out_dt.strftime('%Y%m%d')
            else:
                out_dt_str = str(out_dt).replace('-', '')[:8]

            if dt_from <= out_dt_str <= dt_to:
                result.append(modon)

        return result

    def _delete_existing(self) -> None:
        """기존 DOPE 데이터 삭제"""
        sql = """
        DELETE FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no AND GUBUN = 'DOPE'
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

    def _insert_stats_python(self, week_modon: List[Dict], month_modon: List[Dict],
                              year_total: int) -> None:
        """유형별 통계 INSERT (Python 가공)

        Args:
            week_modon: 지난주 도폐사 모돈
            month_modon: 최근 1개월 도폐사 모돈
            year_total: 당해년도 누계
        """
        # SORT_NO=1: 지난주
        week_counts = self._count_by_out_gubun(week_modon)
        total_cnt = sum(week_counts)

        week_vals = [round(cnt / total_cnt * 100, 1) if total_cnt > 0 else 0
                     for cnt in week_counts]

        sql_ins1 = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            CNT_1, CNT_2, CNT_3, CNT_4,
            VAL_1, VAL_2, VAL_3, VAL_4
        ) VALUES (
            :master_seq, :farm_no, 'DOPE', 'STAT', 1,
            :cnt_1, :cnt_2, :cnt_3, :cnt_4,
            :val_1, :val_2, :val_3, :val_4
        )
        """
        self.execute(sql_ins1, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'cnt_1': week_counts[0], 'cnt_2': week_counts[1],
            'cnt_3': week_counts[2], 'cnt_4': week_counts[3],
            'val_1': week_vals[0], 'val_2': week_vals[1],
            'val_3': week_vals[2], 'val_4': week_vals[3],
        })

        # SORT_NO=2: 최근1개월
        month_counts = self._count_by_out_gubun(month_modon)
        month_total = sum(month_counts)

        month_vals = [round(cnt / month_total * 100, 1) if month_total > 0 else 0
                      for cnt in month_counts]

        sql_ins2 = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5,
            VAL_1, VAL_2, VAL_3, VAL_4
        ) VALUES (
            :master_seq, :farm_no, 'DOPE', 'STAT', 2,
            :cnt_1, :cnt_2, :cnt_3, :cnt_4, :cnt_5,
            :val_1, :val_2, :val_3, :val_4
        )
        """
        self.execute(sql_ins2, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'cnt_1': month_counts[0], 'cnt_2': month_counts[1],
            'cnt_3': month_counts[2], 'cnt_4': month_counts[3],
            'cnt_5': year_total,
            'val_1': month_vals[0], 'val_2': month_vals[1],
            'val_3': month_vals[2], 'val_4': month_vals[3],
        })

    def _count_by_out_gubun(self, modon_list: List[Dict]) -> List[int]:
        """도폐사 유형별 개수 집계

        Returns:
            [도태, 폐사, 전출, 판매] 개수 리스트
        """
        counts = {code: 0 for code in OUT_GUBUN_ORDER}
        for modon in modon_list:
            code = str(modon.get('OUT_GUBUN_CD', ''))
            if code in counts:
                counts[code] += 1

        return [counts[code] for code in OUT_GUBUN_ORDER]

    def _insert_list_python(self, week_modon: List[Dict], month_modon: List[Dict]) -> int:
        """원인별 목록 INSERT (Python 가공)

        Args:
            week_modon: 지난주 도폐사 모돈
            month_modon: 최근 1개월 도폐사 모돈

        Returns:
            INSERT된 행 수
        """
        # 원인별 집계
        reason_stats = {}

        for modon in month_modon:
            reason_cd = str(modon.get('OUT_REASON_CD', '') or '031001')  # 기타
            if reason_cd not in reason_stats:
                reason_stats[reason_cd] = {'week': 0, 'month': 0}
            reason_stats[reason_cd]['month'] += 1

        for modon in week_modon:
            reason_cd = str(modon.get('OUT_REASON_CD', '') or '031001')
            if reason_cd in reason_stats:
                reason_stats[reason_cd]['week'] += 1

        # 정렬 (기타(031001)는 마지막, 나머지는 월간 개수 내림차순)
        sorted_reasons = sorted(
            reason_stats.items(),
            key=lambda x: (1 if x[0] == '031001' else 0, -x[1]['month'], -x[1]['week'], x[0])
        )

        if not sorted_reasons:
            return 0

        # 15개씩 그룹화하여 INSERT
        insert_count = 0
        for grp_no, i in enumerate(range(0, len(sorted_reasons), 15), 1):
            group = sorted_reasons[i:i+15]

            params = {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'sort_no': grp_no,
            }

            # 15개 컬럼 채우기
            for j in range(15):
                if j < len(group):
                    reason_cd, stats = group[j]
                    params[f'str_{j+1}'] = reason_cd
                    params[f'cnt_{j+1}'] = stats['week']
                    params[f'val_{j+1}'] = stats['month']
                else:
                    params[f'str_{j+1}'] = None
                    params[f'cnt_{j+1}'] = 0
                    params[f'val_{j+1}'] = 0

            sql = """
            INSERT INTO TS_INS_WEEK_SUB (
                MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
                STR_1, STR_2, STR_3, STR_4, STR_5, STR_6, STR_7, STR_8, STR_9, STR_10,
                STR_11, STR_12, STR_13, STR_14, STR_15,
                CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6, CNT_7, CNT_8, CNT_9, CNT_10,
                CNT_11, CNT_12, CNT_13, CNT_14, CNT_15,
                VAL_1, VAL_2, VAL_3, VAL_4, VAL_5, VAL_6, VAL_7, VAL_8, VAL_9, VAL_10,
                VAL_11, VAL_12, VAL_13, VAL_14, VAL_15
            ) VALUES (
                :master_seq, :farm_no, 'DOPE', 'LIST', :sort_no,
                :str_1, :str_2, :str_3, :str_4, :str_5, :str_6, :str_7, :str_8, :str_9, :str_10,
                :str_11, :str_12, :str_13, :str_14, :str_15,
                :cnt_1, :cnt_2, :cnt_3, :cnt_4, :cnt_5, :cnt_6, :cnt_7, :cnt_8, :cnt_9, :cnt_10,
                :cnt_11, :cnt_12, :cnt_13, :cnt_14, :cnt_15,
                :val_1, :val_2, :val_3, :val_4, :val_5, :val_6, :val_7, :val_8, :val_9, :val_10,
                :val_11, :val_12, :val_13, :val_14, :val_15
            )
            """
            self.execute(sql, params)
            insert_count += 1

        return insert_count

    def _insert_chart_python(self, week_modon: List[Dict]) -> None:
        """상태별 차트 INSERT (Python 가공)

        Args:
            week_modon: 지난주 도폐사 모돈
        """
        # 상태별 집계
        status_counts = {code: 0 for code in STATUS_ORDER}
        for modon in week_modon:
            calc_status = str(modon.get('CALC_STATUS_CD', ''))
            if calc_status in status_counts:
                status_counts[calc_status] += 1

        counts = [status_counts[code] for code in STATUS_ORDER]

        sql_ins = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            STR_1, STR_2, STR_3, STR_4, STR_5, STR_6, STR_7,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6, CNT_7
        ) VALUES (
            :master_seq, :farm_no, 'DOPE', 'CHART', 1,
            '010001', '010002', '010003', '010004', '010005', '010006', '010007',
            :cnt_1, :cnt_2, :cnt_3, :cnt_4, :cnt_5, :cnt_6, :cnt_7
        )
        """
        self.execute(sql_ins, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'cnt_1': counts[0], 'cnt_2': counts[1], 'cnt_3': counts[2],
            'cnt_4': counts[3], 'cnt_5': counts[4], 'cnt_6': counts[5],
            'cnt_7': counts[6],
        })

    def _update_week(self, week_modon: List[Dict], year_modon: List[Dict]) -> None:
        """TS_INS_WEEK 도태폐사 관련 컬럼 업데이트

        Oracle과 동일하게 OUT_GUBUN_CD가 있는 모든 모돈 카운트
        (도태/폐사/전출/판매 모두 포함)

        Args:
            week_modon: 지난주 도폐사 모돈
            year_modon: 당해년도 도폐사 모돈
        """
        # OUT_GUBUN_CD가 있는 모든 모돈 카운트 (Oracle V_WEEK_TOTAL, V_YEAR_TOTAL과 동일)
        # 도태(080001), 폐사(080002), 전출(080003), 판매(080004) 모두 포함
        week_cl_cnt = len(week_modon)
        year_cl_cnt = len(year_modon)

        sql = """
        UPDATE TS_INS_WEEK
        SET LAST_CL_CNT = :week_total,
            LAST_CL_SUM = :year_total
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'week_total': week_cl_cnt,
            'year_total': year_cl_cnt,
        })
