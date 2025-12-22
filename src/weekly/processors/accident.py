"""
임신사고 팝업 데이터 추출 프로세서
SP_INS_WEEK_SG_POPUP 프로시저 Python 전환

아키텍처 v2:
- FarmDataLoader에서 로드된 데이터를 Python으로 가공
- FarmDataLoader에서 계산된 마지막 교배일(CALC_LAST_GB_DT) 활용
- SQL 조회 제거, INSERT/UPDATE만 수행
- Oracle 의존도 최소화

역할:
- 임신사고 통계 (GUBUN='SG', SUB_GUBUN='STAT')
  - SORT_NO=1: 지난주 원인별 사고복수
  - SORT_NO=2: 최근1개월/당해년도 원인별 사고복수
- 임신사고 차트 (GUBUN='SG', SUB_GUBUN='CHART')
  - 경과일별 사고복수 차트
- TS_INS_WEEK 임신사고 관련 컬럼 업데이트

SAGO_GUBUN_CD 8개 유형:
  CNT_1: 재발(050008)  CNT_2: 불임(050009)  CNT_3: 공태(050007)  CNT_4: 유산(050002)
  CNT_5: 도태(050003)  CNT_6: 폐사(050004)  CNT_7: 임돈전출(050005)  CNT_8: 임돈판매(050006)
"""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from .base import BaseProcessor

logger = logging.getLogger(__name__)

# 사고구분 코드 매핑
SAGO_CODES = {
    '050008': 'jaebal',     # 재발
    '050009': 'bulim',      # 불임
    '050007': 'gongtae',    # 공태
    '050002': 'yusan',      # 유산
    '050003': 'dotae',      # 도태
    '050004': 'pyesa',      # 폐사
    '050005': 'jeonchul',   # 임돈전출
    '050006': 'panmae',     # 임돈판매
}

# CNT 순서에 맞는 코드 리스트
SAGO_CODE_ORDER = ['050008', '050009', '050007', '050002', '050003', '050004', '050005', '050006']

# 경과일 제외 코드 (임돈전출/판매)
EXCLUDE_GYUNGIL_CODES = ['050005', '050006']


class AccidentProcessor(BaseProcessor):
    """임신사고 팝업 프로세서 (v2 - Python 가공)"""

    PROC_NAME = 'AccidentProcessor'

    def process(self, dt_from: str, dt_to: str, **kwargs) -> Dict[str, Any]:
        """임신사고 데이터 추출

        Args:
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)

        Returns:
            처리 결과 딕셔너리
        """
        self.logger.info(f"임신사고 팝업 시작: 농장={self.farm_no}, 기간={dt_from}~{dt_to}")

        # 최근1개월 시작일 (dt_from - 30일)
        month_from = (datetime.strptime(dt_from, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')
        # 당해년도 시작일
        year_from = dt_to[:4] + '0101'

        # 1. 기존 데이터 삭제
        self._delete_existing()

        # 2. 로드된 데이터 가져오기
        loaded_data = self.get_loaded_data()
        sago_data = loaded_data.get('sago', [])

        # 3. 사고 데이터 전처리 - 경과일 계산 (FarmDataLoader 활용)
        processed_sago = self._preprocess_sago(sago_data)

        # 4. 지난주 원인별 사고복수 계산 및 INSERT (SORT_NO=1)
        week_sago = self.filter_by_period(processed_sago, 'SAGO_DT', dt_from, dt_to)
        week_stats = self._calculate_and_insert_stats(week_sago, sort_no=1)

        # 5. 최근1개월/당해년도 원인별 사고복수 계산 및 INSERT (SORT_NO=2)
        month_sago = self.filter_by_period(processed_sago, 'SAGO_DT', month_from, dt_to)
        year_sago = self.filter_by_period(processed_sago, 'SAGO_DT', year_from, dt_to)
        year_stats = self._calculate_and_insert_year_stats(month_sago, year_sago)

        # 6. 경과일별 사고복수 차트 INSERT
        chart_cnt = self._calculate_and_insert_chart(week_sago)

        # 7. TS_INS_WEEK 업데이트
        self._update_week(week_stats, year_stats)

        self.logger.info(f"임신사고 팝업 완료: 농장={self.farm_no}, 지난주사고={week_stats.get('total_cnt', 0)}")

        return {
            'status': 'success',
            'week_stats': week_stats,
            'year_stats': year_stats,
            'chart_cnt': chart_cnt,
        }

    def _delete_existing(self) -> None:
        """기존 SG 데이터 삭제"""
        sql = """
        DELETE FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no AND GUBUN = 'SG'
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

    def _preprocess_sago(self, sago_data: List[Dict]) -> List[Dict]:
        """사고 데이터 전처리 - 경과일 계산

        FarmDataLoader에서 계산된 마지막 교배일을 활용하여 경과일 계산
        사고별로 그 시점의 마지막 교배일 기준으로 경과일 계산

        Args:
            sago_data: 사고 데이터 리스트

        Returns:
            전처리된 사고 데이터 리스트
        """
        loaded_data = self.get_loaded_data()
        modon_wk = loaded_data.get('modon_wk', [])
        modon_last_gb = loaded_data.get('modon_last_gb_dt', {})

        # 모돈별 교배 작업 리스트 생성 (교배 작업: WK_GUBUN='G')
        modon_gb_dates: Dict[str, List[str]] = {}
        for wk in modon_wk:
            if wk.get('WK_GUBUN') == 'G':
                modon_no = str(wk.get('MODON_NO', ''))
                wk_dt = str(wk.get('WK_DT', ''))
                if modon_no and wk_dt:
                    if modon_no not in modon_gb_dates:
                        modon_gb_dates[modon_no] = []
                    modon_gb_dates[modon_no].append(wk_dt)

        # 각 모돈의 교배일 정렬
        for modon_no in modon_gb_dates:
            modon_gb_dates[modon_no].sort()

        result = []
        for sago in sago_data:
            sago_copy = dict(sago)
            modon_no = str(sago.get('MODON_NO', ''))
            sago_dt = str(sago.get('SAGO_DT', ''))

            # 마지막 교배일 찾기 (사고일 이전의 가장 최근 교배일)
            last_gb_dt = None
            if modon_no in modon_gb_dates:
                for gb_dt in reversed(modon_gb_dates[modon_no]):
                    if gb_dt < sago_dt:
                        last_gb_dt = gb_dt
                        break

            # 마지막 교배일이 없으면 FarmDataLoader에서 계산된 값 사용
            if not last_gb_dt:
                last_gb_dt = modon_last_gb.get(modon_no, '')

            sago_copy['CALC_LAST_GB_DT'] = last_gb_dt

            # 경과일 계산
            if last_gb_dt and sago_dt:
                gyungil = self.calculate_date_diff(str(last_gb_dt), sago_dt)
                sago_copy['GYUNGIL'] = gyungil if gyungil > 0 else None
            else:
                sago_copy['GYUNGIL'] = None

            result.append(sago_copy)

        return result

    def _calculate_and_insert_stats(self, sago_list: List[Dict], sort_no: int) -> Dict[str, Any]:
        """원인별 사고복수 계산 및 INSERT

        Args:
            sago_list: 사고 데이터 리스트
            sort_no: SORT_NO 값

        Returns:
            통계 결과 딕셔너리
        """
        # 코드별 개수 집계
        code_counts = self.count_by_code(sago_list, 'SAGO_GUBUN_CD')

        # CNT_1~CNT_8 순서로 매핑
        counts = [code_counts.get(code, 0) for code in SAGO_CODE_ORDER]
        total_cnt = sum(counts)

        # 비율 계산
        vals = [round(cnt / total_cnt * 100, 1) if total_cnt > 0 else 0 for cnt in counts]

        # 평균 경과일 계산 (임돈전출/판매 제외)
        gyungil_sago = [s for s in sago_list
                        if s.get('SAGO_GUBUN_CD') not in EXCLUDE_GYUNGIL_CODES
                        and s.get('GYUNGIL') is not None]
        avg_gyungil = self.avg_field(gyungil_sago, 'GYUNGIL') if gyungil_sago else 0
        avg_gyungil = round(avg_gyungil, 1)

        # INSERT
        sql_ins = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6, CNT_7, CNT_8,
            VAL_1, VAL_2, VAL_3, VAL_4, VAL_5, VAL_6, VAL_7, VAL_8, VAL_9
        ) VALUES (
            :master_seq, :farm_no, 'SG', 'STAT', :sort_no,
            :cnt_1, :cnt_2, :cnt_3, :cnt_4, :cnt_5, :cnt_6, :cnt_7, :cnt_8,
            :val_1, :val_2, :val_3, :val_4, :val_5, :val_6, :val_7, :val_8, :avg_gyungil
        )
        """
        self.execute(sql_ins, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'sort_no': sort_no,
            'cnt_1': counts[0], 'cnt_2': counts[1], 'cnt_3': counts[2], 'cnt_4': counts[3],
            'cnt_5': counts[4], 'cnt_6': counts[5], 'cnt_7': counts[6], 'cnt_8': counts[7],
            'val_1': vals[0], 'val_2': vals[1], 'val_3': vals[2], 'val_4': vals[3],
            'val_5': vals[4], 'val_6': vals[5], 'val_7': vals[6], 'val_8': vals[7],
            'avg_gyungil': avg_gyungil,
        })

        return {
            'total_cnt': total_cnt,
            'avg_gyungil': avg_gyungil,
            **{f'cnt_{i+1}': counts[i] for i in range(8)},
        }

    def _calculate_and_insert_year_stats(self, month_sago: List[Dict],
                                          year_sago: List[Dict]) -> Dict[str, Any]:
        """최근1개월/당해년도 원인별 사고복수 계산 및 INSERT (SORT_NO=2)

        Args:
            month_sago: 최근 1개월 사고 데이터
            year_sago: 당해년도 사고 데이터

        Returns:
            통계 결과 딕셔너리
        """
        # 최근 1개월 코드별 개수
        month_code_counts = self.count_by_code(month_sago, 'SAGO_GUBUN_CD')
        month_counts = [month_code_counts.get(code, 0) for code in SAGO_CODE_ORDER]
        month_total = sum(month_counts)

        # 당해년도 누계
        year_total = len(year_sago)

        # 비율 계산 (최근 1개월 기준)
        vals = [round(cnt / month_total * 100, 1) if month_total > 0 else 0 for cnt in month_counts]

        # 당해년도 평균 경과일 계산 (임돈전출/판매 제외)
        gyungil_sago = [s for s in year_sago
                        if s.get('SAGO_GUBUN_CD') not in EXCLUDE_GYUNGIL_CODES
                        and s.get('GYUNGIL') is not None]
        avg_gyungil = self.avg_field(gyungil_sago, 'GYUNGIL') if gyungil_sago else 0
        avg_gyungil = round(avg_gyungil, 1)

        # INSERT
        sql_ins = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6, CNT_7, CNT_8, CNT_9,
            VAL_1, VAL_2, VAL_3, VAL_4, VAL_5, VAL_6, VAL_7, VAL_8, VAL_9
        ) VALUES (
            :master_seq, :farm_no, 'SG', 'STAT', 2,
            :cnt_1, :cnt_2, :cnt_3, :cnt_4, :cnt_5, :cnt_6, :cnt_7, :cnt_8, :cnt_9,
            :val_1, :val_2, :val_3, :val_4, :val_5, :val_6, :val_7, :val_8, :avg_gyungil
        )
        """
        self.execute(sql_ins, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'cnt_1': month_counts[0], 'cnt_2': month_counts[1],
            'cnt_3': month_counts[2], 'cnt_4': month_counts[3],
            'cnt_5': month_counts[4], 'cnt_6': month_counts[5],
            'cnt_7': month_counts[6], 'cnt_8': month_counts[7],
            'cnt_9': year_total,
            'val_1': vals[0], 'val_2': vals[1], 'val_3': vals[2], 'val_4': vals[3],
            'val_5': vals[4], 'val_6': vals[5], 'val_7': vals[6], 'val_8': vals[7],
            'avg_gyungil': avg_gyungil,
        })

        return {
            'year_total': year_total,
            'avg_gyungil': avg_gyungil,
        }

    def _calculate_and_insert_chart(self, sago_list: List[Dict]) -> int:
        """경과일별 사고복수 차트 계산 및 INSERT

        경과일 범위:
        - CNT_1: ~7일
        - CNT_2: 8~10일
        - CNT_3: 11~15일
        - CNT_4: 16~20일
        - CNT_5: 21~35일
        - CNT_6: 36~40일
        - CNT_7: 41~45일
        - CNT_8: 46일~

        임돈전출/판매 제외 (050005, 050006)

        Args:
            sago_list: 사고 데이터 리스트

        Returns:
            INSERT된 레코드 수
        """
        # 경과일 범위 정의
        ranges = [
            (None, 7),    # CNT_1: ~7일
            (8, 10),      # CNT_2: 8~10일
            (11, 15),     # CNT_3: 11~15일
            (16, 20),     # CNT_4: 16~20일
            (21, 35),     # CNT_5: 21~35일
            (36, 40),     # CNT_6: 36~40일
            (41, 45),     # CNT_7: 41~45일
            (46, None),   # CNT_8: 46일~
        ]

        # 경과일이 있는 사고만 필터링 (임돈전출/판매 제외)
        valid_sago = [
            s for s in sago_list
            if s.get('SAGO_GUBUN_CD') not in EXCLUDE_GYUNGIL_CODES
            and s.get('GYUNGIL') is not None
        ]

        if not valid_sago:
            return 0

        # 경과일별 집계
        counts = []
        for min_val, max_val in ranges:
            count = 0
            for s in valid_sago:
                gyungil = s.get('GYUNGIL', 0) or 0
                if min_val is None:
                    # ~7일
                    if gyungil <= max_val:
                        count += 1
                elif max_val is None:
                    # 46일~
                    if gyungil >= min_val:
                        count += 1
                else:
                    # 범위
                    if min_val <= gyungil <= max_val:
                        count += 1
            counts.append(count)

        # INSERT
        sql_ins = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6, CNT_7, CNT_8
        ) VALUES (
            :master_seq, :farm_no, 'SG', 'CHART', 1,
            :cnt_1, :cnt_2, :cnt_3, :cnt_4, :cnt_5, :cnt_6, :cnt_7, :cnt_8
        )
        """
        self.execute(sql_ins, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'cnt_1': counts[0], 'cnt_2': counts[1], 'cnt_3': counts[2], 'cnt_4': counts[3],
            'cnt_5': counts[4], 'cnt_6': counts[5], 'cnt_7': counts[6], 'cnt_8': counts[7],
        })

        return 1

    def _update_week(self, week_stats: Dict[str, Any], year_stats: Dict[str, Any]) -> None:
        """TS_INS_WEEK 임신사고 관련 컬럼 업데이트"""
        sql = """
        UPDATE TS_INS_WEEK
        SET LAST_SG_CNT = :total_cnt,
            LAST_SG_AVG_GYUNGIL = :week_avg_gyungil,
            LAST_SG_SUM = :year_total,
            LAST_SG_SUM_AVG_GYUNGIL = :year_avg_gyungil
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'total_cnt': week_stats.get('total_cnt', 0),
            'week_avg_gyungil': week_stats.get('avg_gyungil', 0),
            'year_total': year_stats.get('year_total', 0),
            'year_avg_gyungil': year_stats.get('avg_gyungil', 0),
        })
