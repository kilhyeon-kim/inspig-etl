"""
출하 팝업 데이터 추출 프로세서
SP_INS_WEEK_SHIP_POPUP 프로시저 Python 전환

아키텍처 v2:
- FarmDataLoader에서 로드된 데이터를 Python으로 가공
- SQL 조회 제거, INSERT/UPDATE만 수행
- Oracle 의존도 최소화

역할:
- 출하 통계 (GUBUN='SHIP', SUB_GUBUN='STAT')
- 출하 차트 (GUBUN='SHIP', SUB_GUBUN='CHART') - 일자별
- 출하 산점도 (GUBUN='SHIP', SUB_GUBUN='SCATTER') - 규격×중량
- TS_INS_WEEK 출하 관련 컬럼 업데이트
"""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from .base import BaseProcessor

logger = logging.getLogger(__name__)

# 등급 매핑
GRADE_MAPPING = {
    '1+': '1등급↑',
    '1': '1등급↑',
    '2': '2등급',
}

# 중량 범위 정의
KG_RANGES = [
    (None, 90, '~90'),
    (90, 100, '90~100'),
    (100, 110, '100~110'),
    (110, 120, '110~120'),
    (120, None, '120↑'),
]


class ShipmentProcessor(BaseProcessor):
    """출하 팝업 프로세서 (v2 - Python 가공)"""

    PROC_NAME = 'ShipmentProcessor'

    def process(self, dt_from: str, dt_to: str, **kwargs) -> Dict[str, Any]:
        """출하 데이터 추출

        Args:
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)
            national_price: 전국 탕박 평균 단가 (선택)

        Returns:
            처리 결과 딕셔너리
        """
        national_price = kwargs.get('national_price', 0)
        self.logger.info(f"출하 팝업 시작: 농장={self.farm_no}, 기간={dt_from}~{dt_to}, 전국단가={national_price}")

        # 1. 기존 데이터 삭제
        self._delete_existing()

        # 2. 로드된 데이터 가져오기
        loaded_data = self.get_loaded_data()
        lpd_data = loaded_data.get('lpd', [])

        # 3. 기간별 데이터 필터링
        week_lpd = self._filter_lpd_by_period(lpd_data, dt_from, dt_to)
        year_start = dt_to[:4] + '0101'
        year_lpd = self._filter_lpd_by_period(lpd_data, year_start, dt_to)

        # 4. 출하 통계 계산 및 INSERT
        stats = self._calculate_and_insert_stats(week_lpd, year_lpd, national_price)

        # 5. 출하 차트 INSERT (일자별)
        chart_cnt = self._calculate_and_insert_chart(week_lpd)

        # 6. 출하 산점도 INSERT (규격도수 × 중량도수)
        scatter_cnt = self._calculate_and_insert_scatter(week_lpd)

        # 7. TS_INS_WEEK 업데이트
        self._update_week(stats)

        self.logger.info(f"출하 팝업 완료: 농장={self.farm_no}, 출하두수={stats.get('ship_cnt', 0)}")

        return {
            'status': 'success',
            **stats,
            'chart_cnt': chart_cnt,
            'scatter_cnt': scatter_cnt,
        }

    def _delete_existing(self) -> None:
        """기존 SHIP 데이터 삭제"""
        sql = """
        DELETE FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no AND GUBUN = 'SHIP'
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

    def _filter_lpd_by_period(self, lpd_data: List[Dict], dt_from: str, dt_to: str) -> List[Dict]:
        """기간으로 LPD 데이터 필터링

        LPD 데이터의 날짜 필드가 DOCHUK_DT (YYYY-MM-DD 형식) 또는 MEAS_DT일 수 있음
        """
        result = []
        for lpd in lpd_data:
            # DOCHUK_DT 또는 MEAS_DT 필드 확인
            dt_value = lpd.get('DOCHUK_DT') or lpd.get('MEAS_DT', '')
            if not dt_value:
                continue

            # 날짜 형식 통일 (YYYYMMDD)
            dt_str = str(dt_value).replace('-', '')[:8]

            if dt_from <= dt_str <= dt_to:
                result.append(lpd)

        return result

    def _calculate_and_insert_stats(self, week_lpd: List[Dict], year_lpd: List[Dict],
                                     national_price: int) -> Dict[str, Any]:
        """출하 통계 계산 및 INSERT

        Args:
            week_lpd: 주간 LPD 데이터
            year_lpd: 연간 LPD 데이터
            national_price: 전국 평균 단가

        Returns:
            통계 딕셔너리
        """
        # 주간 통계
        ship_cnt = len(week_lpd)

        kg_values = [lpd.get('DOCHUK_KG') or lpd.get('LPD_VAL', 0) or 0 for lpd in week_lpd]
        avg_kg = round(sum(kg_values) / len(kg_values), 1) if kg_values else 0

        amt_values = [lpd.get('GYEOK_AUCTAMT', 0) or 0 for lpd in week_lpd]
        avg_amt = round(sum(amt_values) / len(amt_values)) if amt_values else 0

        # 연간 누적 통계
        sum_cnt = len(year_lpd)
        year_kg_values = [lpd.get('DOCHUK_KG') or lpd.get('LPD_VAL', 0) or 0 for lpd in year_lpd]
        sum_avg_kg = round(sum(year_kg_values) / len(year_kg_values), 1) if year_kg_values else 0

        stats = {
            'ship_cnt': ship_cnt,
            'avg_kg': avg_kg,
            'avg_amt': avg_amt,
            'sum_cnt': sum_cnt,
            'sum_avg_kg': sum_avg_kg,
            'national_price': national_price,
        }

        # INSERT
        sql_ins = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            CNT_1, VAL_1, VAL_2, CNT_2, VAL_3, VAL_4
        ) VALUES (
            :master_seq, :farm_no, 'SHIP', 'STAT', 1,
            :ship_cnt, :avg_kg, :avg_amt, :sum_cnt, :sum_avg_kg, :national_price
        )
        """
        self.execute(sql_ins, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            **stats,
        })

        return stats

    def _calculate_and_insert_chart(self, week_lpd: List[Dict]) -> int:
        """출하 차트 계산 및 INSERT (일자별)

        Args:
            week_lpd: 주간 LPD 데이터

        Returns:
            INSERT된 레코드 수
        """
        if not week_lpd:
            return 0

        # 일자별 그룹핑
        daily_data: Dict[str, List[Dict]] = {}
        for lpd in week_lpd:
            dt_value = lpd.get('DOCHUK_DT') or lpd.get('MEAS_DT', '')
            if not dt_value:
                continue

            # YYYY-MM-DD 형식으로 통일
            dt_str = str(dt_value)
            if len(dt_str) == 8:  # YYYYMMDD
                dt_str = f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]}"
            else:
                dt_str = dt_str[:10]  # YYYY-MM-DD

            if dt_str not in daily_data:
                daily_data[dt_str] = []
            daily_data[dt_str].append(lpd)

        # 날짜순 정렬 및 INSERT
        insert_count = 0
        for sort_no, dt_str in enumerate(sorted(daily_data.keys()), 1):
            day_lpd = daily_data[dt_str]
            day_cnt = len(day_lpd)
            kg_values = [lpd.get('DOCHUK_KG') or lpd.get('LPD_VAL', 0) or 0 for lpd in day_lpd]
            avg_kg = round(sum(kg_values) / len(kg_values), 1) if kg_values else 0

            sql_ins = """
            INSERT INTO TS_INS_WEEK_SUB (
                MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO, CODE_1, CNT_1, VAL_1
            ) VALUES (
                :master_seq, :farm_no, 'SHIP', 'CHART', :sort_no, :code_1, :cnt_1, :val_1
            )
            """
            self.execute(sql_ins, {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'sort_no': sort_no,
                'code_1': dt_str,
                'cnt_1': day_cnt,
                'val_1': avg_kg,
            })
            insert_count += 1

        return insert_count

    def _calculate_and_insert_scatter(self, week_lpd: List[Dict]) -> int:
        """출하 산점도 계산 및 INSERT (규격도수 × 중량도수)

        Args:
            week_lpd: 주간 LPD 데이터

        Returns:
            INSERT된 레코드 수
        """
        if not week_lpd:
            return 0

        # 등급×중량 그룹핑
        scatter_data: Dict[tuple, int] = {}

        for lpd in week_lpd:
            # 등급 처리
            grade = str(lpd.get('GYEOK_GRADE', '') or '')
            grade_label = GRADE_MAPPING.get(grade, '등외')

            # 중량 처리
            kg = lpd.get('DOCHUK_KG') or lpd.get('LPD_VAL', 0) or 0

            # 중량 범위 결정
            kg_label = '등외'
            for min_kg, max_kg, label in KG_RANGES:
                if min_kg is None:
                    if kg < max_kg:
                        kg_label = label
                        break
                elif max_kg is None:
                    if kg >= min_kg:
                        kg_label = label
                        break
                else:
                    if min_kg <= kg < max_kg:
                        kg_label = label
                        break

            # 그룹 카운트
            key = (grade_label, kg_label)
            scatter_data[key] = scatter_data.get(key, 0) + 1

        # 정렬 및 INSERT
        insert_count = 0
        sort_no = 1
        for (grade_label, kg_label), cnt in sorted(scatter_data.items()):
            sql_ins = """
            INSERT INTO TS_INS_WEEK_SUB (
                MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
                CODE_1, CODE_2, CNT_1
            ) VALUES (
                :master_seq, :farm_no, 'SHIP', 'SCATTER', :sort_no,
                :code_1, :code_2, :cnt_1
            )
            """
            self.execute(sql_ins, {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'sort_no': sort_no,
                'code_1': grade_label,
                'code_2': kg_label,
                'cnt_1': cnt,
            })
            insert_count += 1
            sort_no += 1

        return insert_count

    def _update_week(self, stats: Dict[str, Any]) -> None:
        """TS_INS_WEEK 출하 관련 컬럼 업데이트"""
        sql = """
        UPDATE TS_INS_WEEK
        SET LAST_SH_CNT = :ship_cnt,
            LAST_SH_AVG_KG = :avg_kg,
            LAST_SH_SUM = :sum_cnt,
            LAST_SH_AVG_SUM = :sum_avg_kg
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            **stats,
        })
