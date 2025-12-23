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
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta
from typing import Any, Dict, List

from .base import BaseProcessor


def oracle_round(value: float, decimals: int = 1) -> float:
    """Oracle ROUND()와 동일한 반올림 (ROUND_HALF_UP)

    Python의 round()는 banker's rounding (짝수 방향)을 사용하지만
    Oracle은 traditional rounding (5 이상이면 올림)을 사용함
    """
    if value is None:
        return 0.0
    d = Decimal(str(value))
    return float(d.quantize(Decimal(10) ** -decimals, rounding=ROUND_HALF_UP))

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
        stats = self._calculate_and_insert_stats(week_lpd, year_lpd, national_price, dt_from, dt_to)

        # 5. 출하 ROW 크로스탭 INSERT (13행 × 7일)
        row_cnt = self._calculate_and_insert_row(week_lpd, dt_from, dt_to)

        # 6. 출하 차트 INSERT (일자별)
        chart_cnt = self._calculate_and_insert_chart(week_lpd)

        # 7. 출하 산점도 INSERT (규격도수 × 중량도수)
        scatter_cnt = self._calculate_and_insert_scatter(week_lpd)

        # 8. TS_INS_WEEK 업데이트
        self._update_week(stats)

        self.logger.info(f"출하 팝업 완료: 농장={self.farm_no}, 출하두수={stats.get('ship_cnt', 0)}, ROW={row_cnt}")

        return {
            'status': 'success',
            **stats,
            'row_cnt': row_cnt,
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
                                     national_price: int, dt_from: str = '', dt_to: str = '') -> Dict[str, Any]:
        """출하 통계 계산 및 INSERT

        Oracle SP_INS_WEEK_SHIP_POPUP 프로시저와 동일한 컬럼 매핑:
        - CNT_1: 출하두수, CNT_2: 당해년도누계, CNT_3: 1등급+두수
        - CNT_4: 기준출하일령 (V_SHIP_DAY), CNT_5: 평균포유기간 (V_WEAN_PERIOD)
        - CNT_6: 역산일 (V_EU_DAYS = V_SHIP_DAY - V_WEAN_PERIOD)
        - VAL_1: 1등급+율, VAL_2: 평균도체중, VAL_3: 평균등지방
        - VAL_4: 내농장단가, VAL_5: 전국탕박평균단가
        - STR_1: 이유일 FROM, STR_2: 이유일 TO

        Args:
            week_lpd: 주간 LPD 데이터
            year_lpd: 연간 LPD 데이터
            national_price: 전국 평균 단가
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)

        Returns:
            통계 딕셔너리
        """
        # 설정값 조회 (CONFIG에서)
        ship_day = 180  # 기준출하일령
        wean_period = 21  # 평균포유기간
        try:
            sql_config = """
                SELECT NVL(CNT_3, 180), NVL(CNT_2, 21)
                FROM TS_INS_WEEK_SUB
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no AND GUBUN = 'CONFIG'
            """
            result = self.fetch_one(sql_config, {'master_seq': self.master_seq, 'farm_no': self.farm_no})
            if result:
                ship_day = result[0] or 180
                wean_period = result[1] or 21
        except Exception:
            pass
        eu_days = ship_day - wean_period  # 역산일

        # 주간 통계
        ship_cnt = len(week_lpd)

        # 평균 도체중 (NET_KG) - Oracle ROUND()와 동일
        kg_values = [lpd.get('NET_KG') or lpd.get('DOCHUK_KG') or lpd.get('LPD_VAL', 0) or 0 for lpd in week_lpd]
        avg_kg = oracle_round(sum(kg_values) / len(kg_values), 1) if kg_values else 0

        # 평균 등지방 - Oracle: BACK_DEPTH > 0 조건
        backfat_values = [lpd.get('BACK_DEPTH', 0) or 0 for lpd in week_lpd if lpd.get('BACK_DEPTH') and lpd.get('BACK_DEPTH') > 0]
        avg_backfat = oracle_round(sum(backfat_values) / len(backfat_values), 1) if backfat_values else 0

        # 1등급+ 두수 및 비율 (MEAT_QUALITY = '1+' 또는 '1')
        grade1_cnt = sum(1 for lpd in week_lpd if str(lpd.get('MEAT_QUALITY', '')).strip() in ('1+', '1'))
        grade1_rate = oracle_round(grade1_cnt / ship_cnt * 100, 1) if ship_cnt > 0 else 0

        # 연간 누적 통계
        sum_cnt = len(year_lpd)
        year_kg_values = [lpd.get('NET_KG') or lpd.get('DOCHUK_KG') or lpd.get('LPD_VAL', 0) or 0 for lpd in year_lpd]
        sum_avg_kg = oracle_round(sum(year_kg_values) / len(year_kg_values), 1) if year_kg_values else 0

        # 이유일 계산 (출하일 - 역산일)
        str_1 = ''  # 이유일 FROM
        str_2 = ''  # 이유일 TO
        if dt_from and dt_to:
            try:
                from_date = datetime.strptime(dt_from, '%Y%m%d') - timedelta(days=eu_days)
                to_date = datetime.strptime(dt_to, '%Y%m%d') - timedelta(days=eu_days)
                str_1 = from_date.strftime('%y.%m.%d')
                str_2 = to_date.strftime('%y.%m.%d')
            except Exception:
                pass

        stats = {
            'ship_cnt': ship_cnt,
            'avg_kg': avg_kg,
            'avg_backfat': avg_backfat,
            'grade1_cnt': grade1_cnt,
            'grade1_rate': grade1_rate,
            'sum_cnt': sum_cnt,
            'sum_avg_kg': sum_avg_kg,
            'national_price': national_price,
            'ship_day': ship_day,
            'wean_period': wean_period,
            'eu_days': eu_days,
        }

        # 내농장 단가 계산 (data_loader의 get_farm_price 사용)
        farm_price = 0
        try:
            farm_price = self.data_loader.get_farm_price(dt_from, dt_to)
        except Exception as e:
            self.logger.warning(f"내농장 단가 조회 실패: {e}")

        stats['farm_price'] = farm_price

        # INSERT - Oracle과 동일한 컬럼 매핑
        sql_ins = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6,
            VAL_1, VAL_2, VAL_3, VAL_4, VAL_5,
            STR_1, STR_2
        ) VALUES (
            :master_seq, :farm_no, 'SHIP', 'STAT', 1,
            :cnt_1, :cnt_2, :cnt_3, :cnt_4, :cnt_5, :cnt_6,
            :val_1, :val_2, :val_3, :val_4, :val_5,
            :str_1, :str_2
        )
        """
        self.execute(sql_ins, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'cnt_1': ship_cnt,        # 출하두수
            'cnt_2': sum_cnt,         # 당해년도 누계
            'cnt_3': grade1_cnt,      # 1등급+ 두수
            'cnt_4': ship_day,        # 기준출하일령
            'cnt_5': wean_period,     # 평균포유기간
            'cnt_6': eu_days,         # 역산일
            'val_1': grade1_rate,     # 1등급+율(%)
            'val_2': avg_kg,          # 평균도체중
            'val_3': avg_backfat,     # 평균등지방
            'val_4': farm_price,      # 내농장단가 (TM_ETC_TRADE)
            'val_5': national_price,  # 전국탕박평균단가
            'str_1': str_1,           # 이유일 FROM
            'str_2': str_2,           # 이유일 TO
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
            kg_values = [lpd.get('NET_KG') or lpd.get('DOCHUK_KG') or lpd.get('LPD_VAL', 0) or 0 for lpd in day_lpd]
            avg_kg = oracle_round(sum(kg_values) / len(kg_values), 1) if kg_values else 0

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
            # 등급 처리 (MEAT_QUALITY 또는 GYEOK_GRADE)
            grade = str(lpd.get('MEAT_QUALITY', '') or lpd.get('GYEOK_GRADE', '') or '').strip()
            grade_label = GRADE_MAPPING.get(grade, '등외')

            # 중량 처리
            kg = lpd.get('NET_KG') or lpd.get('DOCHUK_KG') or lpd.get('LPD_VAL', 0) or 0

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

    def _calculate_and_insert_row(self, week_lpd: List[Dict], dt_from: str, dt_to: str) -> int:
        """출하 ROW 크로스탭 계산 및 INSERT (13행 × 7일)

        Oracle SP_INS_WEEK_SHIP_POPUP의 ROW 로직 Python 전환:
        - 13개 행: BUT_CNT, EU_DUSU, EU_RATIO, ONE_RATIO, Q_11, Q_1, Q_2, FEMALE, MALE, ETC, TNET_KG, AVG_NET, AVG_BACK
        - 각 행: D1~D7 (일별), VAL_1 (합계), VAL_2 (비율%), VAL_3 (평균)

        Args:
            week_lpd: 주간 LPD 데이터
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)

        Returns:
            INSERT된 레코드 수 (13)
        """
        # 1. 7일간의 날짜 리스트 생성
        start_date = datetime.strptime(dt_from, '%Y%m%d')
        date_list = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
        date_disp = [(start_date + timedelta(days=i)).strftime('%m.%d') for i in range(7)]

        # 2. 설정값 조회 (역산일 계산용)
        ship_day = 180  # 기준출하일령
        wean_period = 21  # 평균포유기간
        try:
            sql_config = """
                SELECT NVL(CNT_3, 180), NVL(CNT_2, 21)
                FROM TS_INS_WEEK_SUB
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no AND GUBUN = 'CONFIG'
            """
            result = self.fetch_one(sql_config, {'master_seq': self.master_seq, 'farm_no': self.farm_no})
            if result:
                ship_day = result[0] or 180
                wean_period = result[1] or 21
        except Exception:
            pass
        eu_days = ship_day - wean_period  # 역산일

        # 3. 일별 LPD 데이터 그룹핑
        daily_lpd: Dict[int, List[Dict]] = {i: [] for i in range(7)}  # DAY_NO 0~6
        for lpd in week_lpd:
            dt_value = lpd.get('DOCHUK_DT') or lpd.get('MEAS_DT', '')
            if not dt_value:
                continue
            dt_str = str(dt_value)
            if len(dt_str) == 8:  # YYYYMMDD
                dt_str = f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]}"
            else:
                dt_str = dt_str[:10]  # YYYY-MM-DD
            if dt_str in date_list:
                day_no = date_list.index(dt_str)
                daily_lpd[day_no].append(lpd)

        # 4. 일별 이유두수 조회 (TB_EU - 역산일 기준)
        daily_eu: Dict[int, int] = {i: 0 for i in range(7)}
        for day_no in range(7):
            eu_date = (start_date + timedelta(days=day_no) - timedelta(days=eu_days)).strftime('%Y%m%d')
            try:
                sql_eu = """
                    SELECT NVL(SUM(NVL(DUSU, 0) + NVL(DUSU_SU, 0)), 0)
                    FROM TB_EU
                    WHERE FARM_NO = :farm_no AND WK_DT = :wk_dt
                """
                result = self.fetch_one(sql_eu, {'farm_no': self.farm_no, 'wk_dt': eu_date})
                if result and result[0]:
                    daily_eu[day_no] = int(result[0])
            except Exception:
                pass

        # 5. 일별 통계 계산
        daily_stats: List[Dict] = []
        for day_no in range(7):
            day_data = daily_lpd[day_no]
            day_cnt = len(day_data)

            if day_cnt == 0:
                # 출하 데이터 없음 - NULL로 처리 (Oracle과 동일)
                daily_stats.append({
                    'day_no': day_no,
                    'dt_disp': date_disp[day_no],
                    'but_cnt': None,
                    'tot_net': None,
                    'avg_net': None,
                    'avg_back': None,
                    'q_11': None,
                    'q_1': None,
                    'q_2': None,
                    'female': None,
                    'male': None,
                    'etc': None,
                    'eu_dusu': daily_eu[day_no],  # 이유는 출하와 별개
                    'one_ratio': None,
                    'eu_ratio': None,
                })
            else:
                # 출하 데이터 있음
                kg_values = [lpd.get('NET_KG') or lpd.get('DOCHUK_KG') or 0 for lpd in day_data if lpd.get('NET_KG') or lpd.get('DOCHUK_KG')]
                back_values = [lpd.get('BACK_DEPTH') or 0 for lpd in day_data if lpd.get('BACK_DEPTH') and lpd.get('BACK_DEPTH') > 0]

                tot_net = sum(kg_values) if kg_values else 0
                avg_net = oracle_round(sum(kg_values) / len(kg_values), 1) if kg_values else None
                avg_back = oracle_round(sum(back_values) / len(back_values), 1) if back_values else None

                # 등급별 두수
                q_11 = sum(1 for lpd in day_data if str(lpd.get('MEAT_QUALITY', '')).strip() == '1+')
                q_1 = sum(1 for lpd in day_data if str(lpd.get('MEAT_QUALITY', '')).strip() == '1')
                q_2 = sum(1 for lpd in day_data if str(lpd.get('MEAT_QUALITY', '')).strip() == '2')

                # 성별 두수
                female = sum(1 for lpd in day_data if str(lpd.get('SEX_GUBUN', '')).strip() == '암')
                male = sum(1 for lpd in day_data if str(lpd.get('SEX_GUBUN', '')).strip() == '수')
                sex_val = [str(lpd.get('SEX_GUBUN', '') or '').strip() for lpd in day_data]
                etc = sum(1 for s in sex_val if s == '거세' or s not in ('암', '수'))

                # 비율 계산
                one_ratio = oracle_round((q_11 + q_1) / day_cnt * 100, 1) if day_cnt > 0 else None
                eu_dusu = daily_eu[day_no]
                eu_ratio = oracle_round(day_cnt / eu_dusu * 100, 1) if eu_dusu > 0 else None

                daily_stats.append({
                    'day_no': day_no,
                    'dt_disp': date_disp[day_no],
                    'but_cnt': day_cnt,
                    'tot_net': tot_net,
                    'avg_net': avg_net,
                    'avg_back': avg_back,
                    'q_11': q_11,
                    'q_1': q_1,
                    'q_2': q_2,
                    'female': female,
                    'male': male,
                    'etc': etc,
                    'eu_dusu': eu_dusu,
                    'one_ratio': one_ratio,
                    'eu_ratio': eu_ratio,
                })

        # 6. 합계/평균 계산
        # 합계 (데이터 있는 날만)
        s_but = sum(d['but_cnt'] or 0 for d in daily_stats)
        s_eu = sum(d['eu_dusu'] or 0 for d in daily_stats)
        s_net = sum(d['tot_net'] or 0 for d in daily_stats)
        s_q11 = sum(d['q_11'] or 0 for d in daily_stats)
        s_q1 = sum(d['q_1'] or 0 for d in daily_stats)
        s_q2 = sum(d['q_2'] or 0 for d in daily_stats)
        s_fem = sum(d['female'] or 0 for d in daily_stats)
        s_male = sum(d['male'] or 0 for d in daily_stats)
        s_etc = sum(d['etc'] or 0 for d in daily_stats)

        # 평균 (데이터 있는 날만, Oracle AVG와 동일 - NULL 제외)
        valid_but = [d['but_cnt'] for d in daily_stats if d['but_cnt'] is not None]
        valid_eu = [d['eu_dusu'] for d in daily_stats if d['eu_dusu'] and d['eu_dusu'] > 0]
        valid_q11 = [d['q_11'] for d in daily_stats if d['q_11'] is not None]
        valid_q1 = [d['q_1'] for d in daily_stats if d['q_1'] is not None]
        valid_q2 = [d['q_2'] for d in daily_stats if d['q_2'] is not None]
        valid_fem = [d['female'] for d in daily_stats if d['female'] is not None]
        valid_male = [d['male'] for d in daily_stats if d['male'] is not None]
        valid_etc = [d['etc'] for d in daily_stats if d['etc'] is not None]
        valid_tot_net = [d['tot_net'] for d in daily_stats if d['tot_net'] is not None]
        valid_avg_back = [d['avg_back'] for d in daily_stats if d['avg_back'] is not None]
        valid_one_ratio = [d['one_ratio'] for d in daily_stats if d['one_ratio'] is not None]

        a_but = oracle_round(sum(valid_but) / len(valid_but), 1) if valid_but else None
        a_eu = oracle_round(sum(valid_eu) / len(valid_eu), 1) if valid_eu else None
        a_q11 = oracle_round(sum(valid_q11) / len(valid_q11), 1) if valid_q11 else None
        a_q1 = oracle_round(sum(valid_q1) / len(valid_q1), 1) if valid_q1 else None
        a_q2 = oracle_round(sum(valid_q2) / len(valid_q2), 1) if valid_q2 else None
        a_fem = oracle_round(sum(valid_fem) / len(valid_fem), 1) if valid_fem else None
        a_male = oracle_round(sum(valid_male) / len(valid_male), 1) if valid_male else None
        a_etc = oracle_round(sum(valid_etc) / len(valid_etc), 1) if valid_etc else None
        a_tot_net = oracle_round(sum(valid_tot_net) / len(valid_tot_net), 1) if valid_tot_net else None
        a_back = oracle_round(sum(valid_avg_back) / len(valid_avg_back), 1) if valid_avg_back else None
        a_one_ratio = oracle_round(sum(valid_one_ratio) / len(valid_one_ratio), 1) if valid_one_ratio else None

        # 전체 평균 (가중평균) - 원본 데이터에서 직접 계산
        all_kg = [lpd.get('NET_KG') or lpd.get('DOCHUK_KG') or 0 for lpd in week_lpd if lpd.get('NET_KG') or lpd.get('DOCHUK_KG')]
        total_avg_net = oracle_round(sum(all_kg) / len(all_kg), 1) if all_kg else None

        # 7. ROW 정의 (13행)
        row_defs = [
            # (RN, CODE, get_daily_val, val_1, val_2, val_3)
            (1, 'BUT_CNT', lambda d: d['but_cnt'], s_but, None, a_but),
            (2, 'EU_DUSU', lambda d: d['eu_dusu'], s_eu, None, a_eu),
            (3, 'EU_RATIO', lambda d: d['eu_ratio'], None, None, oracle_round(s_but / s_eu * 100, 1) if s_eu > 0 else 0),
            (4, 'ONE_RATIO', lambda d: d['one_ratio'], None, None, a_one_ratio),
            (5, 'Q_11', lambda d: d['q_11'], s_q11, oracle_round(s_q11 / s_but * 100, 1) if s_but > 0 else 0, a_q11),
            (6, 'Q_1', lambda d: d['q_1'], s_q1, oracle_round(s_q1 / s_but * 100, 1) if s_but > 0 else 0, a_q1),
            (7, 'Q_2', lambda d: d['q_2'], s_q2, oracle_round(s_q2 / s_but * 100, 1) if s_but > 0 else 0, a_q2),
            (8, 'FEMALE', lambda d: d['female'], s_fem, oracle_round(s_fem / s_but * 100, 1) if s_but > 0 else 0, a_fem),
            (9, 'MALE', lambda d: d['male'], s_male, oracle_round(s_male / s_but * 100, 1) if s_but > 0 else 0, a_male),
            (10, 'ETC', lambda d: d['etc'], s_etc, oracle_round(s_etc / s_but * 100, 1) if s_but > 0 else 0, a_etc),
            (11, 'TNET_KG', lambda d: d['tot_net'], s_net, None, a_tot_net),
            (12, 'AVG_NET', lambda d: d['avg_net'], None, None, total_avg_net),  # 가중평균
            (13, 'AVG_BACK', lambda d: d['avg_back'], None, None, a_back),
        ]

        # 8. INSERT 실행
        insert_count = 0
        sql_ins = """
            INSERT INTO TS_INS_WEEK_SUB (
                MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO, CODE_1,
                STR_1, STR_2, STR_3, STR_4, STR_5, STR_6, STR_7,
                CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6, CNT_7,
                VAL_1, VAL_2, VAL_3
            ) VALUES (
                :master_seq, :farm_no, 'SHIP', 'ROW', :sort_no, :code_1,
                :str_1, :str_2, :str_3, :str_4, :str_5, :str_6, :str_7,
                :cnt_1, :cnt_2, :cnt_3, :cnt_4, :cnt_5, :cnt_6, :cnt_7,
                :val_1, :val_2, :val_3
            )
        """

        for rn, code, get_val, val_1, val_2, val_3 in row_defs:
            # 일별 값 추출
            daily_vals = [get_val(d) for d in daily_stats]

            self.execute(sql_ins, {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'sort_no': rn,
                'code_1': code,
                'str_1': date_disp[0],
                'str_2': date_disp[1],
                'str_3': date_disp[2],
                'str_4': date_disp[3],
                'str_5': date_disp[4],
                'str_6': date_disp[5],
                'str_7': date_disp[6],
                'cnt_1': daily_vals[0],
                'cnt_2': daily_vals[1],
                'cnt_3': daily_vals[2],
                'cnt_4': daily_vals[3],
                'cnt_5': daily_vals[4],
                'cnt_6': daily_vals[5],
                'cnt_7': daily_vals[6],
                'val_1': val_1,
                'val_2': val_2,
                'val_3': val_3,
            })
            insert_count += 1

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
            'ship_cnt': stats.get('ship_cnt', 0),
            'avg_kg': stats.get('avg_kg', 0),
            'sum_cnt': stats.get('sum_cnt', 0),
            'sum_avg_kg': stats.get('sum_avg_kg', 0),
        })
