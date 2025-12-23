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

        # 2. 로드된 요약 데이터 가져오기
        loaded_data = self.get_loaded_data()
        lpd_daily = loaded_data.get('lpd_daily', [])        # 일별 요약 (7행)
        lpd_year_stats = loaded_data.get('lpd_year_stats', {})  # 연간 누계

        # 3. lpd_daily에서 주간 전체 평균 계산 (Oracle AVG_TBL과 동일)
        lpd_week_avg = self._calculate_week_avg(lpd_daily)

        # 4. 출하 ROW 크로스탭 INSERT (13행 × 7일)
        row_cnt = self._calculate_and_insert_row(lpd_daily, lpd_week_avg, dt_from, dt_to)

        # 5. 출하 통계 INSERT (ROW 데이터 기반) - Oracle SP와 동일 순서
        stats = self._calculate_and_insert_stats(lpd_year_stats, national_price, dt_from, dt_to)

        # 6. 출하 차트 INSERT (일자별 7행)
        chart_cnt = self._calculate_and_insert_chart(lpd_daily)

        # 7. 출하 산점도 INSERT (직접 SQL 조회)
        scatter_cnt = self._calculate_and_insert_scatter(dt_from, dt_to)

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

    def _calculate_week_avg(self, lpd_daily: List[Dict]) -> Dict[str, Any]:
        """lpd_daily에서 주간 전체 평균 계산 (Oracle AVG_TBL과 동일)

        가중평균 계산: 총 지육중량 / 총 두수
        """
        total_cnt = sum(d.get('CNT') or 0 for d in lpd_daily)
        total_net = sum(d.get('TOT_NET') or 0 for d in lpd_daily)

        # 평균 등지방: 데이터 있는 날만 평균 (Oracle AVG와 동일)
        valid_back = [d.get('AVG_BACK') for d in lpd_daily if d.get('AVG_BACK') is not None]

        return {
            'TOTAL_AVG_NET': oracle_round(total_net / total_cnt, 1) if total_cnt > 0 else None,
            'TOTAL_AVG_BACK': oracle_round(sum(valid_back) / len(valid_back), 1) if valid_back else None,
        }

    def _calculate_and_insert_stats(self, lpd_year_stats: Dict,
                                     national_price: int, dt_from: str, dt_to: str) -> Dict[str, Any]:
        """출하 통계 INSERT (Oracle SP와 동일: ROW INSERT 후 STAT INSERT)

        Oracle SP_INS_WEEK_SHIP_POPUP 프로시저와 동일:
        - SHIP/ROW에서 합계/평균 값을 읽어서 STAT INSERT
        - CNT_1: 출하두수, CNT_2: 당해년도누계, CNT_3: 1등급+두수
        - CNT_4: 기준출하일령, CNT_5: 평균포유기간, CNT_6: 역산일
        - VAL_1: 1등급+율, VAL_2: 평균도체중, VAL_3: 평균등지방
        - VAL_4: 내농장단가, VAL_5: 전국탕박평균단가
        - STR_1: 이유일 FROM, STR_2: 이유일 TO

        Args:
            lpd_year_stats: 연간 누계 통계 {'CNT': N, 'AVG_NET': N.N}
            national_price: 전국 평균 단가
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)
        """
        # 설정값 조회 (CONFIG에서)
        ship_day = 180
        wean_period = 21
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
        eu_days = ship_day - wean_period

        # SHIP/ROW에서 값 추출 (Oracle SP와 동일)
        sql_row = """
            SELECT NVL(MAX(CASE WHEN CODE_1 = 'BUT_CNT' THEN VAL_1 END), 0),
                   NVL(MAX(CASE WHEN CODE_1 = 'Q_11' THEN VAL_1 END), 0) +
                   NVL(MAX(CASE WHEN CODE_1 = 'Q_1' THEN VAL_1 END), 0),
                   NVL(MAX(CASE WHEN CODE_1 = 'AVG_NET' THEN VAL_3 END), 0),
                   NVL(MAX(CASE WHEN CODE_1 = 'AVG_BACK' THEN VAL_3 END), 0)
            FROM TS_INS_WEEK_SUB
            WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
              AND GUBUN = 'SHIP' AND SUB_GUBUN = 'ROW'
        """
        row_result = self.fetch_one(sql_row, {'master_seq': self.master_seq, 'farm_no': self.farm_no})
        ship_cnt = int(row_result[0] or 0) if row_result else 0
        grade1_cnt = int(row_result[1] or 0) if row_result else 0
        avg_kg = float(row_result[2] or 0) if row_result else 0
        avg_backfat = float(row_result[3] or 0) if row_result else 0

        # 1등급+ 합격율 계산
        grade1_rate = oracle_round(grade1_cnt / ship_cnt * 100, 1) if ship_cnt > 0 else 0

        # 연간 누계
        sum_cnt = int(lpd_year_stats.get('CNT') or 0)
        sum_avg_kg = float(lpd_year_stats.get('AVG_NET') or 0)

        # 이유일 계산
        str_1 = ''
        str_2 = ''
        try:
            from_date = datetime.strptime(dt_from, '%Y%m%d') - timedelta(days=eu_days)
            to_date = datetime.strptime(dt_to, '%Y%m%d') - timedelta(days=eu_days)
            str_1 = from_date.strftime('%y.%m.%d')
            str_2 = to_date.strftime('%y.%m.%d')
        except Exception:
            pass

        # 내농장 단가
        farm_price = 0
        try:
            farm_price = self.data_loader.get_farm_price(dt_from, dt_to)
        except Exception as e:
            self.logger.warning(f"내농장 단가 조회 실패: {e}")

        # INSERT
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
            'cnt_1': ship_cnt,
            'cnt_2': sum_cnt,
            'cnt_3': grade1_cnt,
            'cnt_4': ship_day,
            'cnt_5': wean_period,
            'cnt_6': eu_days,
            'val_1': grade1_rate,
            'val_2': avg_kg,
            'val_3': avg_backfat,
            'val_4': farm_price,
            'val_5': national_price,
            'str_1': str_1,
            'str_2': str_2,
        })

        return {
            'ship_cnt': ship_cnt,
            'avg_kg': avg_kg,
            'avg_backfat': avg_backfat,
            'grade1_cnt': grade1_cnt,
            'grade1_rate': grade1_rate,
            'sum_cnt': sum_cnt,
            'sum_avg_kg': sum_avg_kg,
            'national_price': national_price,
            'farm_price': farm_price,
            'ship_day': ship_day,
            'wean_period': wean_period,
            'eu_days': eu_days,
        }

    def _calculate_and_insert_chart(self, lpd_daily: List[Dict]) -> int:
        """출하 차트 INSERT (일자별 7행)

        Oracle SP_INS_WEEK_SHIP_POPUP과 동일:
        - 7일 모두 INSERT (데이터 없는 날은 NULL)
        - STR_1: 날짜 표시 (MM.DD)
        - CNT_1: 출하두수 (없으면 NULL)
        - VAL_1: 평균 NET_KG (없으면 NULL)
        - VAL_2: 평균 BACK_DEPTH (없으면 NULL)

        Args:
            lpd_daily: data_loader에서 조회된 일별 요약 데이터 (7행)

        Returns:
            INSERT된 레코드 수 (항상 7)
        """
        insert_count = 0
        sql_ins = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO, STR_1, CNT_1, VAL_1, VAL_2
        ) VALUES (
            :master_seq, :farm_no, 'SHIP', 'CHART', :sort_no, :str_1, :cnt_1, :val_1, :val_2
        )
        """

        for day_data in lpd_daily:
            day_no = day_data.get('DAY_NO', 0)
            day_cnt = day_data.get('CNT') or 0

            # 데이터 없는 날은 NULL (Oracle과 동일)
            if day_cnt > 0:
                cnt_1 = day_cnt
                val_1 = day_data.get('AVG_NET')
                val_2 = day_data.get('AVG_BACK')
            else:
                cnt_1 = None
                val_1 = None
                val_2 = None

            self.execute(sql_ins, {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'sort_no': day_no,
                'str_1': day_data.get('DT_DISP', ''),
                'cnt_1': cnt_1,
                'val_1': val_1,
                'val_2': val_2,
            })
            insert_count += 1

        return insert_count

    def _calculate_and_insert_scatter(self, dt_from: str, dt_to: str) -> int:
        """출하 산점도 INSERT (직접 SQL 조회)

        Oracle SP_INS_WEEK_SHIP_POPUP과 동일:
        - ROUND(NET_KG), ROUND(BACK_DEPTH) 단위로 GROUP BY
        - VAL_1: ROUND(NET_KG)
        - VAL_2: ROUND(BACK_DEPTH)
        - CNT_1: 건수

        Args:
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)

        Returns:
            INSERT된 레코드 수
        """
        dt_from_str = f"{dt_from[:4]}-{dt_from[4:6]}-{dt_from[6:8]}"
        dt_to_str = f"{dt_to[:4]}-{dt_to[4:6]}-{dt_to[6:8]}"

        # 산점도 데이터 조회 (Oracle SP와 동일)
        sql_scatter = """
            SELECT ROUND(NET_KG) AS NET_KG_GRP, ROUND(BACK_DEPTH) AS BACK_GRP, COUNT(*) AS CNT
            FROM TM_LPD_DATA
            WHERE FARM_NO = :farm_no AND USE_YN = 'Y'
              AND DOCHUK_DT >= :dt_from_str AND DOCHUK_DT <= :dt_to_str
              AND NET_KG IS NOT NULL AND BACK_DEPTH IS NOT NULL
            GROUP BY ROUND(NET_KG), ROUND(BACK_DEPTH)
            ORDER BY 1, 2
        """
        lpd_scatter = self.fetch_all(sql_scatter, {
            'farm_no': self.farm_no,
            'dt_from_str': dt_from_str,
            'dt_to_str': dt_to_str,
        })

        if not lpd_scatter:
            return 0

        insert_count = 0
        sql_ins = """
            INSERT INTO TS_INS_WEEK_SUB (
                MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
                VAL_1, VAL_2, CNT_1
            ) VALUES (
                :master_seq, :farm_no, 'SHIP', 'SCATTER', :sort_no,
                :val_1, :val_2, :cnt_1
            )
        """

        for sort_no, row in enumerate(lpd_scatter, start=1):
            self.execute(sql_ins, {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'sort_no': sort_no,
                'val_1': row.get('NET_KG_GRP'),
                'val_2': row.get('BACK_GRP'),
                'cnt_1': row.get('CNT'),
            })
            insert_count += 1

        return insert_count

    def _calculate_and_insert_row(self, lpd_daily: List[Dict], lpd_week_avg: Dict,
                                     dt_from: str, dt_to: str) -> int:
        """출하 ROW 크로스탭 INSERT (13행 × 7일)

        Oracle SP_INS_WEEK_SHIP_POPUP의 ROW 로직:
        - 13개 행: BUT_CNT, EU_DUSU, EU_RATIO, ONE_RATIO, Q_11, Q_1, Q_2, FEMALE, MALE, ETC, TNET_KG, AVG_NET, AVG_BACK
        - 각 행: D1~D7 (일별), VAL_1 (합계), VAL_2 (비율%), VAL_3 (평균)

        Args:
            lpd_daily: data_loader에서 조회된 일별 요약 데이터 (7행)
            lpd_week_avg: 주간 전체 평균 {'TOTAL_AVG_NET': N.N, 'TOTAL_AVG_BACK': N.N}
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)

        Returns:
            INSERT된 레코드 수 (13)
        """
        # 1. 설정값 조회 (역산일 계산용)
        ship_day = 180
        wean_period = 21
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
        eu_days = ship_day - wean_period

        # 2. 일별 이유두수 조회 (TB_EU - 역산일 기준)
        start_date = datetime.strptime(dt_from, '%Y%m%d')
        daily_eu: Dict[int, int] = {}
        for day_no in range(1, 8):  # DAY_NO 1~7
            eu_date = (start_date + timedelta(days=day_no - 1) - timedelta(days=eu_days)).strftime('%Y%m%d')
            try:
                sql_eu = """
                    SELECT NVL(SUM(NVL(DUSU, 0) + NVL(DUSU_SU, 0)), 0)
                    FROM TB_EU
                    WHERE FARM_NO = :farm_no AND WK_DT = :wk_dt
                """
                result = self.fetch_one(sql_eu, {'farm_no': self.farm_no, 'wk_dt': eu_date})
                daily_eu[day_no] = int(result[0]) if result and result[0] else 0
            except Exception:
                daily_eu[day_no] = 0

        # 3. 일별 통계 변환 (lpd_daily 데이터 사용)
        daily_stats: List[Dict] = []
        for day_data in lpd_daily:
            day_no = day_data.get('DAY_NO', 0)
            day_cnt = day_data.get('CNT') or 0
            eu_dusu = daily_eu.get(day_no, 0)

            if day_cnt == 0:
                # 출하 데이터 없음 - NULL로 처리 (Oracle과 동일)
                daily_stats.append({
                    'day_no': day_no,
                    'dt_disp': day_data.get('DT_DISP', ''),
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
                    'eu_dusu': eu_dusu,
                    'one_ratio': None,
                    'eu_ratio': None,
                })
            else:
                # 출하 데이터 있음 (lpd_daily에서 이미 계산됨)
                q_11 = day_data.get('Q_11') or 0
                q_1 = day_data.get('Q_1') or 0
                one_ratio = oracle_round((q_11 + q_1) / day_cnt * 100, 1) if day_cnt > 0 else None
                eu_ratio = oracle_round(day_cnt / eu_dusu * 100, 1) if eu_dusu > 0 else None

                daily_stats.append({
                    'day_no': day_no,
                    'dt_disp': day_data.get('DT_DISP', ''),
                    'but_cnt': day_cnt,
                    'tot_net': day_data.get('TOT_NET'),
                    'avg_net': day_data.get('AVG_NET'),
                    'avg_back': day_data.get('AVG_BACK'),
                    'q_11': q_11,
                    'q_1': q_1,
                    'q_2': day_data.get('Q_2') or 0,
                    'female': day_data.get('FEMALE') or 0,
                    'male': day_data.get('MALE') or 0,
                    'etc': day_data.get('ETC') or 0,
                    'eu_dusu': eu_dusu,
                    'one_ratio': one_ratio,
                    'eu_ratio': eu_ratio,
                })

        # 4. 합계/평균 계산
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

        # 전체 평균 (가중평균) - lpd_week_avg에서 가져옴 (Oracle AVG_TBL과 동일)
        total_avg_net = lpd_week_avg.get('TOTAL_AVG_NET')
        total_avg_back = lpd_week_avg.get('TOTAL_AVG_BACK')

        # 5. ROW 정의 (13행)
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
            (12, 'AVG_NET', lambda d: d['avg_net'], None, None, total_avg_net),  # 가중평균 (Oracle AVG_TBL)
            (13, 'AVG_BACK', lambda d: d['avg_back'], None, None, a_back),  # 일별 평균의 평균
        ]

        # 6. INSERT 실행
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

        # 날짜 표시용 (lpd_daily에서 추출)
        date_disp = [d['dt_disp'] for d in daily_stats]
        if len(date_disp) < 7:
            # 데이터 부족시 빈 문자열로 채움
            date_disp.extend([''] * (7 - len(date_disp)))

        for rn, code, get_val, val_1, val_2, val_3 in row_defs:
            # 일별 값 추출
            daily_vals = [get_val(d) for d in daily_stats]
            if len(daily_vals) < 7:
                daily_vals.extend([None] * (7 - len(daily_vals)))

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
