"""
금주 예정 팝업 데이터 추출 프로세서
SP_INS_WEEK_SCHEDULE_POPUP 프로시저 Python 전환

아키텍처 v2:
- FN_MD_SCHEDULE_BSE_2020 Oracle Function 직접 호출
- 예정 계산은 Oracle Function 사용 (Python 가공 안함)
- INSERT/UPDATE만 Python에서 수행

역할:
- 금주 예정 요약 (GUBUN='SCHEDULE', SUB_GUBUN='-')
- 금주 예정 캘린더 (GUBUN='SCHEDULE', SUB_GUBUN='CAL')
- 팝업 상세 (SUB_GUBUN='GB/BM/EU/VACCINE')
- TS_INS_WEEK 금주 예정 관련 컬럼 업데이트

예정 유형 (FN_MD_SCHEDULE_BSE_2020 JOB_GUBUN_CD):
- 150005: 교배예정 (후보돈+이유돈+사고돈)
- 150002: 분만예정 (임신돈)
- 150003: 이유예정 (포유돈+대리모돈)
- 150004: 백신예정 (전체)
"""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .base import BaseProcessor

logger = logging.getLogger(__name__)


class ScheduleProcessor(BaseProcessor):
    """금주 예정 팝업 프로세서 (v2 - Oracle Function 호출)"""

    PROC_NAME = 'ScheduleProcessor'

    def process(self, dt_from: str, dt_to: str, **kwargs) -> Dict[str, Any]:
        """금주 예정 데이터 추출

        Args:
            dt_from: 시작일 (YYYYMMDD) - 금주 월요일
            dt_to: 종료일 (YYYYMMDD) - 금주 일요일

        Returns:
            처리 결과 딕셔너리
        """
        self.logger.info(f"금주 예정 팝업 시작: 농장={self.farm_no}, 기간={dt_from}~{dt_to}")

        # 날짜 형식 변환 (FN_MD_SCHEDULE_BSE_2020용: yyyy-MM-dd)
        v_sdt = f"{dt_from[:4]}-{dt_from[4:6]}-{dt_from[6:8]}"
        v_edt = f"{dt_to[:4]}-{dt_to[4:6]}-{dt_to[6:8]}"

        # 1. 농장 설정값 조회 (CONFIG에서)
        config = self._get_config()

        # 2. 기존 데이터 삭제
        self._delete_existing()

        # 3. 요일별 날짜 배열 생성
        dt_from_obj = datetime.strptime(dt_from, '%Y%m%d')
        dates = [dt_from_obj + timedelta(days=i) for i in range(7)]

        # 4. FN_MD_SCHEDULE_BSE_2020 호출하여 예정 데이터 집계
        schedule_counts = self._get_schedule_counts(v_sdt, v_edt, dates)

        # 5. 재발확인 (3주/4주) 집계
        imsin_counts = self._get_imsin_check_counts(dt_from_obj, dates)

        # 6. 출하예정 계산
        ship_sum = self._get_ship_schedule(dt_from_obj, dates, config)

        # 7. 요약 INSERT (SUB_GUBUN='-')
        stats = self._insert_summary(schedule_counts, imsin_counts, ship_sum, dt_from_obj)

        # 8. 캘린더 그리드 INSERT (SUB_GUBUN='CAL')
        self._insert_calendar(schedule_counts, imsin_counts, dates)

        # 9. 팝업 상세 INSERT (SUB_GUBUN='GB/BM/EU/VACCINE')
        self._insert_popup_details(v_sdt, v_edt, dt_from_obj)

        # 10. HELP 정보 INSERT (SUB_GUBUN='HELP')
        self._insert_help_info(config)

        # 11. TS_INS_WEEK 업데이트
        self._update_week(stats)

        self.logger.info(f"금주 예정 팝업 완료: 농장={self.farm_no}")

        return {
            'status': 'success',
            **stats,
        }

    def _get_config(self) -> Dict[str, Any]:
        """농장 설정값 조회 (CONFIG에서 저장한 값)"""
        sql = """
        SELECT NVL(CNT_1, 115) AS PREG_PERIOD,
               NVL(CNT_2, 21) AS WEAN_PERIOD,
               NVL(CNT_3, 180) AS SHIP_DAY,
               NVL(VAL_1, 85) AS REARING_RATE,
               STR_4 AS RATE_FROM,
               STR_5 AS RATE_TO
        FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq
          AND FARM_NO = :farm_no
          AND GUBUN = 'CONFIG'
        """
        result = self.fetch_one(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

        if result:
            return {
                'preg_period': result[0],
                'wean_period': result[1],
                'ship_day': result[2],
                'rearing_rate': result[3],
                'rate_from': result[4] or '',
                'rate_to': result[5] or '',
            }
        return {
            'preg_period': 115,
            'wean_period': 21,
            'ship_day': 180,
            'rearing_rate': 85,
            'rate_from': '',
            'rate_to': '',
        }

    def _delete_existing(self) -> None:
        """기존 SCHEDULE 데이터 삭제"""
        sql = """
        DELETE FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no AND GUBUN = 'SCHEDULE'
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

    def _get_schedule_counts(self, v_sdt: str, v_edt: str, dates: List[datetime]) -> Dict[str, Dict]:
        """FN_MD_SCHEDULE_BSE_2020 호출하여 예정 집계

        Returns:
            {
                'gb': {'sum': N, 'daily': [0,0,0,0,0,0,0]},
                'bm': {...},
                'eu': {...},
                'vaccine': {...}
            }
        """
        result = {
            'gb': {'sum': 0, 'daily': [0] * 7},
            'bm': {'sum': 0, 'daily': [0] * 7},
            'eu': {'sum': 0, 'daily': [0] * 7},
            'vaccine': {'sum': 0, 'daily': [0] * 7},
        }

        # 교배예정 (150005)
        self._count_schedule('150005', None, v_sdt, v_edt, dates, result['gb'])

        # 분만예정 (150002)
        self._count_schedule('150002', None, v_sdt, v_edt, dates, result['bm'])

        # 이유예정 (150003) - 포유돈 + 대리모돈
        self._count_schedule('150003', '010003', v_sdt, v_edt, dates, result['eu'])
        self._count_schedule('150003', '010004', v_sdt, v_edt, dates, result['eu'])

        # 백신예정 (150004)
        self._count_schedule('150004', None, v_sdt, v_edt, dates, result['vaccine'])

        return result

    def _count_schedule(self, job_gubun_cd: str, status_cd: Optional[str],
                        v_sdt: str, v_edt: str, dates: List[datetime],
                        count_dict: Dict) -> None:
        """FN_MD_SCHEDULE_BSE_2020 호출하여 카운트"""
        sql = """
        SELECT TO_DATE(PASS_DT, 'YYYY-MM-DD') AS SCH_DT
        FROM TABLE(FN_MD_SCHEDULE_BSE_2020(
            :farm_no, 'JOB-DAJANG', :job_gubun_cd, :status_cd,
            :v_sdt, :v_edt, NULL, 'ko', 'yyyy-MM-dd', '-1', NULL
        ))
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, {
                'farm_no': self.farm_no,
                'job_gubun_cd': job_gubun_cd,
                'status_cd': status_cd,
                'v_sdt': v_sdt,
                'v_edt': v_edt,
            })

            for row in cursor.fetchall():
                sch_dt = row[0]
                if sch_dt:
                    count_dict['sum'] += 1
                    # 기간 이전 데이터는 첫째 날에 합산
                    if sch_dt < dates[0]:
                        count_dict['daily'][0] += 1
                    else:
                        for i, dt in enumerate(dates):
                            if sch_dt.date() == dt.date():
                                count_dict['daily'][i] += 1
                                break
        finally:
            cursor.close()

    def _get_imsin_check_counts(self, dt_from: datetime, dates: List[datetime]) -> Dict[str, Dict]:
        """재발확인 (3주/4주) 집계

        마지막 작업이 교배(G)인 모돈 대상
        - 3주령: 교배일 + 21일
        - 4주령: 교배일 + 28일
        """
        result = {
            '3w': {'sum': 0, 'daily': [0] * 7},
            '4w': {'sum': 0, 'daily': [0] * 7},
        }

        sql = """
        SELECT TO_DATE(WK.WK_DT, 'YYYYMMDD') AS GB_DT
        FROM (
            SELECT FARM_NO, PIG_NO, MAX(SEQ) AS MSEQ
            FROM TB_MODON_WK
            WHERE FARM_NO = :farm_no AND USE_YN = 'Y'
            GROUP BY FARM_NO, PIG_NO
        ) LW
        INNER JOIN TB_MODON_WK WK
            ON WK.FARM_NO = LW.FARM_NO AND WK.PIG_NO = LW.PIG_NO AND WK.SEQ = LW.MSEQ
        INNER JOIN TB_MODON MD
            ON MD.FARM_NO = WK.FARM_NO AND MD.PIG_NO = WK.PIG_NO
           AND MD.USE_YN = 'Y'
           AND MD.OUT_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD')
        WHERE WK.FARM_NO = :farm_no
          AND WK.WK_GUBUN = 'G'
        """

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, {'farm_no': self.farm_no})

            for row in cursor.fetchall():
                gb_dt = row[0]
                if not gb_dt:
                    continue

                # 3주령 (교배일 + 21일)
                check_3w = gb_dt + timedelta(days=21)
                for i, dt in enumerate(dates):
                    if check_3w.date() == dt.date():
                        result['3w']['daily'][i] += 1
                        result['3w']['sum'] += 1
                        break

                # 4주령 (교배일 + 28일)
                check_4w = gb_dt + timedelta(days=28)
                for i, dt in enumerate(dates):
                    if check_4w.date() == dt.date():
                        result['4w']['daily'][i] += 1
                        result['4w']['sum'] += 1
                        break
        finally:
            cursor.close()

        return result

    def _get_ship_schedule(self, dt_from: datetime, dates: List[datetime],
                           config: Dict[str, Any]) -> int:
        """출하예정 계산

        이유일 + (기준출하일령 - 평균포유기간) = 출하예정일
        출하예정두수 = 이유두수 * 이유후육성율
        """
        ship_offset = config['ship_day'] - config['wean_period']
        rearing_rate = config['rearing_rate'] / 100

        dt_to = dates[-1]

        sql = """
        SELECT NVL(ROUND(SUM(NVL(E.DUSU, 0) + NVL(E.DUSU_SU, 0)) * :rearing_rate), 0)
        FROM TB_EU E
        WHERE E.FARM_NO = :farm_no
          AND E.USE_YN = 'Y'
          AND E.WK_DT BETWEEN TO_CHAR(:dt_from - :ship_offset, 'YYYYMMDD')
                          AND TO_CHAR(:dt_to - :ship_offset, 'YYYYMMDD')
        """

        result = self.fetch_one(sql, {
            'farm_no': self.farm_no,
            'rearing_rate': rearing_rate,
            'dt_from': dt_from,
            'dt_to': dt_to,
            'ship_offset': ship_offset,
        })

        return result[0] if result and result[0] else 0

    def _insert_summary(self, schedule_counts: Dict, imsin_counts: Dict,
                        ship_sum: int, dt_from: datetime) -> Dict[str, int]:
        """요약 INSERT (SUB_GUBUN='-')"""
        week_num = int(dt_from.strftime('%V'))
        period_from = dt_from.strftime('%m.%d')
        period_to = (dt_from + timedelta(days=6)).strftime('%m.%d')

        imsin_sum = imsin_counts['3w']['sum'] + imsin_counts['4w']['sum']

        stats = {
            'gb_sum': schedule_counts['gb']['sum'],
            'imsin_sum': imsin_sum,
            'bm_sum': schedule_counts['bm']['sum'],
            'eu_sum': schedule_counts['eu']['sum'],
            'vaccine_sum': schedule_counts['vaccine']['sum'],
            'ship_sum': ship_sum,
        }

        sql = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6, CNT_7,
            STR_1, STR_2
        ) VALUES (
            :master_seq, :farm_no, 'SCHEDULE', '-', 1,
            :gb_sum, :imsin_sum, :bm_sum, :eu_sum, :vaccine_sum, :ship_sum, :week_num,
            :period_from, :period_to
        )
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'week_num': week_num,
            'period_from': period_from,
            'period_to': period_to,
            **stats,
        })

        return stats

    def _insert_calendar(self, schedule_counts: Dict, imsin_counts: Dict,
                         dates: List[datetime]) -> None:
        """캘린더 그리드 INSERT (SUB_GUBUN='CAL')"""
        cal_data = [
            (1, 'GB', schedule_counts['gb']['daily']),
            (2, 'BM', schedule_counts['bm']['daily']),
            (3, 'IMSIN_3W', imsin_counts['3w']['daily']),
            (4, 'IMSIN_4W', imsin_counts['4w']['daily']),
            (5, 'EU', schedule_counts['eu']['daily']),
            (6, 'VACCINE', schedule_counts['vaccine']['daily']),
        ]

        sql = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO, CODE_1,
            STR_1, STR_2, STR_3, STR_4, STR_5, STR_6, STR_7,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6, CNT_7
        ) VALUES (
            :master_seq, :farm_no, 'SCHEDULE', 'CAL', :sort_no, :code_1,
            :str_1, :str_2, :str_3, :str_4, :str_5, :str_6, :str_7,
            :cnt_1, :cnt_2, :cnt_3, :cnt_4, :cnt_5, :cnt_6, :cnt_7
        )
        """

        for sort_no, code_1, daily in cal_data:
            self.execute(sql, {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'sort_no': sort_no,
                'code_1': code_1,
                'str_1': dates[0].strftime('%d'),
                'str_2': dates[1].strftime('%d'),
                'str_3': dates[2].strftime('%d'),
                'str_4': dates[3].strftime('%d'),
                'str_5': dates[4].strftime('%d'),
                'str_6': dates[5].strftime('%d'),
                'str_7': dates[6].strftime('%d'),
                'cnt_1': daily[0],
                'cnt_2': daily[1],
                'cnt_3': daily[2],
                'cnt_4': daily[3],
                'cnt_5': daily[4],
                'cnt_6': daily[5],
                'cnt_7': daily[6],
            })

    def _insert_popup_details(self, v_sdt: str, v_edt: str, dt_from: datetime) -> None:
        """팝업 상세 INSERT (SUB_GUBUN='GB/BM/EU/VACCINE')

        TB_PLAN_MODON 기준으로 작업명별 그룹화
        """
        # GB, BM, EU는 공통 메소드 사용
        popup_configs = [
            ('GB', '150005'),
            ('BM', '150002'),
            ('EU', '150003'),
        ]

        for sub_gubun, job_gubun_cd in popup_configs:
            self._insert_popup_by_job(sub_gubun, job_gubun_cd, v_sdt, v_edt, dt_from)

        # VACCINE은 ARTICLE_NM(백신명) 포함하므로 별도 처리
        self._insert_vaccine_popup(v_sdt, v_edt, dt_from)

    def _insert_popup_by_job(self, sub_gubun: str, job_gubun_cd: str,
                              v_sdt: str, v_edt: str, dt_from: datetime) -> None:
        """작업유형별 팝업 상세 INSERT"""
        sql = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            STR_1, STR_2, STR_3, STR_4, CNT_1,
            CNT_2, CNT_3, CNT_4, CNT_5, CNT_6, CNT_7, CNT_8
        )
        SELECT :master_seq, :farm_no, 'SCHEDULE', :sub_gubun, ROWNUM,
               WK_NM, STD_CD, MODON_STATUS_CD, PASS_DAY || '일', NVL(CNT, 0),
               NVL(D1, 0), NVL(D2, 0), NVL(D3, 0), NVL(D4, 0), NVL(D5, 0), NVL(D6, 0), NVL(D7, 0)
        FROM (
            SELECT P.WK_NM, P.STD_CD, P.MODON_STATUS_CD, P.PASS_DAY,
                   S.CNT, S.D1, S.D2, S.D3, S.D4, S.D5, S.D6, S.D7
            FROM TB_PLAN_MODON P
            LEFT JOIN (
                SELECT WK_NM,
                       COUNT(*) CNT,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) < :dt_from THEN 1
                                WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from THEN 1 ELSE 0 END) AS D1,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 1 THEN 1 ELSE 0 END) AS D2,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 2 THEN 1 ELSE 0 END) AS D3,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 3 THEN 1 ELSE 0 END) AS D4,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 4 THEN 1 ELSE 0 END) AS D5,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 5 THEN 1 ELSE 0 END) AS D6,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 6 THEN 1 ELSE 0 END) AS D7
                FROM TABLE(FN_MD_SCHEDULE_BSE_2020(
                    :farm_no, 'JOB-DAJANG', :job_gubun_cd, NULL,
                    :v_sdt, :v_edt, NULL, 'ko', 'yyyy-MM-dd', '-1', NULL
                ))
                GROUP BY WK_NM
            ) S ON P.WK_NM = S.WK_NM
            WHERE P.FARM_NO = :farm_no
              AND P.JOB_GUBUN_CD = :job_gubun_cd
              AND P.USE_YN = 'Y'
            ORDER BY P.WK_NM
        )
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'sub_gubun': sub_gubun,
            'job_gubun_cd': job_gubun_cd,
            'v_sdt': v_sdt,
            'v_edt': v_edt,
            'dt_from': dt_from,
        })

    def _insert_vaccine_popup(self, v_sdt: str, v_edt: str, dt_from: datetime) -> None:
        """백신예정 팝업 상세 INSERT (SUB_GUBUN='VACCINE')

        ARTICLE_NM(백신명) 포함하여 INSERT
        """
        sql = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            STR_1, STR_2, STR_3, STR_4, STR_5, CNT_1,
            CNT_2, CNT_3, CNT_4, CNT_5, CNT_6, CNT_7, CNT_8
        )
        SELECT :master_seq, :farm_no, 'SCHEDULE', 'VACCINE', ROWNUM,
               WK_NM, STD_CD, MODON_STATUS_CD, PASS_DAY || '일', ARTICLE_NM, NVL(CNT, 0),
               NVL(D1, 0), NVL(D2, 0), NVL(D3, 0), NVL(D4, 0), NVL(D5, 0), NVL(D6, 0), NVL(D7, 0)
        FROM (
            SELECT P.WK_NM, P.STD_CD, P.MODON_STATUS_CD, P.PASS_DAY,
                   NVL(S.ARTICLE_NM, '-') AS ARTICLE_NM,
                   S.CNT, S.D1, S.D2, S.D3, S.D4, S.D5, S.D6, S.D7
            FROM TB_PLAN_MODON P
            LEFT JOIN (
                SELECT WK_NM, ARTICLE_NM,
                       COUNT(*) CNT,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from THEN 1 ELSE 0 END) AS D1,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 1 THEN 1 ELSE 0 END) AS D2,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 2 THEN 1 ELSE 0 END) AS D3,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 3 THEN 1 ELSE 0 END) AS D4,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 4 THEN 1 ELSE 0 END) AS D5,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 5 THEN 1 ELSE 0 END) AS D6,
                       SUM(CASE WHEN TRUNC(TO_DATE(PASS_DT, 'YYYY-MM-DD')) = :dt_from + 6 THEN 1 ELSE 0 END) AS D7
                FROM TABLE(FN_MD_SCHEDULE_BSE_2020(
                    :farm_no, 'JOB-DAJANG', '150004', NULL,
                    :v_sdt, :v_edt, NULL, 'ko', 'yyyy-MM-dd', '-1', NULL
                ))
                GROUP BY WK_NM, ARTICLE_NM
            ) S ON P.WK_NM = S.WK_NM
            WHERE P.FARM_NO = :farm_no
              AND P.JOB_GUBUN_CD = '150004'
              AND P.USE_YN = 'Y'
            ORDER BY P.WK_NM
        )
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'v_sdt': v_sdt,
            'v_edt': v_edt,
            'dt_from': dt_from,
        })

    def _insert_help_info(self, config: Dict[str, Any]) -> None:
        """HELP 정보 INSERT (SUB_GUBUN='HELP')"""
        ship_offset = config['ship_day'] - config['wean_period']

        sql = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            STR_1, STR_2, STR_3, STR_4, STR_5, STR_6
        )
        SELECT :master_seq, :farm_no, 'SCHEDULE', 'HELP', 1,
               (SELECT LISTAGG(WK_NM || '(' || PASS_DAY || '일)', ',') WITHIN GROUP (ORDER BY WK_NM)
                FROM TB_PLAN_MODON WHERE FARM_NO = :farm_no AND JOB_GUBUN_CD = '150005' AND USE_YN = 'Y'),
               (SELECT LISTAGG(WK_NM || '(' || PASS_DAY || '일)', ',') WITHIN GROUP (ORDER BY WK_NM)
                FROM TB_PLAN_MODON WHERE FARM_NO = :farm_no AND JOB_GUBUN_CD = '150002' AND USE_YN = 'Y'),
               (SELECT LISTAGG(WK_NM || '(' || PASS_DAY || '일)', ',') WITHIN GROUP (ORDER BY WK_NM)
                FROM TB_PLAN_MODON WHERE FARM_NO = :farm_no AND JOB_GUBUN_CD = '150003' AND USE_YN = 'Y'),
               (SELECT LISTAGG(WK_NM || '(' || PASS_DAY || '일)', ',') WITHIN GROUP (ORDER BY WK_NM)
                FROM TB_PLAN_MODON WHERE FARM_NO = :farm_no AND JOB_GUBUN_CD = '150004' AND USE_YN = 'Y'),
               '* 공식: (이유두수 × 이유후육성율)' || CHR(10) ||
               '* 이유일 = 출하예정일 - (기준출하일령 ' || :ship_day || '일 - 평균포유기간 ' || :wean_period || '일)' || CHR(10) ||
               '  (설정값: ' || :ship_day || ' - ' || :wean_period || ' = ' || :ship_offset || '일 전)' || CHR(10) ||
               '* 이유후육성율: ' || :rearing_rate || '% (' || :rate_from || '~' || :rate_to || ' 평균, 기본 85%)',
               '(고정)교배후 3주(21일~27일), 4주(28일~35일) 대상모돈'
        FROM DUAL
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'ship_day': config['ship_day'],
            'wean_period': config['wean_period'],
            'ship_offset': ship_offset,
            'rearing_rate': config['rearing_rate'],
            'rate_from': config['rate_from'],
            'rate_to': config['rate_to'],
        })

    def _update_week(self, stats: Dict[str, int]) -> None:
        """TS_INS_WEEK 금주 예정 관련 컬럼 업데이트"""
        sql = """
        UPDATE TS_INS_WEEK
        SET THIS_GB_SUM = :gb_sum,
            THIS_IMSIN_SUM = :imsin_sum,
            THIS_BM_SUM = :bm_sum,
            THIS_EU_SUM = :eu_sum,
            THIS_VACCINE_SUM = :vaccine_sum,
            THIS_SHIP_SUM = :ship_sum
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            **stats,
        })
