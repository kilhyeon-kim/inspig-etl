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

TS_INS_CONF 설정 지원:
- method='farm': 농장 기본값 사용 (TC_FARM_CONFIG)
- method='modon': 모돈 작업설정 사용 (TB_PLAN_MODON), tasks에 선택된 SEQ만 필터링
"""
import json
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

        # 2. TS_INS_CONF 설정 조회 (금주 작업예정 산정방식)
        ins_conf = self._get_ins_conf()

        # 3. 기존 데이터 삭제
        self._delete_existing()

        # 4. 요일별 날짜 배열 생성
        dt_from_obj = datetime.strptime(dt_from, '%Y%m%d')
        dates = [dt_from_obj + timedelta(days=i) for i in range(7)]

        # 5. 예정 데이터 집계 (ins_conf 설정에 따라 farm/modon 분기)
        schedule_counts = self._get_schedule_counts(v_sdt, v_edt, dates, ins_conf, config)

        # 6. 재발확인 (3주/4주) 집계 - ins_conf['pregnancy'] 설정에 따라 farm/modon 분기
        imsin_counts = self._get_imsin_check_counts(v_sdt, v_edt, dt_from_obj, dates, ins_conf)

        # 7. 출하예정 계산
        ship_sum = self._get_ship_schedule(dt_from_obj, dates, config)

        # 8. 요약 INSERT (SUB_GUBUN='-')
        stats = self._insert_summary(schedule_counts, imsin_counts, ship_sum, dt_from_obj)

        # 9. 캘린더 그리드 INSERT (SUB_GUBUN='CAL')
        self._insert_calendar(schedule_counts, imsin_counts, dates, ins_conf)

        # 10. 팝업 상세 INSERT (SUB_GUBUN='GB/BM/EU/VACCINE')
        self._insert_popup_details(v_sdt, v_edt, dt_from_obj, ins_conf)

        # 11. HELP 정보 INSERT (SUB_GUBUN='HELP')
        self._insert_help_info(config, dt_from_obj, ins_conf)

        # 12. 산정방식 정보 INSERT (SUB_GUBUN='METHOD')
        self._insert_method_info(ins_conf)

        # 13. TS_INS_WEEK 업데이트
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
               NVL(VAL_1, 90) AS REARING_RATE,
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
            'rearing_rate': 90,
            'rate_from': '',
            'rate_to': '',
        }

    def _get_ins_conf(self) -> Dict[str, Dict[str, Any]]:
        """TS_INS_CONF에서 금주 작업예정 산정방식 설정 조회

        Returns:
            {
                'mating': {'method': 'farm'|'modon', 'tasks': [], 'seq_filter': '-1'},
                'farrowing': {...},
                'pregnancy': {...},
                'weaning': {...},
                'vaccine': {...}
            }
        """
        # 기본값: 교배/분만/이유/백신은 modon(전체), 임신감정은 farm
        # seq_filter: ''=작업없음(0개), '1,2,3'=선택된 작업
        default_conf = {
            'mating': {'method': 'modon', 'tasks': None, 'seq_filter': ''},
            'farrowing': {'method': 'modon', 'tasks': None, 'seq_filter': ''},
            'pregnancy': {'method': 'farm', 'tasks': None, 'seq_filter': ''},
            'weaning': {'method': 'modon', 'tasks': None, 'seq_filter': ''},
            'vaccine': {'method': 'modon', 'tasks': None, 'seq_filter': ''},
        }

        sql = """
        SELECT WEEK_TW_GY, WEEK_TW_BM, WEEK_TW_IM, WEEK_TW_EU, WEEK_TW_VC
        FROM TS_INS_CONF
        WHERE FARM_NO = :farm_no
        """
        result = self.fetch_one(sql, {'farm_no': self.farm_no})

        if not result:
            self.logger.info(f"TS_INS_CONF 설정 없음, 기본값 사용: farm_no={self.farm_no}")
            return default_conf

        # 컬럼 매핑: (인덱스, 키)
        col_map = [
            (0, 'mating'),      # WEEK_TW_GY
            (1, 'farrowing'),   # WEEK_TW_BM
            (2, 'pregnancy'),   # WEEK_TW_IM
            (3, 'weaning'),     # WEEK_TW_EU
            (4, 'vaccine'),     # WEEK_TW_VC
        ]

        for idx, key in col_map:
            json_str = result[idx]
            if json_str:
                try:
                    parsed = json.loads(json_str)
                    method = parsed.get('method', 'modon')
                    # tasks 키 존재 여부 확인: 키가 없으면 None, 있으면 값 (빈 배열 포함)
                    tasks = parsed.get('tasks') if 'tasks' in parsed else None

                    # tasks를 seq_filter 문자열로 변환
                    # - tasks 키 없음(None) → '' (JSON 오류, 작업 없음)
                    # - tasks=[] (빈 배열) → '' (작업 없음, 카운트 0)
                    # - tasks=[1,2,3] → '1,2,3' (선택된 작업)
                    if method == 'modon':
                        if tasks is None or len(tasks) == 0:
                            seq_filter = ''  # 작업 없음 (카운트 0)
                        else:
                            seq_filter = ','.join(str(t) for t in tasks)  # 선택된 작업
                    else:
                        seq_filter = '-1'  # farm 모드에서는 사용 안함

                    default_conf[key] = {
                        'method': method,
                        'tasks': tasks,
                        'seq_filter': seq_filter,
                    }
                except json.JSONDecodeError:
                    self.logger.warning(f"JSON 파싱 실패: {key}={json_str}")

        self.logger.info(f"TS_INS_CONF 설정 로드: farm_no={self.farm_no}, conf={default_conf}")
        return default_conf

    def _delete_existing(self) -> None:
        """기존 SCHEDULE 데이터 삭제"""
        sql = """
        DELETE FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no AND GUBUN = 'SCHEDULE'
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

    def _get_schedule_counts(self, v_sdt: str, v_edt: str, dates: List[datetime],
                              ins_conf: Dict[str, Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Dict]:
        """예정 집계 (TS_INS_CONF 설정에 따라 분기)

        Args:
            v_sdt: 시작일 (yyyy-MM-dd)
            v_edt: 종료일 (yyyy-MM-dd)
            dates: 요일별 날짜 리스트
            ins_conf: TS_INS_CONF 설정 (method, tasks, seq_filter)
            config: 농장 설정값 (preg_period, wean_period 등)

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

        # 농장 기본값이 필요한지 확인
        need_farm_config = (
            ins_conf['mating']['method'] == 'farm' or
            ins_conf['farrowing']['method'] == 'farm' or
            ins_conf['weaning']['method'] == 'farm'
        )

        # TC_FARM_CONFIG 한 번만 조회 (농장 기본값이 필요한 경우만)
        farm_config = None
        if need_farm_config:
            farm_config = self._get_farm_config_for_schedule()

        # 교배예정 (150005)
        if ins_conf['mating']['method'] == 'farm':
            self._count_schedule_by_farm('mating', dates, result['gb'], config, farm_config, add_early_to_first=True)
        else:
            seq_filter = ins_conf['mating']['seq_filter']
            self._count_schedule('150005', None, v_sdt, v_edt, dates, result['gb'],
                                 seq_filter=seq_filter, add_early_to_first=True)

        # 분만예정 (150002)
        if ins_conf['farrowing']['method'] == 'farm':
            self._count_schedule_by_farm('farrowing', dates, result['bm'], config, farm_config)
        else:
            seq_filter = ins_conf['farrowing']['seq_filter']
            self._count_schedule('150002', None, v_sdt, v_edt, dates, result['bm'], seq_filter=seq_filter)

        # 이유예정 (150003) - 포유돈 + 대리모돈
        if ins_conf['weaning']['method'] == 'farm':
            self._count_schedule_by_farm('weaning', dates, result['eu'], config, farm_config)
        else:
            seq_filter = ins_conf['weaning']['seq_filter']
            self._count_schedule('150003', '010003', v_sdt, v_edt, dates, result['eu'], seq_filter=seq_filter)
            self._count_schedule('150003', '010004', v_sdt, v_edt, dates, result['eu'], seq_filter=seq_filter)

        # 백신예정 (150004) - 항상 modon (농장기본값 옵션 없음)
        seq_filter = ins_conf['vaccine']['seq_filter']
        self._count_schedule('150004', None, v_sdt, v_edt, dates, result['vaccine'], seq_filter=seq_filter)

        return result

    def _get_farm_config_for_schedule(self) -> Dict[str, int]:
        """TC_FARM_CONFIG에서 금주 예정 계산에 필요한 설정값 조회 (1회 호출)

        Returns:
            {
                'avg_return_day': 평균재귀일 (140008, 기본 7일),
                'first_mating_age': 초교배일령 (140007, 기본 240일),
                'preg_period': 평균임신기간 (140002, 기본 115일),
                'wean_period': 평균포유기간 (140003, 기본 21일)
            }
        """
        sql = """
        SELECT CODE, TO_NUMBER(NVL(CVALUE, DECODE(CODE, '140002', '115', '140003', '21', '140008', '7', '140007', '240')))
        FROM TC_FARM_CONFIG
        WHERE FARM_NO = :farm_no AND CODE IN ('140002', '140003', '140007', '140008')
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, {'farm_no': self.farm_no})
            rows = cursor.fetchall()

            # 기본값 설정
            config = {
                'avg_return_day': 7,       # 140008
                'first_mating_age': 240,   # 140007
                'preg_period': 115,        # 140002
                'wean_period': 21,         # 140003
            }

            # 조회 결과로 업데이트
            code_map = {
                '140002': 'preg_period',
                '140003': 'wean_period',
                '140007': 'first_mating_age',
                '140008': 'avg_return_day',
            }
            for code, value in rows:
                if code in code_map:
                    config[code_map[code]] = int(value) if value else config[code_map[code]]

            self.logger.info(f"TC_FARM_CONFIG 조회: farm_no={self.farm_no}, config={config}")
            return config
        finally:
            cursor.close()

    def _count_schedule(self, job_gubun_cd: str, status_cd: Optional[str],
                        v_sdt: str, v_edt: str, dates: List[datetime],
                        count_dict: Dict, seq_filter: str = '-1',
                        add_early_to_first: bool = False) -> None:
        """FN_MD_SCHEDULE_BSE_2020 호출하여 카운트 (모돈 작업설정 기준)

        Args:
            job_gubun_cd: 작업구분코드
            status_cd: 상태코드 (None이면 전체)
            v_sdt: 시작일 (yyyy-MM-dd)
            v_edt: 종료일 (yyyy-MM-dd)
            dates: 요일별 날짜 리스트
            count_dict: 카운트 저장 딕셔너리
            seq_filter: TB_PLAN_MODON.SEQ 필터 ('-1'=전체, ''=작업없음, '1,2,3'=선택)
            add_early_to_first: True면 기간 이전 데이터를 첫째 날에 합산 (교배예정만 해당)
        """
        # seq_filter가 빈 문자열이면 선택된 작업이 없으므로 카운트 0
        if seq_filter == '':
            self.logger.info(f"작업 없음 (seq_filter=''), 카운트 생략: {job_gubun_cd}")
            return

        sql = """
        SELECT TO_DATE(PASS_DT, 'YYYY-MM-DD') AS SCH_DT
        FROM TABLE(FN_MD_SCHEDULE_BSE_2020(
            :farm_no, 'JOB-DAJANG', :job_gubun_cd, :status_cd,
            :v_sdt, :v_edt, NULL, 'ko', 'yyyy-MM-dd', :seq_filter, NULL
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
                'seq_filter': seq_filter,
            })

            for row in cursor.fetchall():
                sch_dt = row[0]
                if sch_dt:
                    # Oracle과 동일: 교배예정만 기간 이전 데이터를 첫째 날에 합산
                    if add_early_to_first and sch_dt < dates[0]:
                        count_dict['sum'] += 1
                        count_dict['daily'][0] += 1
                    else:
                        for i, dt in enumerate(dates):
                            if sch_dt.date() == dt.date():
                                count_dict['sum'] += 1
                                count_dict['daily'][i] += 1
                                break
        finally:
            cursor.close()

    def _count_schedule_by_farm(self, schedule_type: str, dates: List[datetime],
                                 count_dict: Dict, config: Dict[str, Any],
                                 farm_config: Dict[str, int],
                                 add_early_to_first: bool = False) -> None:
        """농장 기본값 기준으로 예정 카운트

        TC_FARM_CONFIG 설정값을 사용하여 예정일 계산:
        - 교배예정: 이유일 + 평균재귀일 (기본 7일)
        - 분만예정: 교배일 + 평균임신기간 (기본 115일)
        - 이유예정: 분만일 + 평균포유기간 (기본 21일)

        Args:
            schedule_type: 'mating', 'farrowing', 'weaning'
            dates: 요일별 날짜 리스트
            count_dict: 카운트 저장 딕셔너리
            farm_config: TC_FARM_CONFIG 설정값 (avg_return_day, first_mating_age, preg_period, wean_period)
            config: 농장 설정값
            add_early_to_first: True면 기간 이전 데이터를 첫째 날에 합산
        """
        dt_from = dates[0]
        dt_to = dates[6]

        if schedule_type == 'mating':
            # 교배예정: 이유돈/후보돈/재발돈/유산돈 중 예정일이 금주인 모돈
            # TB_MODON_WK 마지막 작업은 금주 월요일(dt_from) 이전 데이터만 조회
            # OUT_DT >= dt_from: 금주 월요일 이전까지 살아있던 모돈 (금주에 도폐사되어도 포함)
            # 이유돈: 이유일 + 평균재귀일 (TC_FARM_CONFIG.901003, 기본 7일), DAERI_YN='N'만
            # 후보돈: 생년월일(BIRTH_DT) + 초교배일령 (TC_FARM_CONFIG.901007, 기본 240일)
            # 재발/유산돈: 사고일 + 1일 (피그플랜과 동일, 즉시 교배 가능)
            # 성능: TC_FARM_CONFIG 사전 조회, WHERE 조건 컬럼 가공 없음 (인덱스 활용)
            sql = """
            WITH LAST_WK AS (
                -- 금주 월요일 이전 마지막 작업 (금주 마침일까지 살아있는 모돈만 대상)
                SELECT WK.FARM_NO, WK.PIG_NO, WK.WK_DT, WK.WK_GUBUN, WK.DAERI_YN
                FROM (
                    SELECT FARM_NO, PIG_NO, WK_DT, WK_GUBUN, DAERI_YN,
                           ROW_NUMBER() OVER (PARTITION BY FARM_NO, PIG_NO ORDER BY SEQ DESC) AS RN
                    FROM TB_MODON_WK
                    WHERE FARM_NO = :farm_no
                      AND USE_YN = 'Y'
                      AND WK_DT < TO_CHAR(:dt_from, 'YYYYMMDD')
                      AND PIG_NO IN (
                          SELECT PIG_NO FROM TB_MODON
                          WHERE FARM_NO = :farm_no AND USE_YN = 'Y' AND OUT_DT > :dt_to
                      )
                ) WK
                WHERE WK.RN = 1
            )
            SELECT PASS_DT
            FROM (
                -- 1. 이유돈: 마지막 작업이 이유(E), 대리모 아님(DAERI_YN='N')
                SELECT MD.PIG_NO,
                       TO_DATE(WK.WK_DT, 'YYYYMMDD') + :avg_return_day AS PASS_DT
                FROM TB_MODON MD
                INNER JOIN LAST_WK WK ON MD.FARM_NO = WK.FARM_NO AND MD.PIG_NO = WK.PIG_NO
                WHERE MD.FARM_NO = :farm_no
                  AND MD.USE_YN = 'Y'
                  AND MD.OUT_DT > :dt_to
                  AND WK.WK_GUBUN = 'E'
                  AND WK.DAERI_YN = 'N'
                  AND WK.WK_DT <= TO_CHAR(:dt_to - :avg_return_day, 'YYYYMMDD')
                UNION ALL
                -- 2. 이유돈: TB_MODON_WK 없고 STATUS_CD='010005' (이유상태)
                SELECT MD.PIG_NO,
                       MD.LAST_WK_DT + :avg_return_day AS PASS_DT
                FROM TB_MODON MD
                WHERE MD.FARM_NO = :farm_no
                  AND MD.USE_YN = 'Y'
                  AND MD.OUT_DT > :dt_to
                  AND MD.STATUS_CD = '010005'
                  AND MD.LAST_WK_DT IS NOT NULL
                  AND MD.LAST_WK_DT <= :dt_to - :avg_return_day
                  AND NOT EXISTS (
                      SELECT /*+ HASH_AJ */ 1 FROM TB_MODON_WK WK
                      WHERE WK.FARM_NO = :farm_no AND WK.PIG_NO = MD.PIG_NO
                        AND WK.USE_YN = 'Y' AND WK.WK_DT < TO_CHAR(:dt_from, 'YYYYMMDD')
                  )
                UNION ALL
                -- 3. 후보돈: TB_MODON_WK 없고 STATUS_CD='010001' (후보상태)
                SELECT MD.PIG_NO,
                       MD.BIRTH_DT + :first_mating_age AS PASS_DT
                FROM TB_MODON MD
                WHERE MD.FARM_NO = :farm_no
                  AND MD.USE_YN = 'Y'
                  AND MD.OUT_DT > :dt_to
                  AND MD.STATUS_CD = '010001'
                  AND MD.BIRTH_DT IS NOT NULL
                  AND MD.BIRTH_DT <= :dt_to - :first_mating_age
                  AND NOT EXISTS (
                      SELECT /*+ HASH_AJ */ 1 FROM TB_MODON_WK WK
                      WHERE WK.FARM_NO = :farm_no AND WK.PIG_NO = MD.PIG_NO
                        AND WK.USE_YN = 'Y' AND WK.WK_DT < TO_CHAR(:dt_from, 'YYYYMMDD')
                  )
                UNION ALL
                -- 4. 재발/유산돈: 마지막 작업이 사고(F)
                SELECT MD.PIG_NO,
                       TO_DATE(WK.WK_DT, 'YYYYMMDD') + 1 AS PASS_DT
                FROM TB_MODON MD
                INNER JOIN LAST_WK WK ON MD.FARM_NO = WK.FARM_NO AND MD.PIG_NO = WK.PIG_NO
                WHERE MD.FARM_NO = :farm_no
                  AND MD.USE_YN = 'Y'
                  AND MD.OUT_DT > :dt_to
                  AND WK.WK_GUBUN = 'F'
                  AND WK.WK_DT <= TO_CHAR(:dt_to - 1, 'YYYYMMDD')
                UNION ALL
                -- 5. 재발/유산돈: TB_MODON_WK 없고 STATUS_CD IN ('010006', '010007')
                SELECT MD.PIG_NO,
                       MD.LAST_WK_DT + 1 AS PASS_DT
                FROM TB_MODON MD
                WHERE MD.FARM_NO = :farm_no
                  AND MD.USE_YN = 'Y'
                  AND MD.OUT_DT > :dt_to
                  AND MD.STATUS_CD IN ('010006', '010007')
                  AND MD.LAST_WK_DT IS NOT NULL
                  AND MD.LAST_WK_DT <= :dt_to - 1
                  AND NOT EXISTS (
                      SELECT /*+ HASH_AJ */ 1 FROM TB_MODON_WK WK
                      WHERE WK.FARM_NO = :farm_no AND WK.PIG_NO = MD.PIG_NO
                        AND WK.USE_YN = 'Y' AND WK.WK_DT < TO_CHAR(:dt_from, 'YYYYMMDD')
                  )
            )
            WHERE PASS_DT <= :dt_to
            """
        elif schedule_type == 'farrowing':
            # 분만예정: 임신돈 중 교배일 + 평균임신기간 (파라미터: preg_period)
            # 금주 범위(dt_from ~ dt_to)에 해당하는 분만예정만 조회
            sql = """
            SELECT PASS_DT
            FROM (
                SELECT TO_DATE(WK.WK_DT, 'YYYYMMDD') + :preg_period AS PASS_DT
                FROM TB_MODON MD
                INNER JOIN VM_LAST_MODON_SEQ_WK WK
                    ON MD.FARM_NO = WK.FARM_NO AND MD.PIG_NO = WK.PIG_NO
                WHERE MD.FARM_NO = :farm_no
                  AND MD.USE_YN = 'Y'
                  AND MD.OUT_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD')
                  AND WK.WK_GUBUN = 'G'
                  AND SF_GET_MODONGB_STATUS('CD', WK.WK_GUBUN, NULL, MD.OUT_DT, NULL, 'N', '') = '010002'
                  AND WK.WK_DT BETWEEN TO_CHAR(:dt_from - :preg_period, 'YYYYMMDD')
                                   AND TO_CHAR(:dt_to - :preg_period, 'YYYYMMDD')
            )
            WHERE PASS_DT BETWEEN :dt_from AND :dt_to
            """
        elif schedule_type == 'weaning':
            # 이유예정: 포유돈/대리모 중 분만일 + 평균포유기간 (파라미터: wean_period)
            # 금주 범위(dt_from ~ dt_to)에 해당하는 이유예정만 조회
            # DAERI_YN은 TB_MODON_WK(VM_LAST_MODON_SEQ_WK)에 있음
            sql = """
            SELECT PASS_DT
            FROM (
                SELECT TO_DATE(WK.WK_DT, 'YYYYMMDD') + :wean_period AS PASS_DT
                FROM TB_MODON MD
                INNER JOIN VM_LAST_MODON_SEQ_WK WK
                    ON MD.FARM_NO = WK.FARM_NO AND MD.PIG_NO = WK.PIG_NO
                WHERE MD.FARM_NO = :farm_no
                  AND MD.USE_YN = 'Y'
                  AND MD.OUT_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD')
                  AND WK.WK_GUBUN = 'B'
                  AND SF_GET_MODONGB_STATUS('CD', WK.WK_GUBUN, NULL, MD.OUT_DT, NULL, WK.DAERI_YN, '') IN ('010003', '010004')
                  AND WK.WK_DT BETWEEN TO_CHAR(:dt_from - :wean_period, 'YYYYMMDD')
                                   AND TO_CHAR(:dt_to - :wean_period, 'YYYYMMDD')
            )
            WHERE PASS_DT BETWEEN :dt_from AND :dt_to
            """
        else:
            return

        cursor = self.conn.cursor()
        try:
            params = {
                'farm_no': self.farm_no,
                'dt_from': dt_from,
                'dt_to': dt_to,
            }

            # schedule_type별 farm_config 파라미터 추가
            if schedule_type == 'mating':
                params['avg_return_day'] = farm_config['avg_return_day']
                params['first_mating_age'] = farm_config['first_mating_age']
            elif schedule_type == 'farrowing':
                params['preg_period'] = farm_config['preg_period']
            elif schedule_type == 'weaning':
                params['wean_period'] = farm_config['wean_period']

            cursor.execute(sql, params)

            for row in cursor.fetchall():
                pass_dt = row[0]
                if pass_dt:
                    if add_early_to_first and pass_dt < dt_from:
                        count_dict['sum'] += 1
                        count_dict['daily'][0] += 1
                    else:
                        for i, dt in enumerate(dates):
                            if pass_dt.date() == dt.date():
                                count_dict['sum'] += 1
                                count_dict['daily'][i] += 1
                                break
        finally:
            cursor.close()

    def _get_imsin_check_counts(self, v_sdt: str, v_edt: str, dt_from: datetime,
                                  dates: List[datetime], ins_conf: Dict[str, Dict[str, Any]]) -> Dict[str, Dict]:
        """재발확인 (3주/4주) 집계

        ins_conf['pregnancy'] 설정에 따라 분기:
        - method='farm': 농장기본값 (교배일 + 21일/28일 고정)
        - method='modon': 모돈작업설정 (FN_MD_SCHEDULE_BSE_2020 호출)

        Args:
            v_sdt: 시작일 (yyyy-MM-dd)
            v_edt: 종료일 (yyyy-MM-dd)
            dt_from: 시작일 (datetime)
            dates: 요일별 날짜 리스트
            ins_conf: TS_INS_CONF 설정
        """
        result = {
            '3w': {'sum': 0, 'daily': [0] * 7},
            '4w': {'sum': 0, 'daily': [0] * 7},
        }

        pregnancy_conf = ins_conf['pregnancy']

        if pregnancy_conf['method'] == 'modon':
            # 모돈작업설정: FN_MD_SCHEDULE_BSE_2020 호출 (JOB_GUBUN_CD='150001')
            seq_filter = pregnancy_conf['seq_filter']
            if seq_filter == '':
                # 선택된 작업이 없으면 카운트 0
                self.logger.info("임신감정 작업 없음 (seq_filter=''), 카운트 생략")
                return result

            sql = """
            SELECT TO_DATE(PASS_DT, 'YYYY-MM-DD') AS SCH_DT
            FROM TABLE(FN_MD_SCHEDULE_BSE_2020(
                :farm_no, 'JOB-DAJANG', '150001', NULL,
                :v_sdt, :v_edt, NULL, 'ko', 'yyyy-MM-dd', :seq_filter, NULL
            ))
            """
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql, {
                    'farm_no': self.farm_no,
                    'v_sdt': v_sdt,
                    'v_edt': v_edt,
                    'seq_filter': seq_filter,
                })

                for row in cursor.fetchall():
                    sch_dt = row[0]
                    if sch_dt:
                        for i, dt in enumerate(dates):
                            if sch_dt.date() == dt.date():
                                # 모돈작업설정은 3w/4w 구분 없이 합산 (3w에 집계)
                                result['3w']['sum'] += 1
                                result['3w']['daily'][i] += 1
                                break
            finally:
                cursor.close()
        else:
            # 농장기본값: 교배일 + 21일/28일 고정
            sql = """
            SELECT /*+ INDEX(WK IX_TB_MODON_WK_01) */
                   TO_DATE(WK.WK_DT, 'YYYYMMDD') AS GB_DT,
                   WK.PIG_NO
            FROM VM_LAST_MODON_SEQ_WK WK
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

                    # 3주령: 교배일 + 21일 (정확히 21일째)
                    check_3w = gb_dt + timedelta(days=21)
                    for i, dt in enumerate(dates):
                        if check_3w.date() == dt.date():
                            result['3w']['daily'][i] += 1
                            break

                    # 4주령: 교배일 + 28일 (정확히 28일째)
                    check_4w = gb_dt + timedelta(days=28)
                    for i, dt in enumerate(dates):
                        if check_4w.date() == dt.date():
                            result['4w']['daily'][i] += 1
                            break
            finally:
                cursor.close()

            # 합계 계산 (농장기본값만)
            for i in range(7):
                result['3w']['sum'] += result['3w']['daily'][i]
                result['4w']['sum'] += result['4w']['daily'][i]

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
            'gb_sum': stats.get('gb_sum', 0),
            'imsin_sum': stats.get('imsin_sum', 0),
            'bm_sum': stats.get('bm_sum', 0),
            'eu_sum': stats.get('eu_sum', 0),
            'vaccine_sum': stats.get('vaccine_sum', 0),
            'ship_sum': stats.get('ship_sum', 0),
        })

        return stats

    def _insert_calendar(self, schedule_counts: Dict, imsin_counts: Dict,
                         dates: List[datetime], ins_conf: Dict[str, Dict[str, Any]]) -> None:
        """캘린더 그리드 INSERT (SUB_GUBUN='CAL')

        임신감정 산정방식에 따라 캘린더 데이터 구성:
        - 농장기본값: IMSIN_3W, IMSIN_4W 별도 표시
        - 모돈작업설정: IMSIN (3w+4w 합산, 실제로는 3w에만 데이터)
        """
        cal_data = [
            (1, 'GB', schedule_counts['gb']['daily']),
            (2, 'BM', schedule_counts['bm']['daily']),
        ]

        # 임신감정: 산정방식에 따라 다르게 처리
        if ins_conf['pregnancy']['method'] == 'modon':
            # 모돈작업설정: IMSIN 하나로 표시 (3w에 집계된 데이터 사용)
            cal_data.append((3, 'IMSIN', imsin_counts['3w']['daily']))
        else:
            # 농장기본값: IMSIN_3W, IMSIN_4W 별도 표시
            cal_data.append((3, 'IMSIN_3W', imsin_counts['3w']['daily']))
            cal_data.append((4, 'IMSIN_4W', imsin_counts['4w']['daily']))

        # EU, VACCINE 추가 (SORT_NO는 임신감정 방식에 따라 조정)
        next_sort = 4 if ins_conf['pregnancy']['method'] == 'modon' else 5
        cal_data.append((next_sort, 'EU', schedule_counts['eu']['daily']))
        cal_data.append((next_sort + 1, 'VACCINE', schedule_counts['vaccine']['daily']))

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

    def _insert_popup_details(self, v_sdt: str, v_edt: str, dt_from: datetime,
                               ins_conf: Dict[str, Dict[str, Any]]) -> None:
        """팝업 상세 INSERT (SUB_GUBUN='GB/BM/EU/VACCINE')

        TB_PLAN_MODON 기준으로 작업명별 그룹화
        ins_conf 설정에 따라 선택된 SEQ만 필터링
        """
        # GB, BM, EU는 공통 메소드 사용
        # (sub_gubun, job_gubun_cd, ins_conf_key)
        popup_configs = [
            ('GB', '150005', 'mating'),
            ('BM', '150002', 'farrowing'),
            ('EU', '150003', 'weaning'),
        ]

        for sub_gubun, job_gubun_cd, conf_key in popup_configs:
            conf = ins_conf[conf_key]
            # method='farm'이면 팝업 상세 INSERT 생략 (농장기본값은 TB_PLAN_MODON 기반이 아님)
            if conf['method'] == 'farm':
                self.logger.info(f"팝업 상세 생략 (농장기본값): {sub_gubun}")
                continue
            seq_filter = conf['seq_filter']
            # seq_filter=''이면 선택된 작업이 없으므로 팝업 상세 생략
            if seq_filter == '':
                self.logger.info(f"팝업 상세 생략 (선택 작업 없음): {sub_gubun}")
                continue
            self._insert_popup_by_job(sub_gubun, job_gubun_cd, v_sdt, v_edt, dt_from, seq_filter)

        # 임신감정(IMSIN)은 모돈작업설정일 때만 팝업 상세 INSERT
        pregnancy_conf = ins_conf['pregnancy']
        if pregnancy_conf['method'] == 'modon':
            pregnancy_seq_filter = pregnancy_conf['seq_filter']
            if pregnancy_seq_filter == '':
                self.logger.info("팝업 상세 생략 (선택 작업 없음): IMSIN")
            else:
                self._insert_popup_by_job('IMSIN', '150001', v_sdt, v_edt, dt_from, pregnancy_seq_filter)
        else:
            self.logger.info("팝업 상세 생략 (농장기본값): IMSIN")

        # VACCINE은 ARTICLE_NM(백신명) 포함하므로 별도 처리
        vaccine_seq_filter = ins_conf['vaccine']['seq_filter']
        # seq_filter=''이면 선택된 작업이 없으므로 팝업 상세 생략
        if vaccine_seq_filter == '':
            self.logger.info("팝업 상세 생략 (선택 작업 없음): VACCINE")
        else:
            self._insert_vaccine_popup(v_sdt, v_edt, dt_from, vaccine_seq_filter)

    def _insert_popup_by_job(self, sub_gubun: str, job_gubun_cd: str,
                              v_sdt: str, v_edt: str, dt_from: datetime,
                              seq_filter: str = '-1') -> None:
        """작업유형별 팝업 상세 INSERT (모돈 작업설정 기준)

        Args:
            sub_gubun: SUB_GUBUN 값 ('GB', 'BM', 'EU')
            job_gubun_cd: 작업구분코드 ('150005', '150002', '150003')
            v_sdt: 시작일 (yyyy-MM-dd)
            v_edt: 종료일 (yyyy-MM-dd)
            dt_from: 시작일 (datetime)
            seq_filter: TB_PLAN_MODON.SEQ 필터 ('-1'=전체, '1,2,3'=선택)
        """
        # seq_filter가 특정 SEQ인 경우 해당 SEQ만 조회
        seq_condition = ""
        if seq_filter != '-1':
            seq_list = seq_filter.split(',')
            seq_condition = f"AND P.SEQ IN ({','.join(seq_list)})"

        sql = f"""
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
                    :v_sdt, :v_edt, NULL, 'ko', 'yyyy-MM-dd', :seq_filter, NULL
                ))
                GROUP BY WK_NM
            ) S ON P.WK_NM = S.WK_NM
            WHERE P.FARM_NO = :farm_no
              AND P.JOB_GUBUN_CD = :job_gubun_cd
              AND P.USE_YN = 'Y'
              {seq_condition}
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
            'seq_filter': seq_filter,
        })

    def _insert_vaccine_popup(self, v_sdt: str, v_edt: str, dt_from: datetime,
                               seq_filter: str = '-1') -> None:
        """백신예정 팝업 상세 INSERT (SUB_GUBUN='VACCINE')

        ARTICLE_NM(백신명) 포함하여 INSERT

        Args:
            v_sdt: 시작일 (yyyy-MM-dd)
            v_edt: 종료일 (yyyy-MM-dd)
            dt_from: 시작일 (datetime)
            seq_filter: TB_PLAN_MODON.SEQ 필터 ('-1'=전체, '1,2,3'=선택)
        """
        # seq_filter가 특정 SEQ인 경우 해당 SEQ만 조회
        seq_condition = ""
        if seq_filter != '-1':
            seq_list = seq_filter.split(',')
            seq_condition = f"AND P.SEQ IN ({','.join(seq_list)})"

        sql = f"""
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
                    :v_sdt, :v_edt, NULL, 'ko', 'yyyy-MM-dd', :seq_filter, NULL
                ))
                GROUP BY WK_NM, ARTICLE_NM
            ) S ON P.WK_NM = S.WK_NM
            WHERE P.FARM_NO = :farm_no
              AND P.JOB_GUBUN_CD = '150004'
              AND P.USE_YN = 'Y'
              {seq_condition}
            ORDER BY P.WK_NM
        )
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'v_sdt': v_sdt,
            'v_edt': v_edt,
            'dt_from': dt_from,
            'seq_filter': seq_filter,
        })

    def _insert_help_info(self, config: Dict[str, Any], dt_from: datetime,
                          ins_conf: Dict[str, Dict[str, Any]]) -> None:
        """HELP 정보 INSERT (SUB_GUBUN='HELP')

        출하예정 계산 안내 + 산정방식 정보 포함:
        - 출하예정두수 = N일전 이유두수 × 육성율
        - N = 기준출하일령 - 평균포유기간 (예: 180 - 25 = 155일)

        Args:
            config: 농장 설정값
            dt_from: 시작일 (datetime)
            ins_conf: TS_INS_CONF 설정 (산정방식 표시용)
        """
        # 농장기본값 표시용 추가 설정 조회 (avg_return_day, first_mating_age)
        farm_config = self._get_farm_config_for_schedule()
        config = {**config, **farm_config}  # 기존 config에 farm_config 병합

        ship_offset = config['ship_day'] - config['wean_period']

        # 이유 기간 계산 (금주 출하예정 대상이 되는 이유일 범위)
        wean_from = dt_from - timedelta(days=ship_offset)
        wean_to = dt_from + timedelta(days=6) - timedelta(days=ship_offset)
        wean_period_str = f"{wean_from.strftime('%Y-%m-%d')} ~ {wean_to.strftime('%Y-%m-%d')}"

        # 산정방식 텍스트 생성
        def get_method_text(conf_key: str, job_name: str) -> str:
            method = ins_conf[conf_key]['method']
            if method == 'farm':
                return f'{job_name}: 농장기본값'
            else:
                seq_filter = ins_conf[conf_key]['seq_filter']
                if seq_filter == '':
                    return f'{job_name}: 모돈작업(없음)'
                else:
                    task_count = len(seq_filter.split(','))
                    return f'{job_name}: 모돈작업({task_count}개)'

        method_info = (
            f"* 산정방식: {get_method_text('mating', '교배')}, "
            f"{get_method_text('farrowing', '분만')}, "
            f"{get_method_text('pregnancy', '임신감정')}, "
            f"{get_method_text('weaning', '이유')}, "
            f"{get_method_text('vaccine', '백신')}"
        )

        # seq_filter 조건 생성 (선택된 SEQ만 조회)
        # 농장기본값일 때 실제 설정값 포함하여 저장 (스냅샷)
        # 형식: settings 페이지(WeeklyScheduleSettings.tsx)와 동일하게
        def get_seq_condition(conf_key: str, job_gubun_cd: str) -> str:
            if ins_conf[conf_key]['method'] == 'farm':
                # 농장기본값: 작업별 실제 설정값 포함 (settings 페이지 형식)
                # 각 항목 별도 줄로 표시 (· 기호 사용)
                if conf_key == 'mating':
                    # 교배: 이유돈, 후보돈, 사고/재발돈 각각 한 줄씩
                    return f"""'(농장기본값)' || CHR(10) ||
                           '· 이유돈(평균재귀일) {config['avg_return_day']}일' || CHR(10) ||
                           '· 후보돈(초교배일령) {config['first_mating_age']}일' || CHR(10) ||
                           '· 사고/재발돈 즉시'"""
                elif conf_key == 'farrowing':
                    # 분만: 임신모돈(평균임신기간) 115일
                    return f"""'(농장기본값)' || CHR(10) ||
                           '· 임신모돈(평균임신기간) {config['preg_period']}일'"""
                elif conf_key == 'weaning':
                    # 이유: 포유모돈(평균포유기간) 21일
                    return f"""'(농장기본값)' || CHR(10) ||
                           '· 포유모돈(평균포유기간) {config['wean_period']}일'"""
                elif conf_key == 'vaccine':
                    return "'(농장기본값) 백신설정 기준'"
                else:
                    return "'농장기본값'"
            seq_filter = ins_conf[conf_key]['seq_filter']
            if seq_filter == '':
                return "'(선택된 작업 없음)'"
            else:
                return f"""(SELECT LISTAGG(WK_NM || '(' || PASS_DAY || '일)', ',') WITHIN GROUP (ORDER BY WK_NM)
                           FROM TB_PLAN_MODON WHERE FARM_NO = :farm_no AND JOB_GUBUN_CD = '{job_gubun_cd}' AND USE_YN = 'Y'
                           AND SEQ IN ({seq_filter}))"""

        # 임신감정(pregnancy) 조건 생성 - JOB_GUBUN_CD='150001' 사용
        # 재발확인(STR_6)과 임신진단(STR_7)을 분리하여 저장
        def get_pregnancy_3w_condition() -> str:
            """재발확인(3주) 조건"""
            if ins_conf['pregnancy']['method'] == 'farm':
                return "'(농장기본값) 교배 후 3주'"
            seq_filter = ins_conf['pregnancy']['seq_filter']
            if seq_filter == '':
                return "'(모돈작업설정) 선택된 작업 없음'"
            else:
                return f"""(SELECT LISTAGG(WK_NM || '(' || PASS_DAY || '일)', ',') WITHIN GROUP (ORDER BY WK_NM)
                           FROM TB_PLAN_MODON WHERE FARM_NO = :farm_no AND JOB_GUBUN_CD = '150001' AND USE_YN = 'Y'
                           AND SEQ IN ({seq_filter}))"""

        def get_pregnancy_4w_condition() -> str:
            """임신진단(4주) 조건"""
            if ins_conf['pregnancy']['method'] == 'farm':
                return "'(농장기본값) 교배 후 4주'"
            seq_filter = ins_conf['pregnancy']['seq_filter']
            if seq_filter == '':
                return "'(모돈작업설정) 선택된 작업 없음'"
            else:
                # 모돈작업설정일 때는 재발확인과 동일 (TB_PLAN_MODON에서 선택된 작업)
                return f"""(SELECT LISTAGG(WK_NM || '(' || PASS_DAY || '일)', ',') WITHIN GROUP (ORDER BY WK_NM)
                           FROM TB_PLAN_MODON WHERE FARM_NO = :farm_no AND JOB_GUBUN_CD = '150001' AND USE_YN = 'Y'
                           AND SEQ IN ({seq_filter}))"""

        sql = f"""
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            STR_1, STR_2, STR_3, STR_4, STR_5, STR_6, STR_7
        )
        SELECT :master_seq, :farm_no, 'SCHEDULE', 'HELP', 1,
               {get_seq_condition('mating', '150005')},
               {get_seq_condition('farrowing', '150002')},
               {get_seq_condition('weaning', '150003')},
               {get_seq_condition('vaccine', '150004')},
               '* 육성율: ' || :rearing_rate || '% (' || :rate_from || '~' || :rate_to || ' 평균, 기본 90%)' || CHR(10) ||
               '* 공식: ' || :ship_offset || '일전 이유두수 × 육성율' || CHR(10) ||
               '  - 기준출하일령(' || :ship_day || '일) - 평균포유기간(' || :wean_period || '일) = ' || :ship_offset || '일' || CHR(10) ||
               '* 이유기간: ' || :wean_period_str || CHR(10) ||
               :method_info,
               {get_pregnancy_3w_condition()},
               {get_pregnancy_4w_condition()}
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
            'wean_period_str': wean_period_str,
            'method_info': method_info,
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
            'gb_sum': stats.get('gb_sum', 0),
            'imsin_sum': stats.get('imsin_sum', 0),
            'bm_sum': stats.get('bm_sum', 0),
            'eu_sum': stats.get('eu_sum', 0),
            'vaccine_sum': stats.get('vaccine_sum', 0),
            'ship_sum': stats.get('ship_sum', 0),
        })

    def _insert_method_info(self, ins_conf: Dict[str, Dict[str, Any]]) -> None:
        """산정방식 정보 INSERT (SUB_GUBUN='METHOD')

        각 예정별 산정방식(농장기본값/모돈작업설정)을 저장하여
        웹에서 분기 처리에 활용할 수 있도록 함.

        저장 컬럼:
        - STR_1: 교배예정 산정방식 (farm/modon)
        - STR_2: 분만예정 산정방식 (farm/modon)
        - STR_3: 임신감정 산정방식 (farm/modon)
        - STR_4: 이유예정 산정방식 (farm/modon)
        - STR_5: 백신예정 산정방식 (farm/modon)

        Args:
            ins_conf: TS_INS_CONF 설정 (method, tasks, seq_filter)
        """
        sql = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO,
            STR_1, STR_2, STR_3, STR_4, STR_5
        ) VALUES (
            :master_seq, :farm_no, 'SCHEDULE', 'METHOD', 1,
            :mating_method, :farrowing_method, :pregnancy_method, :weaning_method, :vaccine_method
        )
        """
        self.execute(sql, {
            'master_seq': self.master_seq,
            'farm_no': self.farm_no,
            'mating_method': ins_conf['mating']['method'],
            'farrowing_method': ins_conf['farrowing']['method'],
            'pregnancy_method': ins_conf['pregnancy']['method'],
            'weaning_method': ins_conf['weaning']['method'],
            'vaccine_method': ins_conf['vaccine']['method'],
        })
