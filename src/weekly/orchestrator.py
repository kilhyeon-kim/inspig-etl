"""
주간 리포트 오케스트레이터
- ETL 작업 흐름 제어
- 1. 생산성 데이터 수집 (선행)
- 2. 기상청 데이터 수집 (선행)
- 3. 주간 리포트 생성

v2 아키텍처:
- 농장별 병렬 처리 (ThreadPoolExecutor)
- 프로세서별 병렬 처리 (AsyncFarmProcessor)
"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Optional

from ..common import Config, Database, setup_logger
from ..collectors import WeatherCollector, ProductivityCollector

logger = logging.getLogger(__name__)

# 테스트 초기화용 설정
TEST_TABLES = [
    'TS_INS_WEEK_SUB',
    'TS_INS_WEEK',
    'TS_INS_MASTER',
    'TS_INS_JOB_LOG',
]

TEST_DATES = [
    '20251110',
    '20251117',
    '20251124',
    '20251201',
    '20251208',
    '20251215',
    '20251222',
]

TEST_FARM_LIST = '1387,2807,4448,1456,1517'


class WeeklyReportOrchestrator:
    """주간 리포트 ETL 오케스트레이터

    ETL 작업 순서:
    1. 생산성 데이터 수집 (외부 API → DB)
    2. 기상청 데이터 수집 (외부 API → DB)
    3. 주간 리포트 생성 (DB 집계)
    """

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.db = Database(self.config)
        self.logger = setup_logger("weekly_orchestrator", self.config.logging.get('log_path'))

    def run(
        self,
        base_date: Optional[str] = None,
        test_mode: bool = False,
        skip_productivity: bool = True,  # 현재는 스킵
        skip_weather: bool = False,
        dry_run: bool = False,
    ) -> dict:
        """ETL 파이프라인 실행

        Args:
            base_date: 기준 날짜 (YYYYMMDD), None이면 오늘
            test_mode: 테스트 모드 (금주 데이터)
            skip_productivity: 생산성 수집 스킵
            skip_weather: 기상청 수집 스킵
            dry_run: 실제 실행 없이 설정만 확인

        Returns:
            실행 결과 딕셔너리
        """
        self.logger.info("=" * 60)
        self.logger.info("InsightPig Weekly ETL 시작")
        self.logger.info("=" * 60)

        # 기준 날짜 설정
        if base_date:
            base_dt = datetime.strptime(base_date, '%Y%m%d')
        else:
            base_dt = datetime.now()

        # 주차 계산 (ISO Week)
        year = int(base_dt.strftime('%G'))  # ISO year
        week_no = int(base_dt.strftime('%V'))  # ISO week

        # 기간 계산
        if test_mode:
            # 테스트 모드: 금주 (월요일 ~ 오늘)
            dt_from = (base_dt - timedelta(days=base_dt.weekday())).strftime('%Y%m%d')
            dt_to = base_dt.strftime('%Y%m%d')
        else:
            # 운영 모드: 지난주
            last_sunday = base_dt - timedelta(days=base_dt.weekday() + 1)
            last_monday = last_sunday - timedelta(days=6)
            dt_from = last_monday.strftime('%Y%m%d')
            dt_to = last_sunday.strftime('%Y%m%d')

        self.logger.info(f"  기준일: {base_dt.strftime('%Y-%m-%d')}")
        self.logger.info(f"  리포트 기간: {dt_from} ~ {dt_to}")
        self.logger.info(f"  주차: {year}년 {week_no}주")
        self.logger.info(f"  테스트 모드: {test_mode}")
        self.logger.info(f"  생산성 수집: {'스킵' if skip_productivity else '실행'}")
        self.logger.info(f"  기상청 수집: {'스킵' if skip_weather else '실행'}")

        if dry_run:
            self.logger.info("DRY-RUN 모드: 실제 작업을 수행하지 않습니다.")
            return {
                'status': 'dry_run',
                'year': year,
                'week_no': week_no,
                'dt_from': dt_from,
                'dt_to': dt_to,
            }

        result = {
            'status': 'success',
            'year': year,
            'week_no': week_no,
            'dt_from': dt_from,
            'dt_to': dt_to,
            'steps': {},
        }

        try:
            # Step 1: 생산성 데이터 수집 (현재 스킵)
            if not skip_productivity:
                self.logger.info("-" * 40)
                self.logger.info("Step 1: 생산성 데이터 수집")
                productivity_count = self._collect_productivity(dt_to)
                result['steps']['productivity'] = productivity_count
            else:
                self.logger.info("Step 1: 생산성 데이터 수집 (스킵)")
                result['steps']['productivity'] = 'skipped'

            # Step 2: 기상청 데이터 수집
            if not skip_weather:
                self.logger.info("-" * 40)
                self.logger.info("Step 2: 기상청 데이터 수집")
                weather_count = self._collect_weather()
                result['steps']['weather'] = weather_count
            else:
                self.logger.info("Step 2: 기상청 데이터 수집 (스킵)")
                result['steps']['weather'] = 'skipped'

            # Step 3: 주간 리포트 생성
            self.logger.info("-" * 40)
            self.logger.info("Step 3: 주간 리포트 생성")
            report_result = self._generate_weekly_report(
                year, week_no, dt_from, dt_to, test_mode
            )
            result['steps']['weekly_report'] = report_result

            self.logger.info("=" * 60)
            self.logger.info("InsightPig Weekly ETL 완료")
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"ETL 실패: {e}", exc_info=True)
            result['status'] = 'error'
            result['error'] = str(e)
            raise

        return result

    def _collect_productivity(self, stat_date: str) -> int:
        """생산성 데이터 수집"""
        collector = ProductivityCollector(self.config, self.db)
        return collector.run(stat_date=stat_date)

    def _collect_weather(self) -> int:
        """기상청 데이터 수집"""
        collector = WeatherCollector(self.config, self.db)
        return collector.run()

    def _generate_weekly_report(
        self,
        year: int,
        week_no: int,
        dt_from: str,
        dt_to: str,
        test_mode: bool,
        use_python: bool = True,
        use_async: bool = True,
        farm_list: Optional[str] = None,
    ) -> dict:
        """주간 리포트 생성

        Python 프로세서 또는 Oracle 프로시저 사용

        Args:
            year: 연도
            week_no: 주차
            dt_from: 시작일
            dt_to: 종료일
            test_mode: 테스트 모드
            use_python: Python 프로세서 사용 여부 (False면 Oracle 프로시저 호출)
            use_async: 비동기 병렬 처리 사용 여부
            farm_list: 처리할 농장 목록 (콤마 구분, None이면 전체)

        Returns:
            처리 결과 딕셔너리
        """
        if use_python:
            if use_async:
                return self._generate_weekly_report_async(year, week_no, dt_from, dt_to, test_mode, farm_list)
            else:
                return self._generate_weekly_report_python(year, week_no, dt_from, dt_to, test_mode, farm_list)
        else:
            return self._generate_weekly_report_procedure(test_mode)

    def _generate_weekly_report_procedure(self, test_mode: bool) -> dict:
        """Oracle 프로시저를 사용한 주간 리포트 생성 (레거시)"""
        test_yn = 'Y' if test_mode else 'N'
        parallel = self.config.processing.get('parallel', 4)

        self.logger.info(f"SP_INS_WEEK_MAIN 호출: WEEK, parallel={parallel}, test={test_yn}")

        try:
            self.db.call_procedure(
                'SP_INS_WEEK_MAIN',
                ['WEEK', None, parallel, test_yn]
            )

            return {
                'status': 'complete',
                'method': 'procedure',
                'proc_name': 'SP_INS_WEEK_MAIN',
            }

        except Exception as e:
            self.logger.error(f"주간 리포트 생성 실패: {e}")
            return {
                'status': 'error',
                'error': str(e),
            }

    def _generate_weekly_report_python(
        self,
        year: int,
        week_no: int,
        dt_from: str,
        dt_to: str,
        test_mode: bool,
        farm_list: Optional[str] = None,
    ) -> dict:
        """Python 프로세서를 사용한 주간 리포트 생성

        SP_INS_WEEK_MAIN 프로시저의 Python 버전
        """
        from .farm_processor import FarmProcessor

        self.logger.info(f"Python ETL 실행: {year}년 {week_no}주, 기간={dt_from}~{dt_to}")

        target_cnt = 0
        complete_cnt = 0
        error_cnt = 0
        farm_results = []

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # 1. 전국 탕박 평균 단가 계산
                national_price = self._get_national_price(cursor, dt_from, dt_to)
                self.logger.info(f"전국 탕박 평균 단가: {national_price}원")

                # 2. 마스터 레코드 생성
                master_seq = self._create_master(cursor, year, week_no, dt_from, dt_to)
                self.logger.info(f"마스터 SEQ: {master_seq}")

                # 3. 대상 농장 조회
                farms = self._get_target_farms(cursor, farm_list, test_mode)
                target_cnt = len(farms)
                self.logger.info(f"대상 농장: {target_cnt}개")

                if target_cnt == 0:
                    self.logger.warning("대상 농장이 없습니다.")
                    return {'status': 'complete', 'method': 'python', 'target_cnt': 0}

                # 4. 농장별 초기 레코드 생성 (TS_INS_WEEK)
                self._create_week_records(cursor, master_seq, farms, year, week_no, dt_from, dt_to)
                conn.commit()

                # 5. 농장별 처리
                for i, farm in enumerate(farms, 1):
                    farm_no = farm['FARM_NO']
                    locale = farm.get('LOCALE', 'KOR')

                    self.logger.info(f"[{i}/{target_cnt}] 농장 {farm_no} 처리 중...")

                    processor = FarmProcessor(conn, master_seq, farm_no, locale)
                    result = processor.process(dt_from, dt_to, national_price=national_price)

                    if result['status'] == 'success':
                        complete_cnt += 1
                    else:
                        error_cnt += 1

                    farm_results.append(result)

                # 6. 마스터 상태 업데이트
                self._update_master(cursor, master_seq, target_cnt, complete_cnt, error_cnt)
                conn.commit()

            except Exception as e:
                self.logger.error(f"주간 리포트 생성 실패: {e}", exc_info=True)
                raise
            finally:
                cursor.close()

        self.logger.info(f"Python ETL 완료: 대상={target_cnt}, 완료={complete_cnt}, 오류={error_cnt}")

        return {
            'status': 'complete' if error_cnt == 0 else 'error',
            'method': 'python',
            'master_seq': master_seq,
            'target_cnt': target_cnt,
            'complete_cnt': complete_cnt,
            'error_cnt': error_cnt,
            'farm_results': farm_results,
        }

    def _generate_weekly_report_async(
        self,
        year: int,
        week_no: int,
        dt_from: str,
        dt_to: str,
        test_mode: bool,
        farm_list: Optional[str] = None,
    ) -> dict:
        """비동기 병렬 처리를 사용한 주간 리포트 생성

        농장별 병렬 처리 + 프로세서별 병렬 처리

        Args:
            year: 연도
            week_no: 주차
            dt_from: 시작일
            dt_to: 종료일
            test_mode: 테스트 모드
            farm_list: 처리할 농장 목록 (콤마 구분, None이면 전체)

        Returns:
            처리 결과 딕셔너리
        """
        from .async_processor import AsyncFarmProcessor

        self.logger.info(f"Python ETL (비동기) 실행: {year}년 {week_no}주, 기간={dt_from}~{dt_to}")

        # 설정에서 병렬 처리 설정 가져오기
        max_farm_workers = self.config.processing.get('max_farm_workers', 4)
        max_processor_workers = self.config.processing.get('max_processor_workers', 5)

        self.logger.info(f"  농장 병렬 처리: {max_farm_workers}개")
        self.logger.info(f"  프로세서 병렬 처리: {max_processor_workers}개")

        target_cnt = 0
        complete_cnt = 0
        error_cnt = 0
        farm_results = []

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # 1. 전국 탕박 평균 단가 계산
                national_price = self._get_national_price(cursor, dt_from, dt_to)
                self.logger.info(f"전국 탕박 평균 단가: {national_price}원")

                # 2. 마스터 레코드 생성
                master_seq = self._create_master(cursor, year, week_no, dt_from, dt_to)
                self.logger.info(f"마스터 SEQ: {master_seq}")

                # 3. 대상 농장 조회
                farms = self._get_target_farms(cursor, farm_list, test_mode)
                target_cnt = len(farms)
                self.logger.info(f"대상 농장: {target_cnt}개")

                if target_cnt == 0:
                    self.logger.warning("대상 농장이 없습니다.")
                    return {'status': 'complete', 'method': 'python_async', 'target_cnt': 0}

                # 4. 농장별 초기 레코드 생성 (TS_INS_WEEK)
                self._create_week_records(cursor, master_seq, farms, year, week_no, dt_from, dt_to)
                conn.commit()

            finally:
                cursor.close()

        # 5. 농장별 병렬 처리 (각 농장은 별도 DB 연결 사용)
        def process_single_farm(farm: dict) -> dict:
            """단일 농장 처리 (별도 스레드에서 실행)"""
            farm_no = farm['FARM_NO']
            locale = farm.get('LOCALE', 'KOR')

            try:
                with self.db.get_connection() as farm_conn:
                    processor = AsyncFarmProcessor(
                        farm_conn,
                        master_seq,
                        farm_no,
                        locale,
                        max_workers=max_processor_workers,
                    )
                    result = processor.process(dt_from, dt_to, national_price=national_price)
                    farm_conn.commit()
                    return result
            except Exception as e:
                self.logger.error(f"농장 {farm_no} 처리 오류: {e}", exc_info=True)
                return {
                    'farm_no': farm_no,
                    'status': 'error',
                    'error': str(e),
                }

        # ThreadPoolExecutor로 농장별 병렬 처리
        self.logger.info(f"농장별 병렬 처리 시작 (workers={max_farm_workers})")

        with ThreadPoolExecutor(max_workers=max_farm_workers) as executor:
            # 모든 농장에 대해 비동기 작업 제출
            future_to_farm = {
                executor.submit(process_single_farm, farm): farm
                for farm in farms
            }

            # 완료된 작업 수집
            for future in as_completed(future_to_farm):
                farm = future_to_farm[future]
                farm_no = farm['FARM_NO']

                try:
                    result = future.result()
                    farm_results.append(result)

                    if result.get('status') == 'success':
                        complete_cnt += 1
                        self.logger.info(f"농장 {farm_no} 완료")
                    else:
                        error_cnt += 1
                        self.logger.warning(f"농장 {farm_no} 오류: {result.get('error', 'unknown')}")

                except Exception as e:
                    error_cnt += 1
                    self.logger.error(f"농장 {farm_no} 처리 예외: {e}")
                    farm_results.append({
                        'farm_no': farm_no,
                        'status': 'error',
                        'error': str(e),
                    })

        # 6. 마스터 상태 업데이트
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                self._update_master(cursor, master_seq, target_cnt, complete_cnt, error_cnt)
                conn.commit()
            finally:
                cursor.close()

        self.logger.info(f"Python ETL (비동기) 완료: 대상={target_cnt}, 완료={complete_cnt}, 오류={error_cnt}")

        return {
            'status': 'complete' if error_cnt == 0 else 'error',
            'method': 'python_async',
            'master_seq': master_seq,
            'target_cnt': target_cnt,
            'complete_cnt': complete_cnt,
            'error_cnt': error_cnt,
            'farm_results': farm_results,
        }

    def _get_national_price(self, cursor, dt_from: str, dt_to: str) -> int:
        """전국 탕박 평균 단가 계산"""
        sql = """
        SELECT NVL(ROUND(SUM(AUCTCNT * AUCTAMT) / NULLIF(SUM(AUCTCNT), 0)), 0)
        FROM TM_SISAE_DETAIL
        WHERE ABATTCD = '057016'
          AND START_DT BETWEEN :dt_from AND :dt_to
          AND GRADE_CD = 'ST'
          AND SKIN_YN = 'Y'
          AND JUDGESEX_CD IS NULL
          AND TO_NUMBER(NVL(AUCTAMT, '0')) > 0
        """
        cursor.execute(sql, {'dt_from': dt_from, 'dt_to': dt_to})
        result = cursor.fetchone()
        return result[0] if result and result[0] else 0

    def _create_master(self, cursor, year: int, week_no: int, dt_from: str, dt_to: str) -> int:
        """TS_INS_MASTER 레코드 생성"""
        cursor.execute("SELECT SEQ_TS_INS_MASTER.NEXTVAL FROM DUAL")
        master_seq = cursor.fetchone()[0]

        sql = """
        INSERT INTO TS_INS_MASTER (
            SEQ, DAY_GB, INS_DT, REPORT_YEAR, REPORT_WEEK_NO,
            DT_FROM, DT_TO, STATUS_CD, START_DT
        ) VALUES (
            :seq, 'WEEK', :ins_dt, :year, :week_no,
            :dt_from, :dt_to, 'RUNNING', SYSDATE
        )
        """
        cursor.execute(sql, {
            'seq': master_seq,
            'ins_dt': dt_to,
            'year': year,
            'week_no': week_no,
            'dt_from': dt_from,
            'dt_to': dt_to,
        })

        return master_seq

    def _get_target_farms(self, cursor, farm_list: Optional[str], test_mode: bool) -> List[dict]:
        """대상 농장 조회"""
        sql = """
        SELECT DISTINCT F.FARM_NO, F.FARM_NM, F.PRINCIPAL_NM, F.SIGUN_CD,
               NVL(F.COUNTRY_CODE, 'KOR') AS LOCALE
        FROM TA_FARM F
        INNER JOIN TS_INS_SERVICE S ON F.FARM_NO = S.FARM_NO
        WHERE F.USE_YN = 'Y'
          AND S.INSPIG_YN = 'Y'
          AND S.USE_YN = 'Y'
          AND (S.INSPIG_TO_DT IS NULL OR S.INSPIG_TO_DT >= TO_CHAR(SYSDATE, 'YYYYMMDD'))
          AND S.INSPIG_STOP_DT IS NULL
        """

        if farm_list:
            # 농장 목록이 지정된 경우 필터링
            farm_nos = [int(f.strip()) for f in farm_list.split(',') if f.strip()]
            placeholders = ', '.join([f':f{i}' for i in range(len(farm_nos))])
            sql += f" AND F.FARM_NO IN ({placeholders})"

            params = {f'f{i}': f for i, f in enumerate(farm_nos)}
            cursor.execute(sql + " ORDER BY F.FARM_NO", params)
        else:
            cursor.execute(sql + " ORDER BY F.FARM_NO")

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _create_week_records(
        self,
        cursor,
        master_seq: int,
        farms: List[dict],
        year: int,
        week_no: int,
        dt_from: str,
        dt_to: str,
    ) -> None:
        """TS_INS_WEEK 초기 레코드 생성"""
        sql = """
        INSERT INTO TS_INS_WEEK (
            MASTER_SEQ, FARM_NO, REPORT_YEAR, REPORT_WEEK_NO,
            DT_FROM, DT_TO, FARM_NM, OWNER_NM, SIGUNGU_CD, STATUS_CD
        ) VALUES (
            :master_seq, :farm_no, :year, :week_no,
            :dt_from, :dt_to, :farm_nm, :owner_nm, :sigun_cd, 'READY'
        )
        """

        for farm in farms:
            cursor.execute(sql, {
                'master_seq': master_seq,
                'farm_no': farm['FARM_NO'],
                'year': year,
                'week_no': week_no,
                'dt_from': dt_from,
                'dt_to': dt_to,
                'farm_nm': farm.get('FARM_NM', ''),
                'owner_nm': farm.get('PRINCIPAL_NM', ''),
                'sigun_cd': farm.get('SIGUN_CD', ''),
            })

        # 마스터 대상 농장수 업데이트
        cursor.execute("""
            UPDATE TS_INS_MASTER SET TARGET_CNT = :cnt WHERE SEQ = :seq
        """, {'cnt': len(farms), 'seq': master_seq})

    def _update_master(self, cursor, master_seq: int, target_cnt: int, complete_cnt: int, error_cnt: int) -> None:
        """TS_INS_MASTER 상태 업데이트"""
        sql = """
        UPDATE TS_INS_MASTER
        SET STATUS_CD = CASE WHEN :error_cnt = 0 THEN 'COMPLETE' ELSE 'ERROR' END,
            TARGET_CNT = :target_cnt,
            COMPLETE_CNT = :complete_cnt,
            ERROR_CNT = :error_cnt,
            END_DT = SYSDATE,
            ELAPSED_SEC = ROUND((SYSDATE - START_DT) * 24 * 60 * 60)
        WHERE SEQ = :seq
        """
        cursor.execute(sql, {
            'seq': master_seq,
            'target_cnt': target_cnt,
            'complete_cnt': complete_cnt,
            'error_cnt': error_cnt,
        })

    def initialize_test_data(self) -> dict:
        """테스트용 테이블 초기화

        주의: 테스트 모드에서만 사용!
        TS_INS_WEEK_SUB, TS_INS_WEEK, TS_INS_MASTER, TS_INS_JOB_LOG 테이블 데이터 삭제

        Returns:
            삭제된 레코드 수 딕셔너리
        """
        self.logger.warning("=" * 60)
        self.logger.warning("테스트 데이터 초기화 시작")
        self.logger.warning("=" * 60)

        result = {}

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                for table in TEST_TABLES:
                    # 삭제 전 레코드 수 확인
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count_before = cursor.fetchone()[0]

                    # 데이터 삭제
                    cursor.execute(f"DELETE FROM {table}")
                    deleted = cursor.rowcount

                    self.logger.info(f"  {table}: {count_before}건 -> 삭제 {deleted}건")
                    result[table] = deleted

                conn.commit()
                self.logger.info("초기화 완료 (COMMIT)")

            except Exception as e:
                conn.rollback()
                self.logger.error(f"초기화 실패: {e}")
                raise
            finally:
                cursor.close()

        return result

    def run_test_batch(
        self,
        farm_list: str = TEST_FARM_LIST,
        dates: Optional[List[str]] = None,
        parallel: int = 4,
    ) -> dict:
        """테스트용 배치 실행

        지정된 날짜들에 대해 SP_INS_WEEK_MAIN 순차 실행

        Args:
            farm_list: 테스트 농장 목록 (콤마 구분)
            dates: 실행할 날짜 목록 (YYYYMMDD), None이면 기본 테스트 날짜 사용
            parallel: 병렬 처리 레벨

        Returns:
            실행 결과 딕셔너리
        """
        if dates is None:
            dates = TEST_DATES

        self.logger.info("=" * 60)
        self.logger.info("테스트 배치 실행 시작")
        self.logger.info(f"  날짜 수: {len(dates)}")
        self.logger.info(f"  농장 목록: {farm_list}")
        self.logger.info(f"  병렬 레벨: {parallel}")
        self.logger.info("=" * 60)

        results = []

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                for i, dt in enumerate(dates, 1):
                    self.logger.info(f"[{i}/{len(dates)}] 날짜: {dt}")

                    # SP_INS_WEEK_MAIN(P_DAY_GB, P_BASE_DT, P_PARALLEL_LEVEL, P_TEST_MODE, P_FARM_LIST)
                    # P_BASE_DT는 DATE 타입이므로 TO_DATE 사용
                    sql = """
                    BEGIN
                        SP_INS_WEEK_MAIN(:p_day_gb, TO_DATE(:p_base_dt, 'YYYYMMDD'), :p_parallel, :p_test_mode, :p_farm_list);
                    END;
                    """
                    cursor.execute(sql, {
                        'p_day_gb': 'WEEK',
                        'p_base_dt': dt,
                        'p_parallel': parallel,
                        'p_test_mode': '',  # 빈 문자열 (테스트 모드 아님 - 실제 데이터 생성)
                        'p_farm_list': farm_list,
                    })
                    conn.commit()

                    self.logger.info(f"  완료: {dt}")
                    results.append({'date': dt, 'status': 'success'})

            except Exception as e:
                self.logger.error(f"배치 실행 실패: {e}")
                results.append({'date': dt, 'status': 'error', 'error': str(e)})
                raise
            finally:
                cursor.close()

        self.logger.info("=" * 60)
        self.logger.info(f"테스트 배치 완료: {len([r for r in results if r['status'] == 'success'])}/{len(dates)}")
        self.logger.info("=" * 60)

        return {
            'total': len(dates),
            'success': len([r for r in results if r['status'] == 'success']),
            'failed': len([r for r in results if r['status'] == 'error']),
            'details': results,
        }

    def run_single_farm(
        self,
        farm_no: int,
        dt_from: Optional[str] = None,
        dt_to: Optional[str] = None,
    ) -> dict:
        """단일 농장 수동 ETL 실행

        웹시스템에서 특정 농장의 주간 리포트를 수동으로 생성할 때 사용

        Args:
            farm_no: 농장번호
            dt_from: 시작일 (YYYYMMDD), None이면 지난주 월요일
            dt_to: 종료일 (YYYYMMDD), None이면 지난주 일요일

        Returns:
            실행 결과 딕셔너리
        """
        self.logger.info("=" * 60)
        self.logger.info(f"단일 농장 수동 ETL 시작: farm_no={farm_no}")
        self.logger.info("=" * 60)

        # 날짜 자동 계산 (지정되지 않은 경우)
        if not dt_from or not dt_to:
            today = datetime.now()
            # 지난주 월요일 (오늘 기준 이번주 월요일 - 7일)
            this_monday = today - timedelta(days=today.weekday())
            last_monday = this_monday - timedelta(days=7)
            last_sunday = last_monday + timedelta(days=6)

            dt_from = dt_from or last_monday.strftime('%Y%m%d')
            dt_to = dt_to or last_sunday.strftime('%Y%m%d')

        self.logger.info(f"기간: {dt_from} ~ {dt_to}")

        # 주차 정보 계산
        dt_from_obj = datetime.strptime(dt_from, '%Y%m%d')
        year = dt_from_obj.isocalendar()[0]
        week_no = dt_from_obj.isocalendar()[1]

        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    # 1. 농장 정보 조회
                    cursor.execute("""
                        SELECT FARM_NO, FARM_NM, PRINCIPAL_NM, SIGUN_CD,
                               NVL(COUNTRY_CODE, 'KOR') AS LOCALE
                        FROM TA_FARM WHERE FARM_NO = :farm_no AND USE_YN = 'Y'
                    """, {'farm_no': farm_no})
                    farm_row = cursor.fetchone()

                    if not farm_row:
                        return {
                            'status': 'error',
                            'error': f'농장번호 {farm_no}를 찾을 수 없습니다.',
                        }

                    farm_info = {
                        'FARM_NO': farm_row[0],
                        'FARM_NM': farm_row[1],
                        'PRINCIPAL_NM': farm_row[2],
                        'SIGUN_CD': farm_row[3],
                        'LOCALE': farm_row[4],
                    }

                    self.logger.info(f"농장 정보: {farm_info['FARM_NM']} ({farm_no})")

                    # 2. 전국 탕박 평균 단가 조회
                    national_price = self._get_national_price(cursor, dt_from, dt_to)
                    self.logger.info(f"전국 탕박 평균 단가: {national_price:,}원")

                    # 3. 마스터 레코드 생성
                    master_seq = self._create_master(cursor, year, week_no, dt_from, dt_to)
                    self.logger.info(f"마스터 생성: SEQ={master_seq}")

                    # 4. TS_INS_WEEK 초기 레코드 생성
                    self._create_week_records(cursor, master_seq, [farm_info], year, week_no, dt_from, dt_to)
                    conn.commit()

                finally:
                    cursor.close()

            # 5. FarmProcessor로 처리
            from .farm_processor import FarmProcessor

            with self.db.get_connection() as conn:
                processor = FarmProcessor(
                    conn=conn,
                    master_seq=master_seq,
                    farm_no=farm_no,
                    locale=farm_info['LOCALE'],
                )
                result = processor.process(
                    dt_from=dt_from,
                    dt_to=dt_to,
                    national_price=national_price,
                )

            # 6. 마스터 상태 업데이트
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    if result.get('status') == 'success':
                        self._update_master(cursor, master_seq, 1, 1, 0)
                    else:
                        self._update_master(cursor, master_seq, 1, 0, 1)
                    conn.commit()
                finally:
                    cursor.close()

            self.logger.info("=" * 60)
            self.logger.info(f"단일 농장 수동 ETL 완료: farm_no={farm_no}, status={result.get('status')}")
            self.logger.info("=" * 60)

            return {
                'status': result.get('status', 'success'),
                'farm_no': farm_no,
                'master_seq': master_seq,
                'dt_from': dt_from,
                'dt_to': dt_to,
            }

        except Exception as e:
            self.logger.error(f"단일 농장 ETL 실패: {e}", exc_info=True)
            return {
                'status': 'error',
                'farm_no': farm_no,
                'error': str(e),
            }
