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

from ..common import Config, Database, setup_logger, now_kst
from ..common.farm_service import SERVICE_FARM_SQL
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

TEST_FARM_LIST = '1387,2807,4448,1456,1517,848'


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

    def _check_schedule_enabled(self) -> bool:
        """시스템 스케줄 실행 여부 확인

        TA_SYS_CONFIG 테이블의 INS_SCHEDULE_YN 값을 확인합니다.
        'Y' 또는 'T'인 경우 True를 반환합니다.
        - 'Y': 운영 모드
        - 'T': 테스트 모드 (현재는 'Y'와 동일하게 동작, 향후 분기 예정)
        - 'N': 비활성화

        Returns:
            bool: INS_SCHEDULE_YN = 'Y' 또는 'T'이면 True, 그 외 False
        """
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        SELECT INS_SCHEDULE_YN
                        FROM TA_SYS_CONFIG
                        WHERE SEQ = 1
                    """)
                    result = cursor.fetchone()

                    if result and result[0] in ('Y', 'T'):
                        mode = '운영' if result[0] == 'Y' else '테스트'
                        self.logger.info(f"INS_SCHEDULE_YN = '{result[0]}' - 스케줄 실행 활성화 ({mode} 모드)")
                        return True
                    else:
                        value = result[0] if result else 'NULL'
                        self.logger.warning(f"INS_SCHEDULE_YN = '{value}' - 스케줄 실행 비활성화")
                        return False
                finally:
                    cursor.close()
        except Exception as e:
            self.logger.error(f"INS_SCHEDULE_YN 확인 실패: {e}")
            # 오류 시 기본적으로 실행하지 않음 (안전 모드)
            return False

    def run(
        self,
        base_date: Optional[str] = None,
        test_mode: bool = False,
        skip_productivity: bool = False,  # 생산성 수집 활성화 (테이블 생성 필요)
        skip_weather: bool = False,
        dry_run: bool = False,
        init_all: bool = False,  # 전체 삭제 여부 (--test --init-all)
        init_week: bool = False,  # 해당 주차만 삭제 여부 (--test --init-week)
        farm_list: Optional[str] = None,  # 처리할 농장 목록 (콤마 구분)
        exclude_farms: Optional[str] = None,  # 제외할 농장 목록 (콤마 구분)
        schedule_group: Optional[str] = None,  # 스케줄 그룹 (AM7, PM2)
    ) -> dict:
        """ETL 파이프라인 실행

        Args:
            base_date: 기준 날짜 (YYYYMMDD), None이면 오늘
            test_mode: 테스트 모드 (금주 데이터)
            skip_productivity: 생산성 수집 스킵 (테이블 생성 후 False로 설정)
            skip_weather: 기상청 수집 스킵
            dry_run: 실제 실행 없이 설정만 확인
            init_all: 전체 데이터 삭제 (test_mode=True일 때만 적용, --test --init-all)
            init_week: 해당 주차 데이터만 삭제 (test_mode=True일 때만 적용, --test --init-week)
            farm_list: 처리할 농장 목록 (콤마 구분, 예: "1387,2807")
                      지정하면 해당 농장만 삭제/수집/리포트 생성
            exclude_farms: 제외할 농장 목록 (콤마 구분, 예: "848,1234")
                          farm_list가 지정되면 무시됨
            schedule_group: 스케줄 그룹 필터 (AM7, PM2, None=전체)
                           TS_INS_SERVICE.SCHEDULE_GROUP_WEEK 기준 필터링

        Returns:
            실행 결과 딕셔너리
        """
        self.logger.info("=" * 60)
        self.logger.info("InsightPig Weekly ETL 시작")
        self.logger.info("=" * 60)

        # INS_SCHEDULE_YN 체크 (시스템 스케줄 실행 여부)
        if not self._check_schedule_enabled():
            self.logger.warning("INS_SCHEDULE_YN = 'N' - 스케줄 실행이 비활성화되어 있습니다.")
            return {
                'status': 'skipped',
                'reason': 'INS_SCHEDULE_YN is N',
                'message': '시스템 설정(TA_SYS_CONFIG.INS_SCHEDULE_YN)이 N으로 설정되어 ETL이 실행되지 않았습니다.',
            }

        # 기준 날짜 설정 (한국 시간 기준)
        if base_date:
            base_dt = datetime.strptime(base_date, '%Y%m%d')
        else:
            base_dt = now_kst()

        # 기간 계산: 기준일의 지난주 (월~일, 7일)
        # test_mode 여부와 관계없이 항상 지난주 전체를 처리
        # 예: 기준일 2025-11-10 (월) → 지난주 2025-11-03 (월) ~ 2025-11-09 (일)
        #
        # 지난주 일요일 계산:
        # - 일요일(weekday=6)인 경우: 7일 전이 지난주 일요일
        # - 그 외: (weekday+1)일 전이 지난주 일요일
        # 공식: (weekday + 1) % 7 또는 7 -> 일요일이면 7, 아니면 weekday+1
        days_to_last_sunday = (base_dt.weekday() + 1) % 7 or 7
        last_sunday = base_dt - timedelta(days=days_to_last_sunday)
        last_monday = last_sunday - timedelta(days=6)
        dt_from = last_monday.strftime('%Y%m%d')
        dt_to = last_sunday.strftime('%Y%m%d')

        # 주차 계산 (ISO Week) - 지난주 기준으로 계산
        year = int(last_sunday.strftime('%G'))  # ISO year
        week_no = int(last_sunday.strftime('%V'))  # ISO week

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
            # Step 1: 외부 데이터 수집 (생산성 + 기상청 병렬 처리)
            self.logger.info("-" * 40)
            self.logger.info("Step 1: 외부 데이터 수집 (병렬 처리)")
            if farm_list:
                self.logger.info(f"  대상 농장: {farm_list}")
            if exclude_farms:
                self.logger.info(f"  제외 농장: {exclude_farms}")
            collect_result = self._collect_external_data(
                dt_to, skip_productivity, skip_weather, farm_list, exclude_farms
            )
            result['steps']['productivity'] = collect_result.get('productivity', 'skipped')
            result['steps']['weather'] = collect_result.get('weather', 'skipped')

            # Step 2: 주간 리포트 생성
            self.logger.info("-" * 40)
            self.logger.info("Step 2: 주간 리포트 생성")
            report_result = self._generate_weekly_report(
                year, week_no, dt_from, dt_to, test_mode,
                init_all=init_all, init_week=init_week,
                farm_list=farm_list, exclude_farms=exclude_farms,
                schedule_group=schedule_group
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

    def _collect_productivity(
        self,
        stat_date: str,
        farm_list: Optional[str] = None,
        exclude_farms: Optional[str] = None,
    ) -> dict:
        """생산성 데이터 수집

        Args:
            stat_date: 기준 날짜 (YYYYMMDD)
            farm_list: 수집 대상 농장 목록 (콤마 구분, 예: "1387,2807")
                      None이면 전체 서비스 농장 대상
            exclude_farms: 제외할 농장 목록 (콤마 구분, 예: "848,1234")

        Returns:
            수집 결과 딕셔너리

        Note:
            API 파라미터 중 statDate 외에는 고정값 사용 (period=Y 등)
            farm_list가 지정되면 해당 농장만 수집, exclude_farms는 무시됨
        """
        try:
            collector = ProductivityCollector(self.config, self.db)

            # farm_list가 지정되면 해당 농장만 수집
            if farm_list:
                farm_nos = [int(f.strip()) for f in farm_list.split(',') if f.strip()]
                farm_list_dict = [{'FARM_NO': f} for f in farm_nos]
                self.logger.info(f"생산성 수집 대상 농장: {farm_nos}")
                count = collector.run(stat_date=stat_date, farm_list=farm_list_dict)
            else:
                count = collector.run(stat_date=stat_date, exclude_farms=exclude_farms)

            return {'status': 'success', 'count': count}
        except Exception as e:
            self.logger.error(f"생산성 데이터 수집 실패: {e}", exc_info=True)
            # 테이블 미존재 등 오류 시에도 ETL 계속 진행
            return {'status': 'error', 'error': str(e), 'count': 0}

    def _collect_weather(self) -> dict:
        """기상청 데이터 수집"""
        try:
            collector = WeatherCollector(self.config, self.db)
            count = collector.run()
            return {'status': 'success', 'count': count}
        except Exception as e:
            self.logger.error(f"기상청 데이터 수집 실패: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e), 'count': 0}

    def _collect_single_farm_productivity(self, farm_no: int, stat_date: str) -> dict:
        """단일 농장 생산성 데이터 수집

        btnInsSmsCreate 등에서 단일 농장의 리포트 생성 시 호출

        Args:
            farm_no: 농장번호
            stat_date: 기준 날짜 (YYYYMMDD)

        Returns:
            수집 결과 딕셔너리
        """
        try:
            collector = ProductivityCollector(self.config, self.db)
            # 단일 농장만 수집
            farm_list = [{'FARM_NO': farm_no}]
            data = collector.collect(farm_list=farm_list, stat_date=stat_date, period='W')
            if data:
                count = collector.save(data)
                return {'status': 'success', 'count': count}
            else:
                return {'status': 'success', 'count': 0, 'message': 'no data'}
        except Exception as e:
            self.logger.error(f"단일 농장 생산성 수집 실패 (farm_no={farm_no}): {e}", exc_info=True)
            # 생산성 수집 실패해도 ETL은 계속 진행
            return {'status': 'error', 'error': str(e), 'count': 0}

    def _collect_external_data(
        self,
        stat_date: str,
        skip_productivity: bool = False,
        skip_weather: bool = False,
        farm_list: Optional[str] = None,
        exclude_farms: Optional[str] = None,
    ) -> dict:
        """외부 데이터 수집 (생산성 + 기상청 병렬 처리)

        Args:
            stat_date: 기준 날짜 (YYYYMMDD)
            skip_productivity: 생산성 수집 스킵
            skip_weather: 기상청 수집 스킵
            farm_list: 수집 대상 농장 목록 (콤마 구분, 예: "1387,2807")
                      None이면 전체 서비스 농장 대상
            exclude_farms: 제외할 농장 목록 (콤마 구분, 예: "848,1234")
                          farm_list가 지정되면 무시됨

        Returns:
            수집 결과 딕셔너리
        """
        result = {}

        # 둘 다 스킵이면 바로 반환
        if skip_productivity and skip_weather:
            self.logger.info("  생산성/기상청 수집 모두 스킵")
            return {'productivity': 'skipped', 'weather': 'skipped'}

        # 하나만 실행하면 병렬 처리 불필요
        if skip_productivity:
            self.logger.info("  기상청 데이터 수집 (생산성 스킵)")
            result['productivity'] = 'skipped'
            result['weather'] = self._collect_weather()
            return result

        if skip_weather:
            self.logger.info("  생산성 데이터 수집 (기상청 스킵)")
            result['productivity'] = self._collect_productivity(stat_date, farm_list, exclude_farms)
            result['weather'] = 'skipped'
            return result

        # 둘 다 실행: 병렬 처리
        self.logger.info("  생산성 + 기상청 병렬 수집 시작")

        with ThreadPoolExecutor(max_workers=2) as executor:
            # 병렬로 두 작업 제출
            future_productivity = executor.submit(self._collect_productivity, stat_date, farm_list, exclude_farms)
            future_weather = executor.submit(self._collect_weather)

            # 결과 수집
            try:
                result['productivity'] = future_productivity.result(timeout=300)
                self.logger.info(f"  생산성 수집 완료: {result['productivity']}")
            except Exception as e:
                self.logger.error(f"  생산성 수집 실패: {e}")
                result['productivity'] = {'status': 'error', 'error': str(e), 'count': 0}

            try:
                result['weather'] = future_weather.result(timeout=300)
                self.logger.info(f"  기상청 수집 완료: {result['weather']}")
            except Exception as e:
                self.logger.error(f"  기상청 수집 실패: {e}")
                result['weather'] = {'status': 'error', 'error': str(e), 'count': 0}

        self.logger.info("  외부 데이터 수집 완료")
        return result

    def _generate_weekly_report(
        self,
        year: int,
        week_no: int,
        dt_from: str,
        dt_to: str,
        test_mode: bool,
        init_all: bool = False,
        init_week: bool = False,
        use_async: bool = True,  # 병렬 처리 활성화 (연결 풀 사용으로 thread-safe 보장)
        farm_list: Optional[str] = None,
        exclude_farms: Optional[str] = None,
        schedule_group: Optional[str] = None,  # 스케줄 그룹 (AM7, PM2)
    ) -> dict:
        """주간 리포트 생성

        Args:
            year: 연도
            week_no: 주차
            dt_from: 시작일
            dt_to: 종료일
            test_mode: 테스트 모드
            init_all: 전체 데이터 삭제 (--test --init-all)
            init_week: 해당 주차 데이터만 삭제 (--test --init-week)
            use_async: 비동기 병렬 처리 사용 여부
            farm_list: 처리할 농장 목록 (콤마 구분, None이면 전체)
            exclude_farms: 제외할 농장 목록 (콤마 구분, 예: "848,1234")
            schedule_group: 스케줄 그룹 필터 (AM7, PM2, None=전체)

        Returns:
            처리 결과 딕셔너리
        """
        if use_async:
            return self._generate_weekly_report_async(year, week_no, dt_from, dt_to, test_mode, init_all, init_week, farm_list, exclude_farms, schedule_group)
        else:
            return self._generate_weekly_report_python(year, week_no, dt_from, dt_to, test_mode, init_all, init_week, farm_list, exclude_farms, schedule_group)

    def _generate_weekly_report_python(
        self,
        year: int,
        week_no: int,
        dt_from: str,
        dt_to: str,
        test_mode: bool,
        init_all: bool = False,
        init_week: bool = False,
        farm_list: Optional[str] = None,
        exclude_farms: Optional[str] = None,
        schedule_group: Optional[str] = None,
    ) -> dict:
        """주간 리포트 생성 (동기 처리)

        Args:
            exclude_farms: 제외할 농장 목록 (콤마 구분, 예: "848,1234")
        """
        from .farm_processor import FarmProcessor

        self.logger.info(f"Python ETL 실행: {year}년 {week_no}주, 기간={dt_from}~{dt_to}")
        if exclude_farms:
            self.logger.info(f"제외 농장: {exclude_farms}")

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

                # 2. 기존 데이터 삭제 (test_mode + init_all/init_week에 따라)
                self._delete_existing_master(cursor, year, week_no, farm_list, test_mode, init_all, init_week)
                conn.commit()

                # 3. 마스터 레코드 생성
                master_seq = self._create_master(cursor, year, week_no, dt_from, dt_to)
                self.logger.info(f"마스터 SEQ: {master_seq}")

                # 4. 대상 농장 조회
                farms = self._get_target_farms(cursor, farm_list, test_mode, exclude_farms, schedule_group)
                target_cnt = len(farms)
                self.logger.info(f"대상 농장: {target_cnt}개" + (f" (스케줄 그룹: {schedule_group})" if schedule_group else ""))

                if target_cnt == 0:
                    self.logger.warning("대상 농장이 없습니다.")
                    return {'status': 'complete', 'method': 'python', 'target_cnt': 0}

                # 5. 농장별 초기 레코드 생성 (TS_INS_WEEK)
                self._create_week_records(cursor, master_seq, farms, year, week_no, dt_from, dt_to)
                conn.commit()

                # 6. 농장별 처리
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
        init_all: bool = False,
        init_week: bool = False,
        farm_list: Optional[str] = None,
        exclude_farms: Optional[str] = None,
        schedule_group: Optional[str] = None,
    ) -> dict:
        """비동기 병렬 처리를 사용한 주간 리포트 생성

        농장별 병렬 처리 (각 농장은 연결 풀에서 독립 연결 획득)

        Args:
            year: 연도
            week_no: 주차
            dt_from: 시작일
            dt_to: 종료일
            test_mode: 테스트 모드
            init_all: 전체 데이터 삭제 (--test --init-all)
            init_week: 해당 주차 데이터만 삭제 (--test --init-week)
            farm_list: 처리할 농장 목록 (콤마 구분, None이면 전체)
            exclude_farms: 제외할 농장 목록 (콤마 구분, 예: "848,1234")

        Returns:
            처리 결과 딕셔너리
        """
        from .async_processor import AsyncFarmProcessor

        self.logger.info(f"Python ETL (비동기) 실행: {year}년 {week_no}주, 기간={dt_from}~{dt_to}")
        if exclude_farms:
            self.logger.info(f"제외 농장: {exclude_farms}")

        # 설정에서 병렬 처리 설정 가져오기
        max_farm_workers = self.config.processing.get('max_farm_workers', 4)

        self.logger.info(f"  농장 병렬 처리: {max_farm_workers}개")

        # 연결 풀 생성 (농장별 독립 연결 제공)
        # pool_max는 max_farm_workers + 2로 설정 → 동시 사용 연결 수 제한
        # 농장 수가 많아도 동시에 사용하는 연결은 max_farm_workers개로 제한됨
        pool_db = Database(self.config, use_pool=True, pool_min=2, pool_max=max_farm_workers + 2)

        target_cnt = 0
        complete_cnt = 0
        error_cnt = 0
        farm_results = []
        master_seq = None

        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    # 1. 전국 탕박 평균 단가 계산
                    national_price = self._get_national_price(cursor, dt_from, dt_to)
                    self.logger.info(f"전국 탕박 평균 단가: {national_price}원")

                    # 2. 기존 데이터 삭제 (test_mode + init_all/init_week에 따라)
                    self._delete_existing_master(cursor, year, week_no, farm_list, test_mode, init_all, init_week)
                    conn.commit()

                    # 3. 마스터 레코드 생성
                    master_seq = self._create_master(cursor, year, week_no, dt_from, dt_to)
                    self.logger.info(f"마스터 SEQ: {master_seq}")

                    # 4. 대상 농장 조회
                    farms = self._get_target_farms(cursor, farm_list, test_mode, exclude_farms, schedule_group)
                    target_cnt = len(farms)
                    self.logger.info(f"대상 농장: {target_cnt}개" + (f" (스케줄 그룹: {schedule_group})" if schedule_group else ""))

                    if target_cnt == 0:
                        self.logger.warning("대상 농장이 없습니다.")
                        return {'status': 'complete', 'method': 'python_async', 'target_cnt': 0}

                    # 5. 농장별 초기 레코드 생성 (TS_INS_WEEK)
                    self._create_week_records(cursor, master_seq, farms, year, week_no, dt_from, dt_to)
                    conn.commit()

                finally:
                    cursor.close()

            # 6. 농장별 병렬 처리 (각 농장은 연결 풀에서 독립 연결 획득)
            def process_single_farm(farm: dict) -> dict:
                """단일 농장 처리 (풀에서 독립 연결 획득)"""
                farm_no = farm['FARM_NO']
                locale = farm.get('LOCALE', 'KOR')

                try:
                    # 연결 풀에서 독립 연결 획득 (thread-safe)
                    with pool_db.get_connection() as farm_conn:
                        processor = AsyncFarmProcessor(
                            farm_conn,
                            master_seq,
                            farm_no,
                            locale,
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

            # 7. 마스터 상태 업데이트
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

        finally:
            # 장애 발생 시에도 연결 풀 반드시 종료
            pool_db.close()
            self.logger.info("연결 풀 종료")

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

    def _delete_all_test_data(self, cursor, farm_list: Optional[str] = None) -> dict:
        """테스트 모드: 전체 테스트 데이터 삭제

        테스트 시 깨끗한 상태에서 시작하기 위해 전체 데이터를 삭제합니다.
        farm_list가 지정되면 해당 농장의 전체 데이터만 삭제합니다.
        삭제 순서: TS_INS_WEEK_SUB → TS_INS_JOB_LOG → TS_INS_WEEK → TS_INS_MASTER

        Args:
            cursor: DB 커서
            farm_list: 농장 목록 (콤마 구분, None이면 전체)

        Returns:
            삭제된 건수 딕셔너리
        """
        deleted = {'master': 0, 'week': 0, 'week_sub': 0, 'job_log': 0}

        # farm_list가 지정된 경우 해당 농장만 삭제
        if farm_list:
            farm_nos = [int(f.strip()) for f in farm_list.split(',') if f.strip()]
            farm_placeholders = ', '.join([f':f{i}' for i in range(len(farm_nos))])
            farm_params = {f'f{i}': f for i, f in enumerate(farm_nos)}

            self.logger.info(f"지정 농장 전체 데이터 삭제 시작: {farm_nos}")

            # 1. TS_INS_WEEK_SUB 삭제 (해당 농장만)
            cursor.execute(f"DELETE FROM TS_INS_WEEK_SUB WHERE FARM_NO IN ({farm_placeholders})", farm_params)
            deleted['week_sub'] = cursor.rowcount

            # 2. TS_INS_JOB_LOG 삭제 (해당 농장만)
            cursor.execute(f"DELETE FROM TS_INS_JOB_LOG WHERE FARM_NO IN ({farm_placeholders})", farm_params)
            deleted['job_log'] = cursor.rowcount

            # 3. TS_INS_WEEK 삭제 (해당 농장만)
            cursor.execute(f"DELETE FROM TS_INS_WEEK WHERE FARM_NO IN ({farm_placeholders})", farm_params)
            deleted['week'] = cursor.rowcount

            # 4. TS_INS_MASTER는 삭제하지 않음 (다른 농장 데이터 존재 가능)
            # 빈 마스터는 별도 정리 필요 시 처리

            self.logger.info(f"지정 농장 전체 데이터 삭제 완료: {deleted}")
        else:
            # farm_list 없으면 전체 삭제
            self.logger.info("전체 테스트 데이터 삭제 시작")

            # 1. TS_INS_WEEK_SUB 전체 삭제
            cursor.execute("DELETE FROM TS_INS_WEEK_SUB")
            deleted['week_sub'] = cursor.rowcount

            # 2. TS_INS_JOB_LOG 전체 삭제
            cursor.execute("DELETE FROM TS_INS_JOB_LOG")
            deleted['job_log'] = cursor.rowcount

            # 3. TS_INS_WEEK 전체 삭제
            cursor.execute("DELETE FROM TS_INS_WEEK")
            deleted['week'] = cursor.rowcount

            # 4. TS_INS_MASTER 전체 삭제
            cursor.execute("DELETE FROM TS_INS_MASTER")
            deleted['master'] = cursor.rowcount

            self.logger.info(f"테스트 데이터 전체 삭제 완료: {deleted}")

        return deleted

    def _delete_existing_master(
        self, cursor, year: int, week_no: int, farm_list: Optional[str] = None,
        test_mode: bool = False, init_all: bool = False, init_week: bool = False
    ) -> dict:
        """기존 테스트 데이터 삭제

        테스트 시 중복 오류 방지를 위해 기존 데이터를 삭제합니다.

        삭제 정책:
        - test_mode=False: 삭제하지 않음 (운영 데이터 보호)
        - test_mode=True AND init_all=False AND init_week=False: 삭제하지 않음
        - test_mode=True AND init_all=True: 전체 테스트 데이터 삭제 (farm_list 있으면 해당 농장만)
        - test_mode=True AND init_week=True: 해당 주차 데이터만 삭제 (farm_list 있으면 해당 농장만)

        Args:
            cursor: DB 커서
            year: 연도
            week_no: 주차
            farm_list: 농장 목록 (콤마 구분, None이면 전체)
            test_mode: 테스트 모드 여부
            init_all: 전체 데이터 삭제 (--test --init-all)
            init_week: 해당 주차 데이터만 삭제 (--test --init-week)

        Returns:
            삭제된 건수 딕셔너리
        """
        # 테스트 모드가 아니면 삭제하지 않음 (운영 데이터 보호)
        if not test_mode:
            self.logger.info("운영 모드: 기존 데이터 삭제 스킵 (test_mode=False)")
            return {'master': 0, 'week': 0, 'week_sub': 0, 'job_log': 0}

        # init_all과 init_week 둘 다 False이면 삭제하지 않음 (--test만 사용한 경우)
        if not init_all and not init_week:
            self.logger.info("테스트 모드: 기존 데이터 삭제 스킵 (init_all=False, init_week=False)")
            return {'master': 0, 'week': 0, 'week_sub': 0, 'job_log': 0}

        # --test --init-all: 전체 테스트 데이터 삭제 (farm_list 있으면 해당 농장만)
        if init_all:
            return self._delete_all_test_data(cursor, farm_list)

        # --test --init-week: 해당 주차 데이터만 삭제
        self.logger.info(f"해당 주차 데이터 삭제 시작: {year}년 {week_no}주")
        deleted = {'master': 0, 'week': 0, 'week_sub': 0, 'job_log': 0}

        # 1. 해당 연도/주차의 마스터 SEQ 조회
        cursor.execute("""
            SELECT SEQ FROM TS_INS_MASTER
            WHERE REPORT_YEAR = :year AND REPORT_WEEK_NO = :week_no AND DAY_GB = 'WEEK'
        """, {'year': year, 'week_no': week_no})
        master_rows = cursor.fetchall()

        if not master_rows:
            self.logger.info(f"삭제할 기존 데이터 없음: {year}년 {week_no}주")
            return deleted

        master_seqs = [row[0] for row in master_rows]
        self.logger.info(f"기존 마스터 SEQ: {master_seqs}")

        # farm_list가 지정된 경우 해당 농장만 삭제
        if farm_list:
            farm_nos = [int(f.strip()) for f in farm_list.split(',') if f.strip()]
            farm_placeholders = ', '.join([f':f{i}' for i in range(len(farm_nos))])
            farm_params = {f'f{i}': f for i, f in enumerate(farm_nos)}
        else:
            farm_nos = None

        for master_seq in master_seqs:
            params = {'master_seq': master_seq}
            if farm_nos:
                params.update(farm_params)

            # 2. TS_INS_WEEK_SUB 삭제
            if farm_nos:
                cursor.execute(f"""
                    DELETE FROM TS_INS_WEEK_SUB
                    WHERE MASTER_SEQ = :master_seq AND FARM_NO IN ({farm_placeholders})
                """, params)
            else:
                cursor.execute("""
                    DELETE FROM TS_INS_WEEK_SUB WHERE MASTER_SEQ = :master_seq
                """, params)
            deleted['week_sub'] += cursor.rowcount

            # 3. TS_INS_JOB_LOG 삭제
            if farm_nos:
                cursor.execute(f"""
                    DELETE FROM TS_INS_JOB_LOG
                    WHERE MASTER_SEQ = :master_seq AND FARM_NO IN ({farm_placeholders})
                """, params)
            else:
                cursor.execute("""
                    DELETE FROM TS_INS_JOB_LOG WHERE MASTER_SEQ = :master_seq
                """, params)
            deleted['job_log'] += cursor.rowcount

            # 4. TS_INS_WEEK 삭제
            if farm_nos:
                cursor.execute(f"""
                    DELETE FROM TS_INS_WEEK
                    WHERE MASTER_SEQ = :master_seq AND FARM_NO IN ({farm_placeholders})
                """, params)
            else:
                cursor.execute("""
                    DELETE FROM TS_INS_WEEK WHERE MASTER_SEQ = :master_seq
                """, params)
            deleted['week'] += cursor.rowcount

            # 5. TS_INS_MASTER 삭제 (farm_list 지정 시 해당 농장이 모두 삭제되었을 때만)
            if not farm_nos:
                cursor.execute("""
                    DELETE FROM TS_INS_MASTER WHERE SEQ = :master_seq
                """, {'master_seq': master_seq})
                deleted['master'] += cursor.rowcount
            else:
                # farm_list 지정 시: 해당 마스터에 남은 농장이 없으면 마스터도 삭제
                cursor.execute("""
                    SELECT COUNT(*) FROM TS_INS_WEEK WHERE MASTER_SEQ = :master_seq
                """, {'master_seq': master_seq})
                remaining = cursor.fetchone()[0]
                if remaining == 0:
                    cursor.execute("""
                        DELETE FROM TS_INS_MASTER WHERE SEQ = :master_seq
                    """, {'master_seq': master_seq})
                    deleted['master'] += cursor.rowcount

        self.logger.info(f"기존 데이터 삭제 완료: {deleted}")
        return deleted

    def _delete_single_farm_data(
        self, cursor, year: int, week_no: int, farm_no: int
    ) -> dict:
        """단일 농장 데이터 삭제 (동일 연도/주차, 해당 농장만)

        run_single_farm에서 특정 농장의 주간 리포트를 수동 재생성할 때 사용.
        해당 농장의 기존 데이터만 삭제합니다.

        Args:
            cursor: DB 커서
            year: 연도
            week_no: 주차
            farm_no: 농장번호

        Returns:
            삭제된 건수 딕셔너리
        """
        deleted = {'week': 0, 'week_sub': 0, 'job_log': 0}

        # 1. 해당 연도/주차의 마스터 SEQ 조회
        cursor.execute("""
            SELECT SEQ FROM TS_INS_MASTER
            WHERE REPORT_YEAR = :year AND REPORT_WEEK_NO = :week_no AND DAY_GB = 'WEEK'
        """, {'year': year, 'week_no': week_no})
        master_rows = cursor.fetchall()

        if not master_rows:
            self.logger.info(f"삭제할 기존 데이터 없음: {year}년 {week_no}주")
            return deleted

        master_seqs = [row[0] for row in master_rows]
        self.logger.info(f"단일 농장 삭제 대상 마스터 SEQ: {master_seqs}, 농장: {farm_no}")

        for master_seq in master_seqs:
            params = {'master_seq': master_seq, 'farm_no': farm_no}

            # 2. TS_INS_WEEK_SUB 삭제
            cursor.execute("""
                DELETE FROM TS_INS_WEEK_SUB
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
            """, params)
            deleted['week_sub'] += cursor.rowcount

            # 3. TS_INS_JOB_LOG 삭제
            cursor.execute("""
                DELETE FROM TS_INS_JOB_LOG
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
            """, params)
            deleted['job_log'] += cursor.rowcount

            # 4. TS_INS_WEEK 삭제
            cursor.execute("""
                DELETE FROM TS_INS_WEEK
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
            """, params)
            deleted['week'] += cursor.rowcount

        self.logger.info(f"단일 농장 데이터 삭제 완료: farm_no={farm_no}, {deleted}")
        return deleted

    def _create_master(self, cursor, year: int, week_no: int, dt_from: str, dt_to: str) -> int:
        """TS_INS_MASTER 레코드 생성 또는 기존 레코드 재사용

        동일 연도/주차의 마스터가 이미 존재하면 재사용합니다.
        """
        # 기존 마스터 확인
        cursor.execute("""
            SELECT SEQ FROM TS_INS_MASTER
            WHERE REPORT_YEAR = :year AND REPORT_WEEK_NO = :week_no AND DAY_GB = 'WEEK'
        """, {'year': year, 'week_no': week_no})
        existing = cursor.fetchone()

        if existing:
            master_seq = existing[0]
            # 상태를 RUNNING으로 업데이트
            cursor.execute("""
                UPDATE TS_INS_MASTER
                SET STATUS_CD = 'RUNNING', START_DT = SYSDATE
                WHERE SEQ = :seq
            """, {'seq': master_seq})
            return master_seq

        # 새 마스터 생성
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

    def _get_target_farms(
        self,
        cursor,
        farm_list: Optional[str],
        test_mode: bool,
        exclude_farms: Optional[str] = None,
        schedule_group: Optional[str] = None,
    ) -> List[dict]:
        """대상 농장 조회

        SQL은 src/common/farm_service.py의 SERVICE_FARM_SQL 사용 (중앙 관리).
        기존 cursor를 사용하여 연결 일관성 유지.

        Args:
            cursor: DB 커서
            farm_list: 처리할 농장 목록 (콤마 구분, None이면 전체)
            test_mode: 테스트 모드 여부
            exclude_farms: 제외할 농장 목록 (콤마 구분, 예: "848,1234")
            schedule_group: 스케줄 그룹 필터 (AM7, PM2, None=전체)
        """
        sql = SERVICE_FARM_SQL
        params = {}

        # 포함 필터링 (IN)
        if farm_list:
            farm_nos = [int(f.strip()) for f in farm_list.split(',') if f.strip()]
            placeholders = ', '.join([f':f{i}' for i in range(len(farm_nos))])
            sql = sql.replace('ORDER BY F.FARM_NO', f'AND F.FARM_NO IN ({placeholders})\n    ORDER BY F.FARM_NO')
            params.update({f'f{i}': f for i, f in enumerate(farm_nos)})

        # 제외 필터링 (NOT IN)
        if exclude_farms:
            exclude_nos = [int(f.strip()) for f in exclude_farms.split(',') if f.strip()]
            exclude_placeholders = ', '.join([f':ex{i}' for i in range(len(exclude_nos))])
            sql = sql.replace('ORDER BY F.FARM_NO', f'AND F.FARM_NO NOT IN ({exclude_placeholders})\n    ORDER BY F.FARM_NO')
            params.update({f'ex{i}': f for i, f in enumerate(exclude_nos)})

        # 스케줄 그룹 필터링 (AM7, PM2)
        if schedule_group:
            sql = sql.replace('ORDER BY F.FARM_NO', f"AND NVL(S.SCHEDULE_GROUP_WEEK, 'AM7') = :schedule_group\n    ORDER BY F.FARM_NO")
            params['schedule_group'] = schedule_group

        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

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
        """TS_INS_WEEK 초기 레코드 생성

        SCHEDULE_GROUP: ETL 시점의 스케줄 그룹 스냅샷 저장
        - pig3.1 카카오 발송 시 TS_INS_WEEK.SCHEDULE_GROUP 기준으로 필터링
        - ETL 시점과 발송 시점 사이에 설정 변경되어도 정확한 발송 보장
        """
        sql = """
        INSERT INTO TS_INS_WEEK (
            MASTER_SEQ, FARM_NO, REPORT_YEAR, REPORT_WEEK_NO,
            DT_FROM, DT_TO, FARM_NM, OWNER_NM, SIGUNGU_CD, STATUS_CD, SCHEDULE_GROUP
        ) VALUES (
            :master_seq, :farm_no, :year, :week_no,
            :dt_from, :dt_to, :farm_nm, :owner_nm, :sigun_cd, 'READY', :schedule_group
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
                'schedule_group': farm.get('SCHEDULE_GROUP_WEEK', 'AM7'),
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
        parallel: int = 4,  # noqa: ARG002 - 호환성 유지용
        exclude_farms: Optional[str] = None,
    ) -> dict:
        """테스트용 배치 실행

        지정된 날짜들에 대해 주간 리포트 생성

        Args:
            farm_list: 테스트 농장 목록 (콤마 구분)
            dates: 실행할 날짜 목록 (YYYYMMDD), None이면 기본 테스트 날짜 사용
            parallel: 미사용 (기존 API 호환성 유지)
            exclude_farms: 제외할 농장 목록 (콤마 구분, 예: "848,1234")

        Returns:
            실행 결과 딕셔너리
        """
        if dates is None:
            dates = TEST_DATES

        self.logger.info("=" * 60)
        self.logger.info("테스트 배치 실행 시작 (Python 프로세서)")
        self.logger.info(f"  날짜 수: {len(dates)}")
        self.logger.info(f"  농장 목록: {farm_list}")
        if exclude_farms:
            self.logger.info(f"  제외 농장: {exclude_farms}")
        self.logger.info("=" * 60)

        results = []

        for i, dt in enumerate(dates, 1):
            self.logger.info(f"[{i}/{len(dates)}] 날짜: {dt}")

            try:
                # 날짜에서 year, week_no, dt_from, dt_to 계산
                base_dt = datetime.strptime(dt, '%Y%m%d')

                # 기준일의 지난주 (월~일) 계산
                # 지난주 일요일: 일요일이면 7일 전, 그 외는 (weekday+1)일 전
                days_to_last_sunday = (base_dt.weekday() + 1) % 7 or 7
                last_sunday = base_dt - timedelta(days=days_to_last_sunday)
                last_monday = last_sunday - timedelta(days=6)
                dt_from = last_monday.strftime('%Y%m%d')
                dt_to = last_sunday.strftime('%Y%m%d')

                # ISO 주차 계산
                year = int(last_sunday.strftime('%G'))
                week_no = int(last_sunday.strftime('%V'))

                self.logger.info(f"  기간: {dt_from} ~ {dt_to}, {year}년 {week_no}주")

                # Python 프로세서로 주간 리포트 생성
                # init_delete: 첫 번째만 True (전체 삭제), 이후는 False
                init_delete = (i == 1)

                report_result = self._generate_weekly_report(
                    year=year,
                    week_no=week_no,
                    dt_from=dt_from,
                    dt_to=dt_to,
                    test_mode=True,
                    init_delete=init_delete,
                    use_async=True,
                    farm_list=farm_list,
                    exclude_farms=exclude_farms,
                )

                self.logger.info(f"  완료: {dt}")
                results.append({
                    'date': dt,
                    'status': report_result.get('status', 'success'),
                    'year': year,
                    'week_no': week_no,
                    'dt_from': dt_from,
                    'dt_to': dt_to,
                })

            except Exception as e:
                self.logger.error(f"배치 실행 실패: {e}")
                results.append({'date': dt, 'status': 'error', 'error': str(e)})
                raise

        self.logger.info("=" * 60)
        self.logger.info(f"테스트 배치 완료: {len([r for r in results if r['status'] in ('success', 'complete')])}/{len(dates)}")
        self.logger.info("=" * 60)

        return {
            'total': len(dates),
            'success': len([r for r in results if r['status'] in ('success', 'complete')]),
            'failed': len([r for r in results if r['status'] == 'error']),
            'details': results,
        }

    def run_single_farm(
        self,
        farm_no: int,
        ins_date: Optional[str] = None,
        skip_productivity: bool = False,
    ) -> dict:
        """단일 농장 수동 ETL 실행

        웹시스템에서 특정 농장의 주간 리포트를 수동으로 생성할 때 사용

        Args:
            farm_no: 농장번호
            ins_date: 기준일 INS_DT (YYYYMMDD), None이면 오늘
            skip_productivity: 생산성 수집 스킵 여부 (기본: False)

        Returns:
            실행 결과 딕셔너리
            - ins_date: 기준일
            - dt_from, dt_to: 리포트 기간 (지난주 월~일)
            - year, week_no: ISO 주차 정보
        """
        self.logger.info("=" * 60)
        self.logger.info(f"단일 농장 수동 ETL 시작: farm_no={farm_no}")
        self.logger.info("=" * 60)

        # 기준일 (INS_DT) 설정
        if ins_date:
            base_dt = datetime.strptime(ins_date, '%Y%m%d')
        else:
            base_dt = now_kst()
            ins_date = base_dt.strftime('%Y%m%d')

        # 지난주 계산 (INS_DT 기준)
        # 지난주 일요일: 일요일이면 7일 전, 그 외는 (weekday+1)일 전
        days_to_last_sunday = (base_dt.weekday() + 1) % 7 or 7
        last_sunday = base_dt - timedelta(days=days_to_last_sunday)
        last_monday = last_sunday - timedelta(days=6)
        dt_from = last_monday.strftime('%Y%m%d')
        dt_to = last_sunday.strftime('%Y%m%d')

        self.logger.info(f"기준일(INS_DT): {ins_date}")
        self.logger.info(f"리포트 기간: {dt_from} ~ {dt_to}")

        # 주차 정보 계산 (지난주 일요일 기준 ISO Week)
        year = int(last_sunday.strftime('%G'))
        week_no = int(last_sunday.strftime('%V'))

        try:
            # 0. 생산성 데이터 수집 (단일 농장)
            if not skip_productivity:
                self.logger.info("-" * 40)
                self.logger.info("Step 0: 생산성 데이터 수집 (단일 농장)")
                productivity_result = self._collect_single_farm_productivity(farm_no, dt_to)
                self.logger.info(f"생산성 수집 결과: {productivity_result}")

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    # 1. 농장 정보 조회 (SCHEDULE_GROUP_WEEK 포함)
                    cursor.execute("""
                        SELECT F.FARM_NO, F.FARM_NM, F.PRINCIPAL_NM, F.SIGUN_CD,
                               NVL(F.COUNTRY_CODE, 'KOR') AS LOCALE,
                               NVL(S.SCHEDULE_GROUP_WEEK, 'AM7') AS SCHEDULE_GROUP_WEEK
                        FROM TA_FARM F
                        LEFT JOIN VW_INS_SERVICE_ACTIVE S ON F.FARM_NO = S.FARM_NO
                        WHERE F.FARM_NO = :farm_no AND F.USE_YN = 'Y'
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
                        'SCHEDULE_GROUP_WEEK': farm_row[5],
                    }

                    self.logger.info(f"농장 정보: {farm_info['FARM_NM']} ({farm_no})")

                    # 2. 전국 탕박 평균 단가 조회
                    national_price = self._get_national_price(cursor, dt_from, dt_to)
                    self.logger.info(f"전국 탕박 평균 단가: {national_price:,}원")

                    # 3. 기존 데이터 삭제 (동일 연도/주차, 해당 농장만)
                    # run_single_farm은 단일 농장 수동 재생성이므로 해당 농장만 삭제
                    self._delete_single_farm_data(cursor, year, week_no, farm_no)
                    conn.commit()

                    # 4. 마스터 레코드 생성
                    master_seq = self._create_master(cursor, year, week_no, dt_from, dt_to)
                    self.logger.info(f"마스터 생성: SEQ={master_seq}")

                    # 5. TS_INS_WEEK 초기 레코드 생성
                    self._create_week_records(cursor, master_seq, [farm_info], year, week_no, dt_from, dt_to)
                    conn.commit()

                finally:
                    cursor.close()

            # 6. FarmProcessor로 처리
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

            # 7. 마스터 상태 업데이트
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

            # 8. 생성된 SHARE_TOKEN 조회
            share_token = None
            if result.get('status') == 'success':
                with self.db.get_connection() as conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute("""
                            SELECT SHARE_TOKEN FROM TS_INS_WEEK
                            WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
                        """, {'master_seq': master_seq, 'farm_no': farm_no})
                        row = cursor.fetchone()
                        if row:
                            share_token = row[0]
                    finally:
                        cursor.close()

            self.logger.info("=" * 60)
            self.logger.info(f"단일 농장 수동 ETL 완료: farm_no={farm_no}, status={result.get('status')}")
            if share_token:
                self.logger.info(f"SHARE_TOKEN: {share_token}")
            self.logger.info("=" * 60)

            return {
                'status': result.get('status', 'success'),
                'farm_no': farm_no,
                'master_seq': master_seq,
                'share_token': share_token,
                'year': year,
                'week_no': week_no,
                'ins_date': ins_date,
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

    def run_all_farms(
        self,
        base_date: Optional[str] = None,
        exclude_farms: Optional[str] = None,
        skip_productivity: bool = True,
        skip_weather: bool = True,
    ) -> dict:
        """전체 서비스 농장 대상 ETL 실행 (특정 농장 제외 가능)

        1회성 전체 농장 ETL 실행 시 사용
        예: 848 농장 제외하고 전체 농장 데이터 생성

        Args:
            base_date: 기준 날짜 (YYYYMMDD), None이면 오늘
            exclude_farms: 제외할 농장 목록 (콤마 구분, 예: "848,1234")
            skip_productivity: 생산성 수집 스킵 (기본: True)
            skip_weather: 기상청 수집 스킵 (기본: True)

        Returns:
            실행 결과 딕셔너리

        Usage:
            # 848 농장 제외하고 전체 농장 ETL 실행
            orchestrator.run_all_farms(exclude_farms="848")

            # 특정 날짜 기준, 848, 1234 농장 제외
            orchestrator.run_all_farms(base_date="20260106", exclude_farms="848,1234")
        """
        self.logger.info("=" * 60)
        self.logger.info("전체 농장 ETL 실행 (exclude_farms 지원)")
        if exclude_farms:
            self.logger.info(f"  제외 농장: {exclude_farms}")
        self.logger.info("=" * 60)

        # 기준 날짜 설정 (한국 시간 기준)
        if base_date:
            base_dt = datetime.strptime(base_date, '%Y%m%d')
        else:
            base_dt = now_kst()

        # 지난주 계산 (기준일의 지난주 월~일)
        days_to_last_sunday = (base_dt.weekday() + 1) % 7 or 7
        last_sunday = base_dt - timedelta(days=days_to_last_sunday)
        last_monday = last_sunday - timedelta(days=6)
        dt_from = last_monday.strftime('%Y%m%d')
        dt_to = last_sunday.strftime('%Y%m%d')

        # 주차 계산 (ISO Week)
        year = int(last_sunday.strftime('%G'))
        week_no = int(last_sunday.strftime('%V'))

        self.logger.info(f"  기준일: {base_dt.strftime('%Y-%m-%d')}")
        self.logger.info(f"  리포트 기간: {dt_from} ~ {dt_to}")
        self.logger.info(f"  주차: {year}년 {week_no}주")

        try:
            # 외부 데이터 수집 (옵션)
            if not skip_productivity or not skip_weather:
                self.logger.info("-" * 40)
                self.logger.info("외부 데이터 수집")
                self._collect_external_data(dt_to, skip_productivity, skip_weather)

            # 주간 리포트 생성 (전체 농장, exclude_farms 적용)
            self.logger.info("-" * 40)
            self.logger.info("주간 리포트 생성")

            report_result = self._generate_weekly_report(
                year=year,
                week_no=week_no,
                dt_from=dt_from,
                dt_to=dt_to,
                test_mode=False,
                init_delete=False,  # 전체 삭제 안함
                use_async=True,
                farm_list=None,  # 전체 농장
                exclude_farms=exclude_farms,  # 제외 농장
            )

            self.logger.info("=" * 60)
            self.logger.info(f"전체 농장 ETL 완료: {report_result.get('complete_cnt', 0)}/{report_result.get('target_cnt', 0)}")
            self.logger.info("=" * 60)

            return {
                'status': report_result.get('status', 'success'),
                'year': year,
                'week_no': week_no,
                'dt_from': dt_from,
                'dt_to': dt_to,
                'exclude_farms': exclude_farms,
                'target_cnt': report_result.get('target_cnt', 0),
                'complete_cnt': report_result.get('complete_cnt', 0),
                'error_cnt': report_result.get('error_cnt', 0),
            }

        except Exception as e:
            self.logger.error(f"전체 농장 ETL 실패: {e}", exc_info=True)
            return {
                'status': 'error',
                'error': str(e),
            }
