"""
비동기 병렬 처리 모듈
- 농장별 병렬 처리 (각 농장이 독립 DB 연결 사용)

아키텍처:
1. 농장별 병렬 처리: ThreadPoolExecutor로 여러 농장 동시 처리
2. 각 농장은 연결 풀에서 독립 연결 획득 → thread-safe 보장
3. 프로세서는 순차 실행 (동일 연결 내에서 안전)

수정 이력:
- 2025-12-23: 연결 풀(SessionPool) 도입으로 농장별 독립 연결 사용
- 프로세서 내부 병렬 처리 제거 (DB 작업 충돌 방지)
"""
import logging
import hashlib
import secrets
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum

from ..common import now_kst

logger = logging.getLogger(__name__)


class ProcessorType(Enum):
    """프로세서 유형 (의존성 그룹)"""
    CONFIG = 'config'       # 1차: 설정값 (선행)
    ALERT = 'alert'         # 2차: 관리대상 (독립)
    MODON = 'modon'         # 2차: 모돈현황 (독립)
    MATING = 'mating'       # 2차: 교배 (독립)
    FARROWING = 'farrowing' # 2차: 분만 (독립)
    WEANING = 'weaning'     # 2차: 이유 (독립)
    ACCIDENT = 'accident'   # 2차: 사고 (독립)
    CULLING = 'culling'     # 2차: 도태 (독립)
    SHIPMENT = 'shipment'   # 2차: 출하 (독립)
    SCHEDULE = 'schedule'   # 2차: 예정 (독립)


@dataclass
class ProcessorResult:
    """프로세서 실행 결과"""
    processor_type: ProcessorType
    status: str
    data: Dict[str, Any]
    elapsed_ms: float
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            'processor_type': self.processor_type.value,
            'status': self.status,
            'data': self.data,
            'elapsed_ms': self.elapsed_ms,
            'error': self.error,
        }


@dataclass
class FarmResult:
    """농장 처리 결과"""
    farm_no: int
    status: str
    processor_results: List[ProcessorResult]
    total_elapsed_ms: float
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            'farm_no': self.farm_no,
            'status': self.status,
            'processor_results': [p.to_dict() for p in self.processor_results],
            'total_elapsed_ms': self.total_elapsed_ms,
            'error': self.error,
        }


class AsyncFarmProcessor:
    """비동기 농장 처리 클래스

    각 농장은 독립 DB 연결을 사용하여 thread-safe 보장
    프로세서는 순차 실행 (동일 연결 내에서 안전)
    """

    def __init__(self, conn, master_seq: int, farm_no: int, locale: str = 'KOR'):
        """
        Args:
            conn: Oracle DB 연결 객체 (농장별 독립 연결)
            master_seq: 마스터 시퀀스
            farm_no: 농장 번호
            locale: 로케일
        """
        self.conn = conn
        self.master_seq = master_seq
        self.farm_no = farm_no
        self.locale = locale
        self.logger = logging.getLogger(f"{__name__}.Farm{farm_no}")

    def process(self, dt_from: str, dt_to: str, national_price: int = 0) -> Dict[str, Any]:
        """농장 주간 리포트 생성 (프로세서 순차 실행)

        Args:
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)
            national_price: 전국 탕박 평균 단가

        Returns:
            처리 결과 딕셔너리
        """
        from .data_loader import FarmDataLoader
        from .processors import (
            ConfigProcessor, ModonProcessor, AlertProcessor,
            MatingProcessor, FarrowingProcessor, WeaningProcessor,
            AccidentProcessor, CullingProcessor, ShipmentProcessor,
            ScheduleProcessor,
        )

        start_time = datetime.now()
        self.logger.info(f"농장 처리 시작: {self.farm_no}")

        processor_results = []

        try:
            # 1. 기존 데이터 삭제
            self._delete_existing_data()

            # 2. 상태 업데이트 (RUNNING)
            self._update_status('RUNNING')

            # ========================================
            # 3. 데이터 1회 로드
            # ========================================
            load_start = datetime.now()
            data_loader = FarmDataLoader(
                conn=self.conn,
                farm_no=self.farm_no,
                dt_from=dt_from,
                dt_to=dt_to,
                locale=self.locale,
            )
            data_loader.load()
            load_elapsed = (datetime.now() - load_start).total_seconds() * 1000
            self.logger.info(f"데이터 로드 완료: {self.farm_no} ({load_elapsed:.0f}ms)")

            # ========================================
            # 4. 1차 프로세서: Config (선행 필수)
            # ========================================
            config_proc = ConfigProcessor(
                self.conn, self.master_seq, self.farm_no, self.locale,
                data_loader=data_loader,
            )
            config_result = self._run_processor(
                ProcessorType.CONFIG,
                lambda: config_proc.process(dt_from, dt_to)
            )
            processor_results.append(config_result)

            # ========================================
            # 5. 2차 프로세서들: 순차 실행 (동일 연결 사용)
            # ========================================
            # 금주 예정 날짜 계산
            this_dt_from = (datetime.strptime(dt_to, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
            this_dt_to = (datetime.strptime(dt_to, '%Y%m%d') + timedelta(days=7)).strftime('%Y%m%d')

            # 프로세서 정의 (타입, 프로세서 클래스, 추가 인자)
            processors = [
                (ProcessorType.ALERT, AlertProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                (ProcessorType.MODON, ModonProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                (ProcessorType.MATING, MatingProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                (ProcessorType.FARROWING, FarrowingProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                (ProcessorType.WEANING, WeaningProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                (ProcessorType.ACCIDENT, AccidentProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                (ProcessorType.CULLING, CullingProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                (ProcessorType.SHIPMENT, ShipmentProcessor, {'dt_from': dt_from, 'dt_to': dt_to, 'national_price': national_price}),
                (ProcessorType.SCHEDULE, ScheduleProcessor, {'dt_from': this_dt_from, 'dt_to': this_dt_to}),
            ]

            # 순차 실행 (동일 연결에서 안전)
            for proc_type, proc_class, kwargs in processors:
                processor = proc_class(
                    self.conn, self.master_seq, self.farm_no, self.locale,
                    data_loader=data_loader,
                )
                result = self._run_processor(
                    proc_type,
                    lambda p=processor, kw=kwargs: p.process(**kw)
                )
                processor_results.append(result)

            # 6. 상태 업데이트 (COMPLETE) + 공유 토큰 생성
            self._update_complete()
            self.conn.commit()

            total_elapsed = (datetime.now() - start_time).total_seconds() * 1000
            self.logger.info(f"농장 처리 완료: {self.farm_no} ({total_elapsed:.0f}ms)")

            return {
                'farm_no': self.farm_no,
                'status': 'success',
                'processor_results': [r.to_dict() for r in processor_results],
                'total_elapsed_ms': total_elapsed,
            }

        except Exception as e:
            self.logger.error(f"농장 처리 실패: {self.farm_no} - {e}", exc_info=True)

            # 상태 업데이트 (ERROR)
            self._update_status('ERROR')
            self._log_error(str(e))
            self.conn.commit()

            total_elapsed = (datetime.now() - start_time).total_seconds() * 1000

            return {
                'farm_no': self.farm_no,
                'status': 'error',
                'processor_results': [r.to_dict() for r in processor_results],
                'total_elapsed_ms': total_elapsed,
                'error': str(e),
            }

    def _run_processor(self, proc_type: ProcessorType,
                        run_func: Callable) -> ProcessorResult:
        """프로세서 실행 래퍼

        Args:
            proc_type: 프로세서 유형
            run_func: 프로세서 실행 함수

        Returns:
            ProcessorResult 객체
        """
        start = datetime.now()
        try:
            result = run_func()
            elapsed = (datetime.now() - start).total_seconds() * 1000
            self.logger.debug(f"프로세서 완료: {proc_type.value} ({elapsed:.0f}ms)")
            return ProcessorResult(
                processor_type=proc_type,
                status='success',
                data=result,
                elapsed_ms=elapsed
            )
        except Exception as e:
            elapsed = (datetime.now() - start).total_seconds() * 1000
            self.logger.error(f"프로세서 실패: {proc_type.value} - {e}")
            return ProcessorResult(
                processor_type=proc_type,
                status='error',
                data={},
                elapsed_ms=elapsed,
                error=str(e)
            )

    def _delete_existing_data(self) -> None:
        """기존 데이터 삭제"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                DELETE FROM TS_INS_WEEK_SUB
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
            """, {'master_seq': self.master_seq, 'farm_no': self.farm_no})
        finally:
            cursor.close()

    def _update_status(self, status: str) -> None:
        """상태 업데이트"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                UPDATE TS_INS_WEEK
                SET STATUS_CD = :status
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
            """, {'status': status, 'master_seq': self.master_seq, 'farm_no': self.farm_no})
        finally:
            cursor.close()

    def _update_complete(self) -> None:
        """완료 상태 + 공유 토큰 생성"""
        cursor = self.conn.cursor()
        try:
            # 토큰 생성 (한국 시간 기준)
            kst_now = now_kst()
            token_data = f"{self.master_seq}-{self.farm_no}-{kst_now.strftime('%Y%m%d%H%M%S')}-{secrets.token_hex(16)}"
            share_token = hashlib.sha256(token_data.encode()).hexdigest().lower()

            # ETL 수행일 + 6일 = 수행일 포함 7일간 열람 가능 (한국 시간 기준)
            expire_dt = (kst_now + timedelta(days=6)).strftime('%Y%m%d')

            cursor.execute("""
                UPDATE TS_INS_WEEK
                SET STATUS_CD = 'COMPLETE',
                    SHARE_TOKEN = :share_token,
                    TOKEN_EXPIRE_DT = :expire_dt
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
            """, {
                'share_token': share_token,
                'expire_dt': expire_dt,
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
            })
        finally:
            cursor.close()

    def _log_error(self, error_msg: str) -> None:
        """오류 로그 기록"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT REPORT_YEAR, REPORT_WEEK_NO FROM TS_INS_MASTER WHERE SEQ = :master_seq
            """, {'master_seq': self.master_seq})
            row = cursor.fetchone()
            year = row[0] if row else 0
            week_no = row[1] if row else 0

            cursor.execute("""
                INSERT INTO TS_INS_JOB_LOG (
                    SEQ, MASTER_SEQ, FARM_NO, JOB_NM, PROC_NM,
                    DAY_GB, REPORT_YEAR, REPORT_WEEK_NO,
                    STATUS_CD, ERROR_MSG,
                    LOG_INS_DT, START_DT, END_DT
                ) VALUES (
                    SEQ_TS_INS_JOB_LOG.NEXTVAL,
                    :master_seq, :farm_no, 'PYTHON_ETL_ASYNC', 'AsyncFarmProcessor',
                    'WEEK', :year, :week_no,
                    'ERROR', :error_msg,
                    SYSDATE, SYSDATE, SYSDATE
                )
            """, {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'year': year,
                'week_no': week_no,
                'error_msg': error_msg[:4000],
            })
        finally:
            cursor.close()


class AsyncOrchestrator:
    """비동기 오케스트레이터

    농장별 병렬 처리 (각 농장은 연결 풀에서 독립 연결 획득)
    """

    def __init__(self, db, config=None, max_farm_workers: int = 4):
        """
        Args:
            db: Database 객체 (use_pool=True 필수)
            config: Config 객체
            max_farm_workers: 동시 처리 농장 수
        """
        self.db = db
        self.config = config
        self.max_farm_workers = max_farm_workers
        self.logger = logging.getLogger(__name__)

    def process_farms_parallel(
        self,
        master_seq: int,
        farms: List[Dict],
        dt_from: str,
        dt_to: str,
        national_price: int = 0,
    ) -> List[FarmResult]:
        """농장들을 병렬로 처리

        Args:
            master_seq: 마스터 시퀀스
            farms: 농장 정보 리스트
            dt_from: 시작일
            dt_to: 종료일
            national_price: 전국 평균 단가

        Returns:
            FarmResult 리스트
        """
        self.logger.info(f"농장 병렬 처리 시작: {len(farms)}개 농장, workers={self.max_farm_workers}")

        results = []

        def process_single_farm(farm: Dict) -> FarmResult:
            """단일 농장 처리 (풀에서 독립 연결 획득)"""
            farm_no = farm['FARM_NO']
            locale = farm.get('LOCALE', 'KOR')

            # 농장별로 풀에서 새 DB 연결 획득 (thread-safe)
            with self.db.get_connection() as conn:
                processor = AsyncFarmProcessor(
                    conn=conn,
                    master_seq=master_seq,
                    farm_no=farm_no,
                    locale=locale,
                )
                return processor.process(dt_from, dt_to, national_price)

        # ThreadPoolExecutor로 농장 병렬 처리
        with ThreadPoolExecutor(max_workers=self.max_farm_workers) as executor:
            futures = {executor.submit(process_single_farm, farm): farm for farm in farms}

            for future in as_completed(futures):
                farm = futures[future]
                farm_no = farm['FARM_NO']
                try:
                    result = future.result()
                    results.append(result)
                    self.logger.info(f"농장 완료: {farm_no} ({result.status})")
                except Exception as e:
                    self.logger.error(f"농장 처리 예외: {farm_no} - {e}")
                    results.append(FarmResult(
                        farm_no=farm_no,
                        status='error',
                        processor_results=[],
                        total_elapsed_ms=0,
                        error=str(e)
                    ))

        # 결과 요약
        success_cnt = len([r for r in results if r.status == 'success'])
        error_cnt = len([r for r in results if r.status == 'error'])
        self.logger.info(f"농장 병렬 처리 완료: 성공={success_cnt}, 실패={error_cnt}")

        return results


def run_async_etl(
    db,
    master_seq: int,
    farms: List[Dict],
    dt_from: str,
    dt_to: str,
    national_price: int = 0,
    max_farm_workers: int = 4,
) -> List[FarmResult]:
    """비동기 ETL 실행 함수

    Args:
        db: Database 객체 (use_pool=True 필수)
        master_seq: 마스터 시퀀스
        farms: 농장 정보 리스트
        dt_from: 시작일
        dt_to: 종료일
        national_price: 전국 평균 단가
        max_farm_workers: 동시 처리 농장 수

    Returns:
        FarmResult 리스트
    """
    orchestrator = AsyncOrchestrator(
        db=db,
        max_farm_workers=max_farm_workers,
    )

    return orchestrator.process_farms_parallel(
        master_seq=master_seq,
        farms=farms,
        dt_from=dt_from,
        dt_to=dt_to,
        national_price=national_price
    )
